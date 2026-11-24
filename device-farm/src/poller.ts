import { createReadStream } from "node:fs";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  type DeviceFarmClient,
  GetRunCommand,
  ListArtifactsCommand,
  ListJobsCommand,
  NotFoundException,
} from "@aws-sdk/client-device-farm";
import { PutObjectCommand, type S3Client } from "@aws-sdk/client-s3";
import type { Octokit } from "octokit";
import { type Config, lookupRepo, type RunRecord } from "./config.js";
import {
  buildCheckOutput,
  consoleUrl,
  DEFAULT_JUNIT_MAX_BYTES,
  DEFAULT_TAIL_LINES,
  downloadStreamToFile,
  extractJUnitFromZip,
  mapRunResult,
  parseJUnit,
  type PhoneReport,
  phoneSlug,
  runIdFromArn,
  tailLines,
} from "./report.js";
import type { RunStore } from "./store.js";

const MAX_OUTPUT_BYTES = 64 << 20;
const MAX_ARTIFACT_BYTES = 256 << 20;

export async function handlePollerEvent(
  cfg: Config,
  store: RunStore,
  df: DeviceFarmClient,
  s3: S3Client,
  getOctokit: (installationId: number) => Promise<Octokit>,
): Promise<void> {
  const errs: unknown[] = [];
  for (const rec of await store.queryActiveRuns()) {
    try {
      await pollRun(rec, cfg, store, df, s3, getOctokit);
    } catch (err) {
      console.error("poller failed run", rec.run_arn, err);
      errs.push(err);
    }
  }
  if (errs.length > 0) {
    throw new AggregateError(errs, `${errs.length} active run(s) failed polling`);
  }
}

async function pollRun(
  rec: RunRecord,
  cfg: Config,
  store: RunStore,
  df: DeviceFarmClient,
  s3: S3Client,
  getOctokit: (installationId: number) => Promise<Octokit>,
): Promise<void> {
  let run;
  try {
    run = (await df.send(new GetRunCommand({ arn: rec.run_arn }))).run;
  } catch (err) {
    if (
      err instanceof NotFoundException ||
      (err as { name?: string })?.name === "NotFoundException"
    ) {
      await store.markCompleted(rec.run_arn, "NOT_FOUND");
      return;
    }
    throw err;
  }
  if (!run) return void (await store.markCompleted(rec.run_arn, "NOT_FOUND"));
  if (run.status !== "COMPLETED") return;

  const repo = lookupRepo(cfg, rec.repo);
  if (!repo) return void (await store.markCompleted(rec.run_arn, run.result ?? "UNKNOWN"));

  const putS3 = (
    key: string,
    body: Buffer | ReturnType<typeof createReadStream>,
    contentType: string,
    filename: string,
  ) =>
    s3.send(
      new PutObjectCommand({
        Bucket: cfg.resultsBucketName,
        Key: key,
        Body: body,
        ContentType: contentType,
        ContentDisposition: `attachment; filename="${filename}"`,
      }),
    );

  const tryDownload = async (url: string, dst: string, maxBytes: number, label: string) => {
    try {
      await downloadStreamToFile(url, dst, maxBytes);
      return true;
    } catch (err) {
      console.warn(`failed to download ${label}`, err);
      return false;
    }
  };

  const dir = await mkdtemp(join(tmpdir(), "device-farm-poller-"));
  try {
    const jobs: Array<{ arn: string; name: string; result: string; minutes: number }> = [];
    let nextToken: string | undefined;
    do {
      const out = await df.send(new ListJobsCommand({ arn: rec.run_arn, nextToken }));
      for (const j of out.jobs ?? []) {
        jobs.push({
          arn: j.arn ?? "",
          name: j.device?.name || j.name || "Unknown device",
          result: j.result ?? "",
          minutes: j.deviceMinutes?.total ?? 0,
        });
      }
      nextToken = out.nextToken;
    } while (nextToken);
    jobs.sort((a, b) => a.name.localeCompare(b.name) || a.arn.localeCompare(b.arn));

    const runId = runIdFromArn(rec.run_arn);
    const slugCounts = new Map<string, number>();
    const phones: PhoneReport[] = [];

    for (const job of jobs) {
      const base = phoneSlug(job.name);
      const count = (slugCounts.get(base) ?? 0) + 1;
      slugCounts.set(base, count);
      const slug = count > 1 ? `${base}-${count}` : base;

      const phoneDir = join(dir, slug);
      await mkdir(phoneDir, { recursive: true });
      const keyPrefix = `${repo.name}/${rec.sha}/${runId}/${slug}`;
      const pr: PhoneReport = {
        phoneName: job.name,
        phoneSlug: slug,
        result: job.result,
        deviceMinutes: job.minutes,
      };

      const artifacts: Array<{ type?: string; url?: string }> = [];
      let artToken: string | undefined;
      do {
        const res = await df.send(
          new ListArtifactsCommand({ arn: job.arn, type: "FILE", nextToken: artToken }),
        );
        artifacts.push(...(res.artifacts ?? []).map((a) => ({ type: a.type, url: a.url })));
        artToken = res.nextToken;
      } while (artToken);

      const outputUrl = artifacts.find((a) => a.type === "TESTSPEC_OUTPUT" && a.url)?.url;
      const artifactUrl = artifacts.find((a) => a.type === "CUSTOMER_ARTIFACT" && a.url)?.url;

      let testspecBytes: Buffer = Buffer.alloc(0);
      const outPath = join(phoneDir, "TESTSPEC_OUTPUT.txt");
      if (outputUrl && (await tryDownload(outputUrl, outPath, MAX_OUTPUT_BYTES, job.name))) {
        const key = `${keyPrefix}/TESTSPEC_OUTPUT.txt`;
        await putS3(
          key,
          createReadStream(outPath),
          "text/plain; charset=utf-8",
          "TESTSPEC_OUTPUT.txt",
        );
        pr.outputUrl = `${cfg.resultsBaseUrl}/${key}`;
        testspecBytes = await readFile(outPath);
      }

      const zipPath = join(phoneDir, "CUSTOMER_ARTIFACT.zip");
      if (artifactUrl && (await tryDownload(artifactUrl, zipPath, MAX_ARTIFACT_BYTES, job.name))) {
        const key = `${keyPrefix}/CUSTOMER_ARTIFACT.zip`;
        await putS3(key, createReadStream(zipPath), "application/zip", "CUSTOMER_ARTIFACT.zip");
        pr.artifactUrl = `${cfg.resultsBaseUrl}/${key}`;

        const junitRaw = await extractJUnitFromZip(zipPath, DEFAULT_JUNIT_MAX_BYTES).catch(
          (err) => (console.warn("failed to extract junit.xml", job.name, err), null),
        );
        if (junitRaw) {
          const junitKey = `${keyPrefix}/junit.xml`;
          await putS3(junitKey, junitRaw, "application/xml", "junit.xml");
          pr.junitUrl = `${cfg.resultsBaseUrl}/${junitKey}`;
          try {
            pr.junit = parseJUnit(junitRaw);
          } catch (err) {
            console.warn("failed to parse junit.xml", job.name, err);
          }
        }
      }

      if (!pr.junit) pr.outputTailLines = tailLines(testspecBytes, DEFAULT_TAIL_LINES);
      phones.push(pr);
    }

    const url = consoleUrl(rec.run_arn);
    const out = buildCheckOutput(run.result, url, phones);
    if (rec.check_run_id && rec.installation_id) {
      const gh = await getOctokit(rec.installation_id);
      await gh.rest.checks.update({
        owner: repo.owner,
        repo: repo.name,
        check_run_id: rec.check_run_id,
        status: "completed",
        conclusion: mapRunResult(run.result),
        completed_at: new Date().toISOString(),
        details_url: url,
        output: { title: out.title, summary: out.summary, text: out.text },
      });
    }
    await store.markCompleted(rec.run_arn, run.result ?? "UNKNOWN");
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}
