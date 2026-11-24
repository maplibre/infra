import { createWriteStream } from "node:fs";
import { rm } from "node:fs/promises";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { XMLParser } from "fast-xml-parser";
import yauzl, { type Entry, type ZipFile } from "yauzl";

export const MAX_CHECK_OUTPUT_LEN = 65_535;
export const DEFAULT_JUNIT_MAX_BYTES = 10 << 20;
export const DEFAULT_TAIL_LINES = 40;
export const MAX_ZIP_ENTRIES = 10_000;

export class InvalidArchiveError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "InvalidArchiveError";
  }
}

export class PackageNotFoundError extends Error {
  constructor(filename: string) {
    super(`Test package not found inside artifact zip: ${filename}`);
    this.name = "PackageNotFoundError";
  }
}

export interface JUnitSummary {
  totalTests: number;
  passedTests: number;
  failedTests: string[];
}

export interface PhoneReport {
  phoneName: string;
  phoneSlug: string;
  result: string;
  deviceMinutes: number;
  outputUrl?: string;
  artifactUrl?: string;
  junitUrl?: string;
  junit?: JUnitSummary;
  outputTailLines?: string[];
}

export const phoneSlug = (name: string) =>
  name
    .trim()
    .replace(/[^a-zA-Z0-9_.]+/g, "-")
    .replace(/^-+|-+$/g, "") || "device";

export function consoleUrl(runArn: string): string {
  const parts = runArn.split(":");
  const region = parts[3] ?? "";
  const [projectId, runId] = (parts[6] ?? "").split("/");
  return region && projectId && runId
    ? `https://${region}.console.aws.amazon.com/devicefarm/home?region=${region}#/mobile/projects/${projectId}/runs/${runId}`
    : "";
}

export const runIdFromArn = (runArn: string) => runArn.slice(runArn.lastIndexOf("/") + 1) || runArn;

export const mapRunResult = (result?: string): "success" | "cancelled" | "failure" =>
  result === "PASSED" ? "success" : result === "STOPPED" ? "cancelled" : "failure";

function byteLimit(maxBytes: number, onExceed: () => Error): Transform {
  let total = 0;
  return new Transform({
    transform(chunk: Buffer, _enc, cb) {
      total += chunk.length;
      cb(total > maxBytes ? onExceed() : null, chunk);
    },
  });
}

export async function downloadStreamToFile(
  url: string,
  dstPath: string,
  maxBytes: number,
  headers?: Record<string, string>,
): Promise<void> {
  const resp = await fetch(url, { headers, redirect: "follow" });
  if (!resp.ok || !resp.body) throw new Error(`Download failed with HTTP ${resp.status}`);
  try {
    await pipeline(
      Readable.fromWeb(resp.body as unknown as import("node:stream/web").ReadableStream),
      byteLimit(maxBytes, () => new Error(`Download exceeds max size ${maxBytes} bytes`)),
      createWriteStream(dstPath),
    );
  } catch (err) {
    await rm(dstPath, { force: true });
    throw err;
  }
}

async function findZipEntry<T>(
  zipPath: string,
  matcher: (entry: Entry) => boolean,
  consume: (zf: ZipFile, entry: Entry) => Promise<T>,
  maxEntries = MAX_ZIP_ENTRIES,
): Promise<{ found: true; value: T } | { found: false }> {
  const zf = await new Promise<ZipFile>((resolve, reject) => {
    yauzl.open(zipPath, { lazyEntries: true, autoClose: false }, (err, zip) => {
      if (err || !zip) {
        const code = (err as NodeJS.ErrnoException | null)?.code;
        reject(
          code === "ENOENT" || code === "EACCES" || code === "EISDIR"
            ? err
            : new InvalidArchiveError(err?.message ?? "invalid zip archive", {
                cause: err,
              }),
        );
        return;
      }
      resolve(zip);
    });
  });

  try {
    if (zf.entryCount > maxEntries) {
      throw new InvalidArchiveError(
        `ZIP archive declares ${zf.entryCount} entries (max ${maxEntries})`,
      );
    }
    let seen = 0;
    while (true) {
      const entry = await new Promise<Entry | null>((resolve, reject) => {
        const done = () => {
          zf.off("entry", onEntry);
          zf.off("end", onEnd);
          zf.off("error", onErr);
        };
        const onEntry = (e: Entry) => (done(), resolve(e));
        const onEnd = () => (done(), resolve(null));
        const onErr = (err: Error) => (
          done(),
          reject(new InvalidArchiveError(err.message, { cause: err }))
        );
        zf.once("entry", onEntry).once("end", onEnd).once("error", onErr).readEntry();
      });
      if (!entry) return { found: false };
      if (++seen > maxEntries) throw new InvalidArchiveError(`ZIP exceeds ${maxEntries} entries`);
      if (!entry.fileName.endsWith("/") && matcher(entry)) {
        return { found: true, value: await consume(zf, entry) };
      }
    }
  } finally {
    zf.close();
  }
}

const openEntryStream = (zf: ZipFile, entry: Entry) =>
  new Promise<NodeJS.ReadableStream>((resolve, reject) => {
    zf.openReadStream(entry, (err, s) =>
      err || !s
        ? reject(
            new InvalidArchiveError(err?.message ?? "failed to open zip entry", { cause: err }),
          )
        : resolve(s),
    );
  });

export async function extractPackageFromZip(
  outerZipPath: string,
  targetFilename: string,
  dstPath: string,
  maxBytes: number,
): Promise<void> {
  const res = await findZipEntry(
    outerZipPath,
    (e) => e.fileName === targetFilename,
    async (zf, entry) => {
      const stream = await openEntryStream(zf, entry);
      try {
        await pipeline(
          stream,
          byteLimit(
            maxBytes,
            () => new InvalidArchiveError(`${targetFilename} exceeds ${maxBytes} bytes`),
          ),
          createWriteStream(dstPath),
        );
      } catch (err) {
        await rm(dstPath, { force: true });
        throw err;
      }
    },
  );
  if (!res.found) throw new PackageNotFoundError(targetFilename);
}

export async function extractJUnitFromZip(
  zipPath: string,
  maxBytes = DEFAULT_JUNIT_MAX_BYTES,
): Promise<Buffer | null> {
  const res = await findZipEntry(
    zipPath,
    (e) => e.fileName === "junit.xml" || e.fileName.endsWith("/junit.xml"),
    async (zf, entry) => {
      const chunks: Buffer[] = [];
      let total = 0;
      for await (const chunk of await openEntryStream(zf, entry)) {
        const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        if ((total += buf.length) > maxBytes) {
          throw new InvalidArchiveError(`${entry.fileName} exceeds max size ${maxBytes} bytes`);
        }
        chunks.push(buf);
      }
      return Buffer.concat(chunks, total);
    },
  );
  return res.found ? res.value : null;
}

const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "@_",
  isArray: (name) =>
    name === "testsuite" || name === "testcase" || name === "failure" || name === "error",
});

export function parseJUnit(rawXml: string | Buffer): JUnitSummary {
  const doc = xmlParser.parse(rawXml.toString("utf8")) as Record<string, any>;
  const cases: Record<string, any>[] = [];
  const collect = (s: Record<string, any>) => {
    if (Array.isArray(s.testcase)) cases.push(...s.testcase);
    if (Array.isArray(s.testsuite)) s.testsuite.forEach(collect);
  };
  let totalAttr = 0;
  if (doc.testsuites && typeof doc.testsuites === "object") {
    totalAttr = Number(doc.testsuites["@_tests"] ?? 0) || 0;
    if (Array.isArray(doc.testsuites.testsuite)) doc.testsuites.testsuite.forEach(collect);
  } else if (Array.isArray(doc.testsuite) && doc.testsuite[0]) {
    totalAttr = Number(doc.testsuite[0]["@_tests"] ?? 0) || 0;
    doc.testsuite.forEach(collect);
  } else {
    throw new Error("Unexpected JUnit XML root element");
  }

  const failedTests = cases
    .filter(
      (tc) =>
        (Array.isArray(tc.failure) && tc.failure.length > 0) ||
        (Array.isArray(tc.error) && tc.error.length > 0),
    )
    .map((tc) => `${tc["@_classname"] ?? ""} ${tc["@_name"] ?? ""}`.trim());
  const totalTests = totalAttr > 0 ? totalAttr : cases.length;
  return {
    totalTests,
    passedTests: Math.max(0, totalTests - failedTests.length),
    failedTests,
  };
}

export function tailLines(data: Buffer, n: number): string[] {
  if (n <= 0 || data.length === 0) return [];
  let end = data.length;
  if (data[end - 1] === 0x0a) end -= end > 1 && data[end - 2] === 0x0d ? 2 : 1;
  if (end <= 0) return [];
  let start = 0;
  for (let i = end - 1, nl = 0; i >= 0; i--) {
    if (data[i] === 0x0a && ++nl === n) {
      start = i + 1;
      break;
    }
  }
  return data.subarray(start, end).toString("utf8").replaceAll("\r\n", "\n").split("\n");
}

export function truncateMarkdown(s: string, maxLen = MAX_CHECK_OUTPUT_LEN): string {
  if (s.length <= maxLen) return s;
  const suffix = "\n\n... (truncated; see full output in artifacts)\n";
  let cut = maxLen - suffix.length;
  if (cut <= 0) return s.slice(0, maxLen);
  if (s.charCodeAt(cut - 1) >= 0xd800 && s.charCodeAt(cut - 1) <= 0xdbff) cut--;
  return s.slice(0, cut) + suffix;
}

function fencedBlock(raw: string): string {
  const content = truncateMarkdown(raw);
  let maxTicks = 0,
    maxTildes = 0,
    curTicks = 0,
    curTildes = 0;
  for (let i = 0; i < content.length; i++) {
    const ch = content[i];
    if (ch === "`") {
      maxTicks = Math.max(maxTicks, ++curTicks);
      curTildes = 0;
    } else if (ch === "~") {
      maxTildes = Math.max(maxTildes, ++curTildes);
      curTicks = 0;
    } else {
      curTicks = curTildes = 0;
    }
  }
  const tickLen = Math.max(3, maxTicks + 1);
  const tildeLen = Math.max(3, maxTildes + 1);
  const fence = tildeLen < tickLen ? "~".repeat(tildeLen) : "`".repeat(tickLen);
  return `${fence}\n${content && !content.endsWith("\n") ? `${content}\n` : content}${fence}`;
}

export function buildCheckOutput(
  runResult: string | undefined,
  runConsoleUrl: string,
  phones: PhoneReport[],
) {
  const passed = phones.filter((p) => p.result === "PASSED").length;
  const title =
    runResult === "STOPPED"
      ? "Stopped"
      : phones.length === 0
        ? runResult === "PASSED"
          ? "Passed"
          : "Failed"
        : runResult === "PASSED"
          ? `Passed (${passed}/${phones.length} phones)`
          : `Failed (${passed}/${phones.length} phones passed)`;

  const esc = (s: string) => s.replace(/\r?\n/g, " ").replaceAll("|", "\\|");
  let summary =
    phones.length === 0
      ? "The run has no phone's results.\n"
      : "| Phone | Result | Device minutes | Output | Artifacts |\n| --- | --- | --- | --- | --- |\n" +
        phones
          .map((p) => {
            const out = p.outputUrl ? `[TESTSPEC_OUTPUT.txt](${p.outputUrl})` : "—";
            const arts =
              [
                p.artifactUrl ? `[CUSTOMER_ARTIFACT.zip](${p.artifactUrl})` : "",
                p.junitUrl ? `[junit.xml](${p.junitUrl})` : "",
              ]
                .filter(Boolean)
                .join(" · ") || "—";
            return `| ${esc(p.phoneName)} | ${esc(p.result)} | ${p.deviceMinutes.toFixed(1)} | ${out} | ${arts} |\n`;
          })
          .join("");
  if (runConsoleUrl) summary += `\n[View run in AWS Device Farm console](${runConsoleUrl})\n`;

  const text =
    phones.length === 0
      ? "The run has no phone's results.\n"
      : phones
          .map((p) => {
            const head = `### ${p.phoneName.replace(/\r?\n/g, " ")}\n\n`;
            if (!p.junit)
              return `${head}The tests didn't run:\n\n${fencedBlock((p.outputTailLines ?? []).join("\n"))}\n`;
            const failed = p.junit.failedTests.length
              ? `\nFailed:\n\n${fencedBlock(p.junit.failedTests.join("\n"))}\n`
              : "";
            return `${head}${p.junit.passedTests} of ${p.junit.totalTests} tests passed.\n${failed}`;
          })
          .join("\n");

  return {
    title,
    summary: truncateMarkdown(summary),
    text: truncateMarkdown(text),
  };
}
