import assert from "node:assert/strict";
import { createWriteStream } from "node:fs";
import { writeFile } from "node:fs/promises";
import { describe, it } from "node:test";
import { GetRunCommand, ListArtifactsCommand, ListJobsCommand } from "@aws-sdk/client-device-farm";
import { PutObjectCommand } from "@aws-sdk/client-s3";
import type { Octokit } from "octokit";
import yazl from "yazl";
import { loadConfig, type RunRecord } from "../src/config.js";
import { handlePollerEvent } from "../src/poller.js";
import type { RunStore } from "../src/store.js";

function makeCfg() {
  return loadConfig({
    MODE: "poller",
    GITHUB_APP_SECRET_ARN: "arn:aws:secretsmanager:us-west-2:123:secret:gh",
    RUNS_TABLE_NAME: "runs-table",
    RESULTS_BUCKET_NAME: "results-bucket",
    RESULTS_BASE_URL: "https://cdn.example",
  });
}

describe("poller handler", () => {
  it("uploads artifacts, parses JUnit, falls back on corrupt ZIP, and completes Check Run", async () => {
    const runArn = "arn:aws:devicefarm:us-west-2:373521797162:run:proj-1/run-1";
    const active: RunRecord = {
      run_arn: runArn,
      state: "ACTIVE",
      created_at: "2026-06-01T12:00:00Z",
      repo: "maplibre/mln-rs",
      sha: "deadbeef",
      check_run_id: 77,
      installation_id: 99,
      expires_at: 9999999999,
    };

    let markedResult = "";
    const store = {
      queryActiveRuns: async () => [active],
      markCompleted: async (_arn: string, result: string) => {
        markedResult = result;
      },
    } as unknown as RunStore;

    const s3Uploads: Array<{
      key: string;
      disposition: string;
      contentType: string;
    }> = [];
    const s3 = {
      send: async (cmd: unknown) => {
        if (cmd instanceof PutObjectCommand) {
          s3Uploads.push({
            key: cmd.input.Key!,
            disposition: cmd.input.ContentDisposition!,
            contentType: cmd.input.ContentType!,
          });
          return {};
        }
        throw new Error(`unexpected s3 cmd: ${String(cmd)}`);
      },
    };

    const df = {
      send: async (cmd: unknown) => {
        if (cmd instanceof GetRunCommand) return { run: { status: "COMPLETED", result: "FAILED" } };
        if (cmd instanceof ListJobsCommand) {
          return {
            jobs: [
              {
                arn: "job-pixel",
                device: { name: "Google Pixel 9" },
                result: "PASSED",
                deviceMinutes: { total: 3.5 },
              },
              {
                arn: "job-s25",
                device: { name: "Samsung Galaxy S25" },
                result: "ERRORED",
                deviceMinutes: { total: 1.2 },
              },
            ],
          };
        }
        if (cmd instanceof ListArtifactsCommand) {
          if (cmd.input.arn === "job-pixel") {
            return {
              artifacts: [
                {
                  type: "TESTSPEC_OUTPUT",
                  url: "https://df.example/pixel.txt",
                },
                {
                  type: "CUSTOMER_ARTIFACT",
                  url: "https://df.example/pixel.zip",
                },
              ],
            };
          }
          return {
            artifacts: [
              { type: "TESTSPEC_OUTPUT", url: "https://df.example/s25.txt" },
              {
                type: "CUSTOMER_ARTIFACT",
                url: "https://df.example/s25-corrupt.zip",
              },
            ],
          };
        }
        throw new Error(`unexpected df cmd: ${String(cmd)}`);
      },
    };

    let checkUpdate: any = null;
    const gh = {
      rest: {
        checks: {
          update: async (params: any) => {
            checkUpdate = params;
            return { data: {} };
          },
        },
      },
    } as unknown as Octokit;

    await handlePollerEvent({
      cfg: makeCfg(),
      store,
      df,
      s3,
      getOctokit: async () => gh,
      now: () => new Date("2026-06-01T12:15:00Z"),
      downloadToFile: async (url, dstPath) => {
        if (url.endsWith("pixel.txt")) return writeFile(dstPath, "pixel ok\n");
        if (url.endsWith("s25.txt")) return writeFile(dstPath, "line 1\nadb: device offline\n");
        if (url.endsWith("s25-corrupt.zip")) return writeFile(dstPath, "not-a-valid-zip");
        const zf = new yazl.ZipFile();
        zf.addBuffer(
          Buffer.from(
            `<testsuite tests="2"><testcase classname="a" name="t1"/><testcase classname="a" name="t2"/></testsuite>`,
          ),
          "junit.xml",
        );
        await new Promise<void>((resolve, reject) => {
          zf.outputStream.pipe(createWriteStream(dstPath)).on("close", resolve).on("error", reject);
          zf.end();
        });
      },
    });

    assert.equal(markedResult, "FAILED");
    assert.equal(checkUpdate.status, "completed");
    assert.equal(checkUpdate.conclusion, "failure");
    assert.equal(checkUpdate.output.title, "Failed (1/2 phones passed)");
    assert.match(checkUpdate.output.text, /2 of 2 tests passed/);
    assert.match(checkUpdate.output.text, /adb: device offline/);
    assert.ok(s3Uploads.every((u) => u.disposition.startsWith("attachment; filename=")));
    assert.ok(s3Uploads.some((u) => u.key.endsWith("/Google-Pixel-9/junit.xml")));
  });

  it("marks missing Device Farm runs as NOT_FOUND", async () => {
    const runArn = "arn:aws:devicefarm:us-west-2:373521797162:run:proj-1/missing";
    let marked = "";
    await handlePollerEvent({
      cfg: makeCfg(),
      store: {
        queryActiveRuns: async () => [
          {
            run_arn: runArn,
            state: "ACTIVE",
            created_at: "2026-06-01T12:00:00Z",
            expires_at: 9999999999,
          },
        ],
        markCompleted: async (_arn: string, res: string) => {
          marked = res;
        },
      } as unknown as RunStore,
      df: {
        send: async () => {
          const err = new Error("not found");
          err.name = "NotFoundException";
          throw err;
        },
      },
      s3: { send: async () => ({}) },
      getOctokit: async () => ({}) as Octokit,
    });
    assert.equal(marked, "NOT_FOUND");
  });
});
