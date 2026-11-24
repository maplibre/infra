import assert from "node:assert/strict";
import { createWriteStream } from "node:fs";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "node:test";
import yazl from "yazl";
import { loadConfig, parseExternalId } from "../src/config.js";
import {
  buildCheckOutput,
  consoleUrl,
  DEFAULT_JUNIT_MAX_BYTES,
  extractJUnitFromZip,
  extractPackageFromZip,
  InvalidArchiveError,
  mapRunResult,
  MAX_CHECK_OUTPUT_LEN,
  MAX_ZIP_ENTRIES,
  PackageNotFoundError,
  parseJUnit,
  phoneSlug,
  tailLines,
  truncateMarkdown,
} from "../src/report.js";

async function makeZip(path: string, files: Record<string, Buffer | string>): Promise<void> {
  const zf = new yazl.ZipFile();
  for (const [name, content] of Object.entries(files)) {
    zf.addBuffer(Buffer.isBuffer(content) ? content : Buffer.from(content, "utf8"), name);
  }
  await new Promise<void>((resolve, reject) => {
    zf.outputStream.pipe(createWriteStream(path)).on("close", resolve).on("error", reject);
    zf.end();
  });
}

describe("config & helpers", () => {
  it("loads mode-specific config and parses external IDs", () => {
    const cfg = loadConfig({
      MODE: "webhook",
      GITHUB_APP_SECRET_ARN: "arn:aws:secretsmanager:us-west-2:123:secret:gh",
      JOBS_QUEUE_URL: "https://sqs.us-west-2.amazonaws.com/123/jobs",
      ALLOWED_BRANCHES: "ci/test, main",
    });
    assert.equal(cfg.mode, "webhook");
    assert.deepEqual([...cfg.allowedBranches], ["ci/test", "main"]);
    assert.throws(() => loadConfig({ MODE: "worker" }));

    assert.deepEqual(parseExternalId("123"), { runId: 123, attempt: 1 });
    assert.deepEqual(parseExternalId("123:2"), { runId: 123, attempt: 2 });
    assert.equal(parseExternalId("123:0"), null);
    assert.equal(parseExternalId("123:2:extra"), null);
    assert.equal(parseExternalId("bad"), null);
  });

  it("formats slugs, console URLs, and conclusions", () => {
    assert.equal(phoneSlug("Google Pixel 9"), "Google-Pixel-9");
    assert.equal(phoneSlug("Samsung Galaxy S25 / Ultra!"), "Samsung-Galaxy-S25-Ultra");
    assert.equal(phoneSlug("///"), "device");
    assert.equal(
      consoleUrl("arn:aws:devicefarm:us-west-2:373521797162:run:proj-1/run-2"),
      "https://us-west-2.console.aws.amazon.com/devicefarm/home?region=us-west-2#/mobile/projects/proj-1/runs/run-2",
    );
    assert.equal(mapRunResult("PASSED"), "success");
    assert.equal(mapRunResult("STOPPED"), "cancelled");
    assert.equal(mapRunResult("FAILED"), "failure");
  });
});

describe("ZIP extraction & bounds", () => {
  it("extracts package and JUnit XML from valid archives", async () => {
    const dir = await mkdtemp(join(tmpdir(), "df-report-test-"));
    try {
      const outer = join(dir, "artifact.zip");
      await makeZip(outer, {
        "android-tests.zip": "opaque-inner-zip",
        "Host_Machine_Files/$DEVICEFARM_LOG_DIR/junit.xml": `<?xml version="1.0"?>
          <testsuites tests="3" failures="1">
            <testsuite name="mln_render" tests="3">
              <testcase classname="mln::gpu" name="clear"/>
              <testcase classname="mln::gpu" name="draw"><failure message="diff">px</failure></testcase>
              <testcase classname="mln::gpu" name="offscreen"><error message="timeout"/></testcase>
            </testsuite>
          </testsuites>`,
      });

      const dst = join(dir, "out.zip");
      await extractPackageFromZip(outer, "android-tests.zip", dst, 1024);
      assert.equal(await readFile(dst, "utf8"), "opaque-inner-zip");

      const junit = await extractJUnitFromZip(outer, DEFAULT_JUNIT_MAX_BYTES);
      assert.ok(junit);
      const summary = parseJUnit(junit);
      assert.deepEqual(summary, {
        totalTests: 3,
        passedTests: 1,
        failedTests: ["mln::gpu draw", "mln::gpu offscreen"],
      });

      await assert.rejects(
        () => extractPackageFromZip(outer, "missing.zip", join(dir, "x"), 1024),
        PackageNotFoundError,
      );
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it("rejects oversized entry counts, non-ZIP, and truncated files with InvalidArchiveError", async () => {
    const dir = await mkdtemp(join(tmpdir(), "df-zip-bounds-"));
    try {
      // Real ZIP with MAX_ZIP_ENTRIES + 1 empty entries.
      const huge = join(dir, "huge.zip");
      const zf = new yazl.ZipFile();
      const empty = Buffer.alloc(0);
      for (let i = 0; i < MAX_ZIP_ENTRIES + 1; i++) {
        zf.addBuffer(empty, `f${i}`, { compress: false });
      }
      await new Promise<void>((resolve, reject) => {
        zf.outputStream.pipe(createWriteStream(huge)).on("close", resolve).on("error", reject);
        zf.end();
      });

      await assert.rejects(() => extractJUnitFromZip(huge), InvalidArchiveError);
      await assert.rejects(
        () => extractPackageFromZip(huge, "android-tests.zip", join(dir, "x"), 1024),
        InvalidArchiveError,
      );

      const nonZip = join(dir, "plain.txt");
      await writeFile(nonZip, "not a zip file");
      await assert.rejects(() => extractJUnitFromZip(nonZip), InvalidArchiveError);

      const trunc = join(dir, "trunc.zip");
      await writeFile(trunc, "PK\x05\x06short");
      await assert.rejects(() => extractJUnitFromZip(trunc), InvalidArchiveError);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});

describe("tailLines & Markdown rendering", () => {
  it("handles huge newline-heavy buffers and CRLF cleanly", () => {
    const big = Buffer.from("\n".repeat(200_000) + "line-a\r\nline-b\r\nline-c\r\n");
    assert.deepEqual(tailLines(big, 2), ["line-b", "line-c"]);
    assert.deepEqual(tailLines(big, 3), ["line-a", "line-b", "line-c"]);
    assert.deepEqual(tailLines(Buffer.alloc(0), 5), []);
    assert.deepEqual(tailLines(Buffer.from("\r\n"), 5), []);
    assert.deepEqual(tailLines(Buffer.from("\n\n"), 5), ["", ""]);
  });

  it("renders summary table, safe fences for backtick floods, and truncates safely", () => {
    const out = buildCheckOutput("FAILED", "https://console.example/run", [
      {
        phoneName: "Google Pixel 9",
        phoneSlug: "Google-Pixel-9",
        result: "PASSED",
        deviceMinutes: 4.2,
        outputUrl: "https://cdn.example/out.txt",
        artifactUrl: "https://cdn.example/art.zip",
        junitUrl: "https://cdn.example/junit.xml",
        junit: { totalTests: 10, passedTests: 10, failedTests: [] },
      },
      {
        phoneName: "Samsung Galaxy S25",
        phoneSlug: "Samsung-Galaxy-S25",
        result: "ERRORED",
        deviceMinutes: 1.5,
        outputTailLines: ["adb: offline", "`".repeat(100_000)],
      },
    ]);
    assert.equal(out.title, "Failed (1/2 phones passed)");
    assert.match(out.summary, /\| Google Pixel 9 \| PASSED \| 4\.2 \|/);
    assert.match(out.summary, /\[junit\.xml\]\(https:\/\/cdn\.example\/junit\.xml\)/);
    assert.ok(out.text.length <= MAX_CHECK_OUTPUT_LEN);
    assert.match(out.text, /\n~~~\nadb: offline/);

    const truncated = truncateMarkdown("a".repeat(MAX_CHECK_OUTPUT_LEN + 500));
    assert.ok(truncated.length <= MAX_CHECK_OUTPUT_LEN);
    assert.match(truncated, /truncated/);
  });
});
