package report

import (
	"archive/zip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/devicefarm/types"
)

func createZip(t *testing.T, path string, files map[string][]byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create zip: %v", err)
	}
	zw := zip.NewWriter(f)
	for name, content := range files {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatalf("zip create %s: %v", name, err)
		}
		if _, err := w.Write(content); err != nil {
			t.Fatalf("zip write %s: %v", name, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("file close: %v", err)
	}
}

func TestPhoneSlug(t *testing.T) {
	if got := PhoneSlug("Google Pixel 9"); got != "Google-Pixel-9" {
		t.Fatalf("PhoneSlug = %q", got)
	}
	if got := PhoneSlug("Samsung Galaxy S25 / Ultra!"); got != "Samsung-Galaxy-S25-Ultra" {
		t.Fatalf("PhoneSlug = %q", got)
	}
	if got := PhoneSlug("///"); got != "device" {
		t.Fatalf("PhoneSlug = %q", got)
	}
}

func TestExtractPackageFromArtifactZip(t *testing.T) {
	dir := t.TempDir()
	outer := filepath.Join(dir, "android-device-tests.zip")
	innerPayload := []byte("untrusted-inner-zip-bytes")
	createZip(t, outer, map[string][]byte{
		"android-tests.zip": innerPayload,
	})

	dst := filepath.Join(dir, "extracted.zip")
	if err := ExtractPackageFromArtifactZip(outer, "android-tests.zip", dst, 1024); err != nil {
		t.Fatalf("ExtractPackageFromArtifactZip() error = %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read extracted: %v", err)
	}
	if string(got) != string(innerPayload) {
		t.Fatalf("got %q, want %q", got, innerPayload)
	}
}

func TestExtractAndParseJUnit(t *testing.T) {
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "CUSTOMER_ARTIFACT.zip")
	junitXML := `<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="nextest-run" tests="3" failures="1">
  <testsuite name="mln_render" tests="3" failures="1">
    <testcase classname="mln_render::gpu" name="clear_screen"/>
    <testcase classname="mln_render::gpu" name="draw_triangle">
      <failure message="pixel mismatch">diff</failure>
    </testcase>
    <testcase classname="mln_render::gpu" name="offscreen">
      <error message="timeout"/>
    </testcase>
  </testsuite>
</testsuites>`

	createZip(t, zipPath, map[string][]byte{
		"Host_Machine_Files/$DEVICEFARM_LOG_DIR/junit.xml": []byte(junitXML),
	})

	raw, found, err := ExtractJUnitFromZip(zipPath, DefaultJUnitMaxBytes)
	if err != nil || !found {
		t.Fatalf("ExtractJUnitFromZip() found=%v err=%v", found, err)
	}

	summary, err := ParseJUnit(raw)
	if err != nil {
		t.Fatalf("ParseJUnit() error = %v", err)
	}
	if summary.TotalTests != 3 || summary.PassedTests != 1 || len(summary.FailedTests) != 2 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	if summary.FailedTests[0] != "mln_render::gpu draw_triangle" || summary.FailedTests[1] != "mln_render::gpu offscreen" {
		t.Fatalf("unexpected failed list: %+v", summary.FailedTests)
	}
}

func TestBuildCheckOutputAndTruncation(t *testing.T) {
	out := BuildCheckOutput(types.ExecutionResultFailed, "https://us-west-2.console.aws.amazon.com/devicefarm/home", []PhoneReport{
		{
			PhoneName:     "Google Pixel 9",
			PhoneSlug:     "Google-Pixel-9",
			Result:        "PASSED",
			DeviceMinutes: 4.2,
			OutputURL:     "https://cdn.example/TESTSPEC_OUTPUT.txt",
			ArtifactURL:   "https://cdn.example/CUSTOMER_ARTIFACT.zip",
			JUnitURL:      "https://cdn.example/junit.xml",
			JUnit:         &JUnitSummary{TotalTests: 10, PassedTests: 10},
		},
		{
			PhoneName:       "Samsung Galaxy S25",
			PhoneSlug:       "Samsung-Galaxy-S25",
			Result:          "ERRORED",
			DeviceMinutes:   1.5,
			OutputURL:       "https://cdn.example/s25/TESTSPEC_OUTPUT.txt",
			OutputTailLines: []string{"adb: device offline", "```injection```"},
		},
	})

	if out.Title != "Failed (1/2 phones passed)" {
		t.Fatalf("Title = %q", out.Title)
	}
	if !strings.Contains(out.Summary, "| Google Pixel 9 | PASSED | 4.2 |") {
		t.Fatalf("Summary missing pixel row:\n%s", out.Summary)
	}
	if !strings.Contains(out.Summary, "[junit.xml](https://cdn.example/junit.xml)") {
		t.Fatalf("Summary missing junit link:\n%s", out.Summary)
	}
	if !strings.Contains(out.Text, "10 of 10 tests passed.") {
		t.Fatalf("Text missing pass count:\n%s", out.Text)
	}
	if !strings.Contains(out.Text, "The tests didn't run:") || !strings.Contains(out.Text, "~~~\nadb: device offline") {
		t.Fatalf("Text missing fenced fallback output:\n%s", out.Text)
	}

	big := strings.Repeat("a", MaxGitHubCheckOutputLen+500)
	truncated := TruncateMarkdown(big, MaxGitHubCheckOutputLen)
	if len(truncated) > MaxGitHubCheckOutputLen {
		t.Fatalf("truncated length %d > %d", len(truncated), MaxGitHubCheckOutputLen)
	}
	if !strings.Contains(truncated, "truncated") {
		t.Fatal("expected truncation notice")
	}
}

func TestBuildCheckOutputHugeBacktickRun(t *testing.T) {
	hugeBackticks := strings.Repeat("`", 100_000)
	out := BuildCheckOutput(types.ExecutionResultFailed, "", []PhoneReport{
		{
			PhoneName: "Google Pixel 9",
			PhoneSlug: "Google-Pixel-9",
			Result:    "FAILED",
			JUnit: &JUnitSummary{
				TotalTests:  1,
				PassedTests: 0,
				FailedTests: []string{"pkg::mod " + hugeBackticks},
			},
		},
	})

	if len(out.Text) > MaxGitHubCheckOutputLen {
		t.Fatalf("Text length %d exceeds limit %d", len(out.Text), MaxGitHubCheckOutputLen)
	}
	if !strings.Contains(out.Text, "\n~~~\n") {
		t.Fatalf("expected short ~~~ fence for huge backtick run, got prefix: %q", out.Text[:min(200, len(out.Text))])
	}
}

func TestMapRunResult(t *testing.T) {
	if got := MapRunResult(types.ExecutionResultPassed); got != "success" {
		t.Fatalf("PASSED -> %q", got)
	}
	if got := MapRunResult(types.ExecutionResultStopped); got != "cancelled" {
		t.Fatalf("STOPPED -> %q", got)
	}
	if got := MapRunResult(types.ExecutionResultFailed); got != "failure" {
		t.Fatalf("FAILED -> %q", got)
	}
	if got := MapRunResult(types.ExecutionResultErrored); got != "failure" {
		t.Fatalf("ERRORED -> %q", got)
	}
}

func TestTailLinesLargeBufferAndCRLF(t *testing.T) {
	large := []byte(strings.Repeat("\n", 200_000) + "line-a\r\nline-b\r\nline-c\r\n")
	got := TailLines(large, 2)
	want := []string{"line-b", "line-c"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("TailLines(large, 2) = %#v, want %#v", got, want)
	}

	got3 := TailLines(large, 3)
	want3 := []string{"line-a", "line-b", "line-c"}
	if len(got3) != len(want3) || got3[0] != want3[0] || got3[1] != want3[1] || got3[2] != want3[2] {
		t.Fatalf("TailLines(large, 3) = %#v, want %#v", got3, want3)
	}

	// Edge cases matching TrimSuffix(normalized, "\n") semantics.
	if res := TailLines(nil, 5); res != nil {
		t.Fatalf("TailLines(nil, 5) = %#v, want nil", res)
	}
	if res := TailLines([]byte("a\nb"), 0); res != nil {
		t.Fatalf("TailLines(data, 0) = %#v, want nil", res)
	}
	if res := TailLines([]byte("\n"), 5); res != nil {
		t.Fatalf("TailLines(\\n, 5) = %#v, want nil", res)
	}
	if res := TailLines([]byte("\r\n"), 5); res != nil {
		t.Fatalf("TailLines(\\r\\n, 5) = %#v, want nil", res)
	}
	if res := TailLines([]byte("\n\n"), 5); len(res) != 2 || res[0] != "" || res[1] != "" {
		t.Fatalf("TailLines(\\n\\n, 5) = %#v, want []string{\"\", \"\"}", res)
	}
	if res := TailLines([]byte("only-one-line"), 5); len(res) != 1 || res[0] != "only-one-line" {
		t.Fatalf("TailLines(single, 5) = %#v, want [only-one-line]", res)
	}
}
