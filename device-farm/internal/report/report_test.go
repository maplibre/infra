package report

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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

// buildZip64FromStandard converts a standard single-disk ZIP buffer into a ZIP64 archive
// by inserting a ZIP64 EOCD + ZIP64 EOCD locator right before the standard EOCD.
// If sentinelEOCD is true, the standard EOCD uses 0xffff / 0xffffffff markers.
func buildZip64FromStandard(t *testing.T, std []byte, sentinelEOCD bool, mutateZ64 func(z64 []byte)) []byte {
	t.Helper()
	eocdPos := bytes.LastIndex(std, []byte{'P', 'K', 0x05, 0x06})
	if eocdPos < 0 || eocdPos+zipDirectoryEndLen > len(std) {
		t.Fatalf("standard zip missing EOCD")
	}
	eocd := append([]byte(nil), std[eocdPos:]...)
	recordsThisDisk := uint64(binary.LittleEndian.Uint16(eocd[8:10]))
	recordsTotal := uint64(binary.LittleEndian.Uint16(eocd[10:12]))
	cdSize := uint64(binary.LittleEndian.Uint32(eocd[12:16]))
	cdOffset := uint64(binary.LittleEndian.Uint32(eocd[16:20]))

	z64 := make([]byte, zipDirectory64EndLen)
	binary.LittleEndian.PutUint32(z64[0:4], zipDirectory64EndSignature)
	binary.LittleEndian.PutUint64(z64[4:12], zipDirectory64EndLen-12)
	binary.LittleEndian.PutUint16(z64[12:14], 45)
	binary.LittleEndian.PutUint16(z64[14:16], 45)
	binary.LittleEndian.PutUint32(z64[16:20], 0)
	binary.LittleEndian.PutUint32(z64[20:24], 0)
	binary.LittleEndian.PutUint64(z64[24:32], recordsThisDisk)
	binary.LittleEndian.PutUint64(z64[32:40], recordsTotal)
	binary.LittleEndian.PutUint64(z64[40:48], cdSize)
	binary.LittleEndian.PutUint64(z64[48:56], cdOffset)
	if mutateZ64 != nil {
		mutateZ64(z64)
	}

	loc := make([]byte, zipDirectory64LocLen)
	binary.LittleEndian.PutUint32(loc[0:4], zipDirectory64LocSignature)
	binary.LittleEndian.PutUint32(loc[4:8], 0)
	binary.LittleEndian.PutUint64(loc[8:16], uint64(eocdPos))
	binary.LittleEndian.PutUint32(loc[16:20], 1)

	if sentinelEOCD {
		binary.LittleEndian.PutUint16(eocd[8:10], 0xffff)
		binary.LittleEndian.PutUint16(eocd[10:12], 0xffff)
		binary.LittleEndian.PutUint32(eocd[12:16], 0xffffffff)
		binary.LittleEndian.PutUint32(eocd[16:20], 0xffffffff)
	}

	out := make([]byte, 0, len(std)+len(z64)+len(loc))
	out = append(out, std[:eocdPos]...)
	out = append(out, z64...)
	out = append(out, loc...)
	out = append(out, eocd...)
	return out
}

func TestOpenBoundedZipZIP64(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.zip")
	createZip(t, basePath, map[string][]byte{
		"android-tests.zip": []byte("zip64-inner-payload"),
		"logs/junit.xml":    []byte(`<testsuite tests="1"><testcase classname="a" name="b"/></testsuite>`),
	})
	baseBytes, err := os.ReadFile(basePath)
	if err != nil {
		t.Fatalf("read base zip: %v", err)
	}

	for _, tc := range []struct {
		name         string
		sentinelEOCD bool
	}{
		{name: "valid zip64 with sentinel EOCD", sentinelEOCD: true},
		{name: "valid zip64 with non-sentinel EOCD", sentinelEOCD: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			z64Path := filepath.Join(dir, strings.ReplaceAll(tc.name, " ", "_")+".zip")
			if err := os.WriteFile(z64Path, buildZip64FromStandard(t, baseBytes, tc.sentinelEOCD, nil), 0o600); err != nil {
				t.Fatalf("write zip64: %v", err)
			}

			raw, found, err := ExtractJUnitFromZip(z64Path, DefaultJUnitMaxBytes)
			if err != nil || !found || !strings.Contains(string(raw), `name="b"`) {
				t.Fatalf("ExtractJUnitFromZip(zip64) found=%v err=%v raw=%q", found, err, raw)
			}

			dst := filepath.Join(dir, strings.ReplaceAll(tc.name, " ", "_")+"_out.zip")
			if err := ExtractPackageFromArtifactZip(z64Path, "android-tests.zip", dst, 1024); err != nil {
				t.Fatalf("ExtractPackageFromArtifactZip(zip64) error = %v", err)
			}
			got, err := os.ReadFile(dst)
			if err != nil || string(got) != "zip64-inner-payload" {
				t.Fatalf("extracted = %q err=%v", got, err)
			}
		})
	}

	t.Run("zip64 totalEntries exceeding MaxZipEntries rejected", func(t *testing.T) {
		p := filepath.Join(dir, "z64-too-many-entries.zip")
		data := buildZip64FromStandard(t, baseBytes, true, func(z64 []byte) {
			binary.LittleEndian.PutUint64(z64[24:32], MaxZipEntries+1)
			binary.LittleEndian.PutUint64(z64[32:40], MaxZipEntries+1)
		})
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		_, _, err := ExtractJUnitFromZip(p, DefaultJUnitMaxBytes)
		if !errors.Is(err, ErrInvalidArchive) {
			t.Fatalf("ExtractJUnitFromZip() err = %v, want ErrInvalidArchive", err)
		}
		err = ExtractPackageFromArtifactZip(p, "android-tests.zip", filepath.Join(dir, "unused"), 1024)
		if !errors.Is(err, ErrInvalidArchive) {
			t.Fatalf("ExtractPackageFromArtifactZip() err = %v, want ErrInvalidArchive", err)
		}
	})

	t.Run("zip64 centralDirSize exceeding MaxZipCentralDirBytes rejected", func(t *testing.T) {
		p := filepath.Join(dir, "z64-huge-cd.zip")
		data := buildZip64FromStandard(t, baseBytes, true, func(z64 []byte) {
			binary.LittleEndian.PutUint64(z64[40:48], MaxZipCentralDirBytes+1)
		})
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		_, _, err := ExtractJUnitFromZip(p, DefaultJUnitMaxBytes)
		if !errors.Is(err, ErrInvalidArchive) {
			t.Fatalf("ExtractJUnitFromZip() err = %v, want ErrInvalidArchive", err)
		}
	})

	t.Run("sentinel EOCD without zip64 locator rejected", func(t *testing.T) {
		p := filepath.Join(dir, "missing-z64-locator.zip")
		corrupt := append([]byte(nil), baseBytes...)
		eocdPos := bytes.LastIndex(corrupt, []byte{'P', 'K', 0x05, 0x06})
		binary.LittleEndian.PutUint16(corrupt[eocdPos+10:eocdPos+12], 0xffff)
		if err := os.WriteFile(p, corrupt, 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		_, _, err := ExtractJUnitFromZip(p, DefaultJUnitMaxBytes)
		if !errors.Is(err, ErrInvalidArchive) {
			t.Fatalf("err = %v, want ErrInvalidArchive", err)
		}
	})

	t.Run("zip64 locator pointing to corrupt zip64 EOCD rejected", func(t *testing.T) {
		p := filepath.Join(dir, "bad-z64-sig.zip")
		data := buildZip64FromStandard(t, baseBytes, false, func(z64 []byte) {
			binary.LittleEndian.PutUint32(z64[0:4], 0xdeadbeef)
		})
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		_, _, err := ExtractJUnitFromZip(p, DefaultJUnitMaxBytes)
		if !errors.Is(err, ErrInvalidArchive) {
			t.Fatalf("err = %v, want ErrInvalidArchive", err)
		}
	})
}

func TestOpenBoundedZipStandardLimitsAndMalformed(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.zip")
	createZip(t, basePath, map[string][]byte{
		"android-tests.zip": []byte("payload"),
	})
	baseBytes, err := os.ReadFile(basePath)
	if err != nil {
		t.Fatalf("read base zip: %v", err)
	}

	t.Run("standard EOCD claiming centralDirSize > MaxZipCentralDirBytes rejected", func(t *testing.T) {
		p := filepath.Join(dir, "crafted-huge-cd.zip")
		crafted := append([]byte(nil), baseBytes...)
		eocdPos := bytes.LastIndex(crafted, []byte{'P', 'K', 0x05, 0x06})
		binary.LittleEndian.PutUint32(crafted[eocdPos+12:eocdPos+16], MaxZipCentralDirBytes+1)
		if err := os.WriteFile(p, crafted, 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := ExtractPackageFromArtifactZip(p, "android-tests.zip", filepath.Join(dir, "out"), 1024); !errors.Is(err, ErrInvalidArchive) {
			t.Fatalf("err = %v, want ErrInvalidArchive", err)
		}
	})

	t.Run("standard EOCD claiming totalEntries > MaxZipEntries rejected", func(t *testing.T) {
		p := filepath.Join(dir, "crafted-too-many-entries.zip")
		crafted := append([]byte(nil), baseBytes...)
		eocdPos := bytes.LastIndex(crafted, []byte{'P', 'K', 0x05, 0x06})
		binary.LittleEndian.PutUint16(crafted[eocdPos+8:eocdPos+10], MaxZipEntries+1)
		binary.LittleEndian.PutUint16(crafted[eocdPos+10:eocdPos+12], MaxZipEntries+1)
		if err := os.WriteFile(p, crafted, 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		if _, _, err := ExtractJUnitFromZip(p, DefaultJUnitMaxBytes); !errors.Is(err, ErrInvalidArchive) {
			t.Fatalf("err = %v, want ErrInvalidArchive", err)
		}
	})

	t.Run("real generated zip with MaxZipEntries+1 empty entries rejected", func(t *testing.T) {
		p := filepath.Join(dir, "real-10001-entries.zip")
		f, err := os.Create(p)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		zw := zip.NewWriter(f)
		for i := 0; i < MaxZipEntries+1; i++ {
			if _, err := zw.CreateHeader(&zip.FileHeader{
				Name:   fmt.Sprintf("f%d", i),
				Method: zip.Store,
			}); err != nil {
				t.Fatalf("CreateHeader %d: %v", i, err)
			}
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("zw.Close: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("f.Close: %v", err)
		}

		if _, _, err := ExtractJUnitFromZip(p, DefaultJUnitMaxBytes); !errors.Is(err, ErrInvalidArchive) {
			t.Fatalf("ExtractJUnitFromZip() err = %v, want ErrInvalidArchive", err)
		}
		if err := ExtractPackageFromArtifactZip(p, "android-tests.zip", filepath.Join(dir, "out"), 1024); !errors.Is(err, ErrInvalidArchive) {
			t.Fatalf("ExtractPackageFromArtifactZip() err = %v, want ErrInvalidArchive", err)
		}
	})

	t.Run("forged small directorySize with huge tail from directoryOffset rejected", func(t *testing.T) {
		// Places a CD signature at offset 0 followed by > MaxZipCentralDirBytes + maxZipTrailerBytes
		// of data and an EOCD claiming directoryOffset=0, directorySize=46, directoryRecords=1.
		p := filepath.Join(dir, "forged-small-cdsize.zip")
		totalSize := MaxZipCentralDirBytes + maxZipTrailerBytes + 4096
		buf := make([]byte, totalSize+zipDirectoryEndLen)
		binary.LittleEndian.PutUint32(buf[0:4], zipDirectoryHeaderSignature)
		eocd := buf[totalSize:]
		binary.LittleEndian.PutUint32(eocd[0:4], zipDirectoryEndSignature)
		binary.LittleEndian.PutUint16(eocd[8:10], 1)
		binary.LittleEndian.PutUint16(eocd[10:12], 1)
		binary.LittleEndian.PutUint32(eocd[12:16], 46)
		binary.LittleEndian.PutUint32(eocd[16:20], 0)
		if err := os.WriteFile(p, buf, 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		if _, _, err := ExtractJUnitFromZip(p, DefaultJUnitMaxBytes); !errors.Is(err, ErrInvalidArchive) {
			t.Fatalf("err = %v, want ErrInvalidArchive", err)
		}
	})

	t.Run("non-ZIP and truncated files rejected with ErrInvalidArchive", func(t *testing.T) {
		nonZip := filepath.Join(dir, "not-a-zip.bin")
		if err := os.WriteFile(nonZip, []byte("plain text not a zip archive at all"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		if _, _, err := ExtractJUnitFromZip(nonZip, DefaultJUnitMaxBytes); !errors.Is(err, ErrInvalidArchive) {
			t.Fatalf("non-ZIP err = %v, want ErrInvalidArchive", err)
		}
		if err := ExtractPackageFromArtifactZip(nonZip, "android-tests.zip", filepath.Join(dir, "out"), 1024); !errors.Is(err, ErrInvalidArchive) {
			t.Fatalf("non-ZIP package err = %v, want ErrInvalidArchive", err)
		}

		truncated := filepath.Join(dir, "truncated.zip")
		if err := os.WriteFile(truncated, []byte("PK\x05\x06short"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		if _, _, err := ExtractJUnitFromZip(truncated, DefaultJUnitMaxBytes); !errors.Is(err, ErrInvalidArchive) {
			t.Fatalf("truncated err = %v, want ErrInvalidArchive", err)
		}
	})
}
