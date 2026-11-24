package report

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/service/devicefarm/types"
)

const (
	// MaxGitHubCheckOutputLen is GitHub Checks API's limit for output.summary and output.text.
	MaxGitHubCheckOutputLen = 65535
	// DefaultJUnitMaxBytes caps decompressed junit.xml reads to protect against zip bombs.
	DefaultJUnitMaxBytes = 10 << 20 // 10 MiB
	// DefaultTailLines matches mln-rs/tools/device-farm/summary.py.
	DefaultTailLines = 40
)

// ErrPackageNotFound indicates the expected package filename wasn't found inside the outer artifact zip.
var ErrPackageNotFound = errors.New("test package not found inside GitHub Actions artifact zip")

// JUnitSummary holds parsed pass/fail counts and failed test identifiers.
type JUnitSummary struct {
	TotalTests  int
	PassedTests int
	FailedTests []string
}

// PhoneReport holds per-device results and CloudFront download links for Markdown rendering.
type PhoneReport struct {
	PhoneName       string
	PhoneSlug       string
	Result          string
	DeviceMinutes   float64
	OutputURL       string
	ArtifactURL     string
	JUnitURL        string
	JUnit           *JUnitSummary
	OutputTailLines []string
}

// CheckOutput is the rendered GitHub Check Run output.
type CheckOutput struct {
	Title   string
	Summary string
	Text    string
}

// PhoneSlug converts a Device Farm device name (e.g. "Google Pixel 9") into a safe
// S3/URL path segment ("Google-Pixel-9").
func PhoneSlug(name string) string {
	var b strings.Builder
	prevDash := false
	for _, r := range strings.TrimSpace(name) {
		isSafe := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '.'
		if isSafe {
			b.WriteRune(r)
			prevDash = false
		} else if !prevDash {
			b.WriteByte('-')
			prevDash = true
		}
	}
	slug := strings.Trim(b.String(), "-")
	if slug == "" {
		return "device"
	}
	return slug
}

// ExtractPackageFromArtifactZip extracts targetFilename (e.g. "android-tests.zip")
// from the outer GitHub Actions artifact zip into dstPath without inspecting,
// unpacking, or executing anything inside targetFilename.
func ExtractPackageFromArtifactZip(outerZipPath, targetFilename, dstPath string, maxBytes int64) error {
	zr, err := zip.OpenReader(outerZipPath)
	if err != nil {
		return fmt.Errorf("open outer artifact zip: %w", err)
	}
	defer zr.Close()

	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		if f.Name != targetFilename {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return fmt.Errorf("open %s in outer zip: %w", targetFilename, err)
		}
		defer rc.Close()

		out, err := os.Create(dstPath)
		if err != nil {
			return err
		}
		n, copyErr := io.Copy(out, io.LimitReader(rc, maxBytes+1))
		closeErr := out.Close()
		if copyErr != nil {
			os.Remove(dstPath)
			return fmt.Errorf("extract %s: %w", targetFilename, copyErr)
		}
		if closeErr != nil {
			os.Remove(dstPath)
			return closeErr
		}
		if n > maxBytes {
			os.Remove(dstPath)
			return fmt.Errorf("%s exceeds max package size %d bytes", targetFilename, maxBytes)
		}
		return nil
	}

	return fmt.Errorf("%w: %s", ErrPackageNotFound, targetFilename)
}

// ExtractJUnitFromZip safely reads the first junit.xml entry inside CUSTOMER_ARTIFACT.zip
// without extracting arbitrary zip contents to disk.
func ExtractJUnitFromZip(zipPath string, maxBytes int64) ([]byte, bool, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultJUnitMaxBytes
	}
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, false, fmt.Errorf("open customer artifact zip: %w", err)
	}
	defer zr.Close()

	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		if f.Name != "junit.xml" && !strings.HasSuffix(f.Name, "/junit.xml") {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return nil, false, fmt.Errorf("open %s: %w", f.Name, err)
		}
		data, err := io.ReadAll(io.LimitReader(rc, maxBytes+1))
		rc.Close()
		if err != nil {
			return nil, false, fmt.Errorf("read %s: %w", f.Name, err)
		}
		if int64(len(data)) > maxBytes {
			return nil, false, fmt.Errorf("%s exceeds max size %d bytes", f.Name, maxBytes)
		}
		return data, true, nil
	}
	return nil, false, nil
}

type junitTestCase struct {
	ClassName string     `xml:"classname,attr"`
	Name      string     `xml:"name,attr"`
	Failures  []struct{} `xml:"failure"`
	Errors    []struct{} `xml:"error"`
}

type junitTestSuite struct {
	Tests     int              `xml:"tests,attr"`
	TestCases []junitTestCase  `xml:"testcase"`
	Suites    []junitTestSuite `xml:"testsuite"`
}

type junitTestSuites struct {
	Tests  int              `xml:"tests,attr"`
	Suites []junitTestSuite `xml:"testsuite"`
}

// ParseJUnit parses a JUnit XML report (root <testsuites> or <testsuite>)
// matching mln-rs/tools/device-farm/summary.py semantics.
func ParseJUnit(rawXML []byte) (JUnitSummary, error) {
	dec := xml.NewDecoder(bytes.NewReader(rawXML))
	var rootName string
	for {
		tok, err := dec.Token()
		if err != nil {
			return JUnitSummary{}, fmt.Errorf("parse junit xml: %w", err)
		}
		if start, ok := tok.(xml.StartElement); ok {
			rootName = start.Name.Local
			break
		}
	}

	var totalAttr int
	var cases []junitTestCase

	var collectSuite func(s junitTestSuite)
	collectSuite = func(s junitTestSuite) {
		cases = append(cases, s.TestCases...)
		for _, sub := range s.Suites {
			collectSuite(sub)
		}
	}

	switch rootName {
	case "testsuites":
		var doc junitTestSuites
		if err := xml.Unmarshal(rawXML, &doc); err != nil {
			return JUnitSummary{}, fmt.Errorf("unmarshal testsuites: %w", err)
		}
		totalAttr = doc.Tests
		for _, s := range doc.Suites {
			collectSuite(s)
		}
	case "testsuite":
		var doc junitTestSuite
		if err := xml.Unmarshal(rawXML, &doc); err != nil {
			return JUnitSummary{}, fmt.Errorf("unmarshal testsuite: %w", err)
		}
		totalAttr = doc.Tests
		collectSuite(doc)
	default:
		return JUnitSummary{}, fmt.Errorf("unexpected junit root element <%s>", rootName)
	}

	failed := make([]string, 0)
	for _, tc := range cases {
		if len(tc.Failures) > 0 || len(tc.Errors) > 0 {
			failed = append(failed, strings.TrimSpace(tc.ClassName+" "+tc.Name))
		}
	}

	total := totalAttr
	if total == 0 && len(cases) > 0 {
		total = len(cases)
	}
	passed := total - len(failed)
	if passed < 0 {
		passed = 0
	}

	return JUnitSummary{
		TotalTests:  total,
		PassedTests: passed,
		FailedTests: failed,
	}, nil
}

// TailLines returns the last n lines of data, normalizing CRLF line endings.
func TailLines(data []byte, n int) []string {
	if n <= 0 || len(data) == 0 {
		return nil
	}
	text := strings.ReplaceAll(string(data), "\r\n", "\n")
	text = strings.TrimSuffix(text, "\n")
	if text == "" {
		return nil
	}
	lines := strings.Split(text, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return lines
}

// MapRunResult maps an AWS Device Farm ExecutionResult to a GitHub Check Run conclusion.
func MapRunResult(result types.ExecutionResult) string {
	switch result {
	case types.ExecutionResultPassed:
		return "success"
	case types.ExecutionResultStopped:
		return "cancelled"
	default:
		return "failure"
	}
}

// BuildCheckOutput formats the Check Run title, summary table, and per-phone details.
func BuildCheckOutput(runResult types.ExecutionResult, consoleURL string, phones []PhoneReport) CheckOutput {
	passedPhones := 0
	for _, p := range phones {
		if p.Result == string(types.ExecutionResultPassed) {
			passedPhones++
		}
	}

	var title string
	switch runResult {
	case types.ExecutionResultPassed:
		title = fmt.Sprintf("Passed (%d/%d phones)", passedPhones, len(phones))
	case types.ExecutionResultStopped:
		title = "Stopped"
	default:
		title = fmt.Sprintf("Failed (%d/%d phones passed)", passedPhones, len(phones))
	}
	if len(phones) == 0 {
		switch runResult {
		case types.ExecutionResultPassed:
			title = "Passed"
		case types.ExecutionResultStopped:
			title = "Stopped"
		default:
			title = "Failed"
		}
	}

	var summary strings.Builder
	if len(phones) > 0 {
		summary.WriteString("| Phone | Result | Device minutes | Output | Artifacts |\n")
		summary.WriteString("| --- | --- | --- | --- | --- |\n")
		for _, p := range phones {
			outputCell := "—"
			if p.OutputURL != "" {
				outputCell = fmt.Sprintf("[TESTSPEC_OUTPUT.txt](%s)", p.OutputURL)
			}
			var artifactLinks []string
			if p.ArtifactURL != "" {
				artifactLinks = append(artifactLinks, fmt.Sprintf("[CUSTOMER_ARTIFACT.zip](%s)", p.ArtifactURL))
			}
			if p.JUnitURL != "" {
				artifactLinks = append(artifactLinks, fmt.Sprintf("[junit.xml](%s)", p.JUnitURL))
			}
			artifactCell := "—"
			if len(artifactLinks) > 0 {
				artifactCell = strings.Join(artifactLinks, " · ")
			}
			fmt.Fprintf(&summary, "| %s | %s | %.1f | %s | %s |\n",
				escapeTableCell(p.PhoneName),
				escapeTableCell(p.Result),
				p.DeviceMinutes,
				outputCell,
				artifactCell,
			)
		}
	} else {
		summary.WriteString("The run has no phone's results.\n")
	}

	if consoleURL != "" {
		summary.WriteString("\n")
		fmt.Fprintf(&summary, "[View run in AWS Device Farm console](%s)\n", consoleURL)
	}

	var text strings.Builder
	if len(phones) == 0 {
		text.WriteString("The run has no phone's results.\n")
	} else {
		for i, p := range phones {
			if i > 0 {
				text.WriteString("\n")
			}
			fmt.Fprintf(&text, "### %s\n\n", sanitizeInline(p.PhoneName))
			if p.JUnit == nil {
				text.WriteString("The tests didn't run:\n\n")
				writeFencedBlock(&text, strings.Join(p.OutputTailLines, "\n"))
				text.WriteString("\n")
				continue
			}
			fmt.Fprintf(&text, "%d of %d tests passed.\n", p.JUnit.PassedTests, p.JUnit.TotalTests)
			if len(p.JUnit.FailedTests) > 0 {
				text.WriteString("\nFailed:\n\n")
				writeFencedBlock(&text, strings.Join(p.JUnit.FailedTests, "\n"))
				text.WriteString("\n")
			}
		}
	}

	return CheckOutput{
		Title:   title,
		Summary: TruncateMarkdown(summary.String(), MaxGitHubCheckOutputLen),
		Text:    TruncateMarkdown(text.String(), MaxGitHubCheckOutputLen),
	}
}

func escapeTableCell(s string) string {
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "|", `\|`)
	return s
}

func sanitizeInline(s string) string {
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}

// writeFencedBlock chooses a fence longer than any run of backticks in content
// so untrusted test output cannot break out of the Markdown code block.
func writeFencedBlock(b *strings.Builder, content string) {
	fence := "```"
	for strings.Contains(content, fence) {
		fence += "`"
	}
	b.WriteString(fence)
	b.WriteByte('\n')
	if content != "" {
		b.WriteString(content)
		if !strings.HasSuffix(content, "\n") {
			b.WriteByte('\n')
		}
	}
	b.WriteString(fence)
}

// TruncateMarkdown truncates s so its UTF-8 byte/rune length stays within maxLen.
func TruncateMarkdown(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	const suffix = "\n\n... (truncated; see full output in artifacts)\n"
	limit := maxLen - len(suffix)
	if limit <= 0 {
		return s[:maxLen]
	}
	// Back up to a valid UTF-8 rune boundary.
	for limit > 0 && !utf8.RuneStart(s[limit]) {
		limit--
	}
	return s[:limit] + suffix
}
