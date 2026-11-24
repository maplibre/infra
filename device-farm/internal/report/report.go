package report

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
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
	// MaxZipEntries caps the number of central directory entries allowed in untrusted ZIP archives.
	MaxZipEntries = 10_000
	// MaxZipCentralDirBytes caps the central directory byte size allowed in untrusted ZIP archives.
	MaxZipCentralDirBytes = 2 * 1024 * 1024 // 2 MiB

	zipDirectoryHeaderSignature = 0x02014b50
	zipDirectoryEndSignature    = 0x06054b50
	zipDirectory64LocSignature  = 0x07064b50
	zipDirectory64EndSignature  = 0x06064b50
	zipDirectoryEndLen          = 22
	zipDirectory64LocLen        = 20
	zipDirectory64EndLen        = 56
	zipMaxCommentLen            = 0xffff
	// maxZipTrailerBytes bounds the maximum bytes permitted after the central directory
	// (ZIP64 EOCD + ZIP64 locator + EOCD + max 64 KiB comment + small extensible data sector).
	maxZipTrailerBytes = zipDirectory64EndLen + zipDirectory64LocLen + zipDirectoryEndLen + zipMaxCommentLen + 4096
)

// ErrPackageNotFound indicates the expected package filename wasn't found inside the outer artifact zip.
var ErrPackageNotFound = errors.New("test package not found inside GitHub Actions artifact zip")

// ErrInvalidArchive indicates a ZIP archive is malformed or exceeds metadata safety bounds.
var ErrInvalidArchive = errors.New("invalid or oversized zip archive")

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

// openBoundedZip inspects ZIP EOCD / ZIP64 EOCD metadata before calling zip.NewReader
// so untrusted archives with forged or oversized central directories cannot allocate
// unbounded memory while reading the central directory.
func openBoundedZip(zipPath string, maxEntries, maxCentralDirBytes uint64) (*zip.Reader, *os.File, error) {
	f, err := os.Open(zipPath)
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	size := info.Size()
	if err := validateZipMetadata(f, size, maxEntries, maxCentralDirBytes); err != nil {
		f.Close()
		return nil, nil, err
	}
	zr, err := zip.NewReader(f, size)
	if err != nil {
		f.Close()
		if errors.Is(err, zip.ErrFormat) || errors.Is(err, zip.ErrInsecurePath) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, nil, fmt.Errorf("%w: %w", ErrInvalidArchive, err)
		}
		return nil, nil, err
	}
	if uint64(len(zr.File)) > maxEntries {
		f.Close()
		return nil, nil, fmt.Errorf("%w: zip contains %d entries (max %d)", ErrInvalidArchive, len(zr.File), maxEntries)
	}
	return zr, f, nil
}

func validateZipMetadata(r io.ReaderAt, size int64, maxEntries, maxCentralDirBytes uint64) error {
	if size < zipDirectoryEndLen {
		return fmt.Errorf("%w: file too small (%d bytes)", ErrInvalidArchive, size)
	}

	// Match archive/zip.readDirectoryEnd: scan last 1 KiB, then last 65 KiB (covers 65535 + 22).
	var eocdBuf []byte
	var eocdOffset int64
	for i, bLen := range []int64{1024, 65 * 1024} {
		if bLen > size {
			bLen = size
		}
		buf := make([]byte, int(bLen))
		if _, err := r.ReadAt(buf, size-bLen); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if p := findZipEOCDSignature(buf); p >= 0 {
			eocdBuf = buf[p:]
			eocdOffset = size - bLen + int64(p)
			break
		}
		if i == 1 || bLen == size {
			return fmt.Errorf("%w: end of central directory not found", ErrInvalidArchive)
		}
	}

	dirRecordsThisDisk := uint64(binary.LittleEndian.Uint16(eocdBuf[8:10]))
	directoryRecords := uint64(binary.LittleEndian.Uint16(eocdBuf[10:12]))
	directorySize := uint64(binary.LittleEndian.Uint32(eocdBuf[12:16]))
	directoryOffset := uint64(binary.LittleEndian.Uint32(eocdBuf[16:20]))
	commentLen := int(binary.LittleEndian.Uint16(eocdBuf[20:22]))
	if commentLen > len(eocdBuf)-zipDirectoryEndLen {
		return fmt.Errorf("%w: invalid comment length", ErrInvalidArchive)
	}

	needsZip64 := directoryRecords == 0xffff || dirRecordsThisDisk == 0xffff ||
		directorySize == 0xffffffff || directoryOffset == 0xffffffff

	// Check for a ZIP64 EOCD locator immediately preceding EOCD.
	var hasZip64Loc bool
	var locBuf [zipDirectory64LocLen]byte
	locOffset := eocdOffset - zipDirectory64LocLen
	if locOffset >= 0 {
		if _, err := r.ReadAt(locBuf[:], locOffset); err != nil {
			return err
		}
		hasZip64Loc = binary.LittleEndian.Uint32(locBuf[0:4]) == zipDirectory64LocSignature
	}

	if needsZip64 && !hasZip64Loc {
		return fmt.Errorf("%w: missing zip64 end of central directory locator", ErrInvalidArchive)
	}

	directoryEndOffset := eocdOffset
	cdEndOffset := eocdOffset
	if hasZip64Loc {
		locDisk := binary.LittleEndian.Uint32(locBuf[4:8])
		zip64EndOffset := binary.LittleEndian.Uint64(locBuf[8:16])
		totalDisks := binary.LittleEndian.Uint32(locBuf[16:20])
		if locDisk != 0 || totalDisks != 1 {
			return fmt.Errorf("%w: unsupported multi-disk zip64 archive", ErrInvalidArchive)
		}
		if zip64EndOffset > uint64(locOffset) || locOffset-int64(zip64EndOffset) < zipDirectory64EndLen {
			return fmt.Errorf("%w: invalid zip64 end of central directory offset", ErrInvalidArchive)
		}

		var z64 [zipDirectory64EndLen]byte
		if _, err := r.ReadAt(z64[:], int64(zip64EndOffset)); err != nil {
			return err
		}
		if binary.LittleEndian.Uint32(z64[0:4]) != zipDirectory64EndSignature {
			return fmt.Errorf("%w: invalid zip64 end of central directory signature", ErrInvalidArchive)
		}
		z64Size := binary.LittleEndian.Uint64(z64[4:12])
		if z64Size < zipDirectory64EndLen-12 || z64Size > uint64(locOffset)-zip64EndOffset-12 {
			return fmt.Errorf("%w: invalid zip64 end of central directory size", ErrInvalidArchive)
		}
		z64DiskNbr := binary.LittleEndian.Uint32(z64[16:20])
		z64DirDiskNbr := binary.LittleEndian.Uint32(z64[20:24])
		if z64DiskNbr != 0 || z64DirDiskNbr != 0 {
			return fmt.Errorf("%w: unsupported multi-disk zip64 archive", ErrInvalidArchive)
		}
		z64DirRecordsThisDisk := binary.LittleEndian.Uint64(z64[24:32])
		z64DirectoryRecords := binary.LittleEndian.Uint64(z64[32:40])
		z64DirectorySize := binary.LittleEndian.Uint64(z64[40:48])
		z64DirectoryOffset := binary.LittleEndian.Uint64(z64[48:56])

		if z64DirectoryRecords > maxEntries || z64DirRecordsThisDisk > maxEntries {
			return fmt.Errorf("%w: zip64 archive has too many entries (%d > %d)", ErrInvalidArchive, max(z64DirectoryRecords, z64DirRecordsThisDisk), maxEntries)
		}
		if z64DirectorySize > maxCentralDirBytes {
			return fmt.Errorf("%w: zip64 central directory too large (%d > %d bytes)", ErrInvalidArchive, z64DirectorySize, maxCentralDirBytes)
		}

		cdEndOffset = int64(zip64EndOffset)
		if needsZip64 {
			directoryEndOffset = int64(zip64EndOffset)
			dirRecordsThisDisk = z64DirRecordsThisDisk
			directoryRecords = z64DirectoryRecords
			directorySize = z64DirectorySize
			directoryOffset = z64DirectoryOffset
		}
	}

	if directoryRecords > maxEntries || dirRecordsThisDisk > maxEntries {
		return fmt.Errorf("%w: zip archive has too many entries (%d > %d)", ErrInvalidArchive, max(directoryRecords, dirRecordsThisDisk), maxEntries)
	}
	if directorySize > maxCentralDirBytes {
		return fmt.Errorf("%w: central directory too large (%d > %d bytes)", ErrInvalidArchive, directorySize, maxCentralDirBytes)
	}

	if directorySize > uint64(directoryEndOffset) || directoryOffset > uint64(directoryEndOffset)-directorySize {
		return fmt.Errorf("%w: central directory extends past end of directory", ErrInvalidArchive)
	}
	baseOffset := directoryEndOffset - int64(directorySize) - int64(directoryOffset)
	cdStart := baseOffset + int64(directoryOffset)
	if cdStart < 0 || cdStart >= size {
		return fmt.Errorf("%w: central directory offset out of range", ErrInvalidArchive)
	}

	// Match archive/zip.readDirectoryEnd's baseOffset > 0 fallback: if a directory
	// header signature sits at raw directoryOffset, zip.NewReader may reset baseOffset=0
	// and scan from directoryOffset instead of baseOffset+directoryOffset.
	if baseOffset > 0 && int64(directoryOffset) <= size-4 {
		var sigBuf [4]byte
		if n, err := r.ReadAt(sigBuf[:], int64(directoryOffset)); (err == nil || (errors.Is(err, io.EOF) && n == len(sigBuf))) &&
			binary.LittleEndian.Uint32(sigBuf[:]) == zipDirectoryHeaderSignature {
			cdStart = int64(directoryOffset)
		}
	}

	if cdStart > cdEndOffset {
		return fmt.Errorf("%w: central directory starts past directory end", ErrInvalidArchive)
	}
	if uint64(cdEndOffset-cdStart) > maxCentralDirBytes {
		return fmt.Errorf("%w: central directory span %d exceeds %d bytes", ErrInvalidArchive, cdEndOffset-cdStart, maxCentralDirBytes)
	}
	if uint64(size-cdStart) > maxCentralDirBytes+maxZipTrailerBytes {
		return fmt.Errorf("%w: archive tail from central directory (%d bytes) exceeds limit", ErrInvalidArchive, size-cdStart)
	}

	return nil
}

// findZipEOCDSignature mirrors archive/zip.findSignatureInBlock exactly so EOCD
// discovery matches zip.NewReader without parser differentials.
func findZipEOCDSignature(b []byte) int {
	for i := len(b) - zipDirectoryEndLen; i >= 0; i-- {
		if b[i] == 'P' && b[i+1] == 'K' && b[i+2] == 0x05 && b[i+3] == 0x06 {
			n := int(b[i+zipDirectoryEndLen-2]) | int(b[i+zipDirectoryEndLen-1])<<8
			if n+zipDirectoryEndLen+i > len(b) {
				return -1
			}
			return i
		}
	}
	return -1
}

// ExtractPackageFromArtifactZip extracts targetFilename (e.g. "android-tests.zip")
// from the outer GitHub Actions artifact zip into dstPath without inspecting,
// unpacking, or executing anything inside targetFilename.
func ExtractPackageFromArtifactZip(outerZipPath, targetFilename, dstPath string, maxBytes int64) error {
	zr, f, err := openBoundedZip(outerZipPath, MaxZipEntries, MaxZipCentralDirBytes)
	if err != nil {
		return fmt.Errorf("open outer artifact zip: %w", err)
	}
	defer f.Close()

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
	zr, f, err := openBoundedZip(zipPath, MaxZipEntries, MaxZipCentralDirBytes)
	if err != nil {
		return nil, false, fmt.Errorf("open customer artifact zip: %w", err)
	}
	defer f.Close()

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
	end := len(data)
	if data[end-1] == '\n' {
		end--
		if end > 0 && data[end-1] == '\r' {
			end--
		}
	}
	if end == 0 {
		return nil
	}
	start := 0
	newlines := 0
	for i := end - 1; i >= 0; i-- {
		if data[i] == '\n' {
			newlines++
			if newlines == n {
				start = i + 1
				break
			}
		}
	}
	text := strings.ReplaceAll(string(data[start:end]), "\r\n", "\n")
	return strings.Split(text, "\n")
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

// writeFencedBlock bounds content and chooses the shorter safe fence (` or ~)
// in a single O(N) pass so untrusted test output cannot break out of the code
// block or trigger superlinear scanning.
func writeFencedBlock(b *strings.Builder, content string) {
	content = TruncateMarkdown(content, MaxGitHubCheckOutputLen)

	var maxBackticks, maxTildes int
	var curBackticks, curTildes int
	for i := 0; i < len(content); i++ {
		switch content[i] {
		case '`':
			curBackticks++
			if curBackticks > maxBackticks {
				maxBackticks = curBackticks
			}
			curTildes = 0
		case '~':
			curTildes++
			if curTildes > maxTildes {
				maxTildes = curTildes
			}
			curBackticks = 0
		default:
			curBackticks = 0
			curTildes = 0
		}
	}

	backtickLen := max(3, maxBackticks+1)
	tildeLen := max(3, maxTildes+1)

	var fence string
	if tildeLen < backtickLen {
		fence = strings.Repeat("~", tildeLen)
	} else {
		fence = strings.Repeat("`", backtickLen)
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
