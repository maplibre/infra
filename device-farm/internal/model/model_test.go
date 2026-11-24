package model

import "testing"

func TestExternalIDAndParseExternalID(t *testing.T) {
	if got := ExternalID(42, 0); got != "42:1" {
		t.Fatalf("ExternalID(42, 0) = %q, want 42:1", got)
	}
	if got := RerunExternalID(42, 2, "deliv-1"); got != "42:2:deliv-1" {
		t.Fatalf("RerunExternalID(42, 2, deliv-1) = %q, want 42:2:deliv-1", got)
	}
	if got := RerunExternalID(42, 0, "deliv-1"); got != "42:1:deliv-1" {
		t.Fatalf("RerunExternalID(42, 0, deliv-1) = %q, want 42:1:deliv-1", got)
	}
	if got := RerunExternalID(42, 2, ""); got != "42:2" {
		t.Fatalf("RerunExternalID(42, 2, empty) = %q, want 42:2", got)
	}

	valid := []struct {
		raw     string
		wantID  int64
		wantAtt int
	}{
		{raw: "123", wantID: 123, wantAtt: 1},
		{raw: "123:2", wantID: 123, wantAtt: 2},
		{raw: "123:2:deliv-abc", wantID: 123, wantAtt: 2},
		{raw: "123:3:deliv:with:colons", wantID: 123, wantAtt: 3},
	}
	for _, tc := range valid {
		id, att, ok := ParseExternalID(tc.raw)
		if !ok || id != tc.wantID || att != tc.wantAtt {
			t.Errorf("ParseExternalID(%q) = (%d, %d, %v), want (%d, %d, true)", tc.raw, id, att, ok, tc.wantID, tc.wantAtt)
		}
	}

	invalid := []string{
		"",
		"abc",
		"0",
		"-5:1",
		"123:",
		"123:0",
		"123:abc",
		"123:2:",
		"123:abc:deliv",
		"123:0:deliv",
		"0:1:deliv",
	}
	for _, raw := range invalid {
		if id, att, ok := ParseExternalID(raw); ok {
			t.Errorf("ParseExternalID(%q) = (%d, %d, true), want ok=false", raw, id, att)
		}
	}
}
