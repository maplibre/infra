package github

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// VerifySignature validates GitHub's X-Hub-Signature-256 header against body using HMAC-SHA256.
func VerifySignature(secret string, body []byte, signatureHeader string) bool {
	if secret == "" || !strings.HasPrefix(signatureHeader, "sha256=") {
		return false
	}
	sigHex := strings.TrimPrefix(signatureHeader, "sha256=")
	got, err := hex.DecodeString(sigHex)
	if err != nil || len(got) != sha256.Size {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expected := mac.Sum(nil)
	return hmac.Equal(expected, got)
}
