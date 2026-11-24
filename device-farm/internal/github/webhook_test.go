package github

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestVerifySignature(t *testing.T) {
	secret := "super-secret-value"
	body := []byte(`{"zen":"Keep it logically awesome."}`)

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	validSig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	if !VerifySignature(secret, body, validSig) {
		t.Fatal("expected valid signature to pass")
	}
	if VerifySignature(secret, []byte(`{"zen":"tampered"}`), validSig) {
		t.Fatal("expected tampered body to fail")
	}
	if VerifySignature("wrong-secret", body, validSig) {
		t.Fatal("expected wrong secret to fail")
	}
	if VerifySignature(secret, body, "sha1="+hex.EncodeToString(mac.Sum(nil))) {
		t.Fatal("expected sha1 prefix to fail")
	}
	if VerifySignature(secret, body, "") {
		t.Fatal("expected empty signature to fail")
	}
	if VerifySignature("", body, validSig) {
		t.Fatal("expected empty secret to fail")
	}
}
