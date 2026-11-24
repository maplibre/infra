package github

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maplibre/infra/device-farm/internal/secrets"
)

type staticCreds struct {
	creds secrets.AppCredentials
}

func (s staticCreds) Get(context.Context) (secrets.AppCredentials, error) {
	return s.creds, nil
}

func generateTestKeyPEM(t *testing.T, pkcs8 bool) (*rsa.PrivateKey, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	var block *pem.Block
	if pkcs8 {
		der, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatalf("MarshalPKCS8PrivateKey: %v", err)
		}
		block = &pem.Block{Type: "PRIVATE KEY", Bytes: der}
	} else {
		block = &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}
	}
	return key, string(pem.EncodeToMemory(block))
}

func TestSignAppJWT(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pkcs8 bool
	}{
		{name: "pkcs1", pkcs8: false},
		{name: "pkcs8", pkcs8: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key, pemText := generateTestKeyPEM(t, tc.pkcs8)
			now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

			jwtTok, err := SignAppJWT("424242", pemText, now)
			if err != nil {
				t.Fatalf("SignAppJWT() error = %v", err)
			}
			parts := strings.Split(jwtTok, ".")
			if len(parts) != 3 {
				t.Fatalf("expected 3 JWT parts, got %d", len(parts))
			}

			sig, err := base64.RawURLEncoding.DecodeString(parts[2])
			if err != nil {
				t.Fatalf("decode signature: %v", err)
			}
			digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
			if err := rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, digest[:], sig); err != nil {
				t.Fatalf("RSA signature verification failed: %v", err)
			}

			claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
			if err != nil {
				t.Fatalf("decode claims: %v", err)
			}
			var claims struct {
				ISS string `json:"iss"`
				IAT int64  `json:"iat"`
				EXP int64  `json:"exp"`
			}
			if err := json.Unmarshal(claimsJSON, &claims); err != nil {
				t.Fatalf("unmarshal claims: %v", err)
			}
			if claims.ISS != "424242" {
				t.Fatalf("iss = %q, want 424242", claims.ISS)
			}
			if claims.EXP-claims.IAT != 600 {
				t.Fatalf("exp - iat = %d, want 600", claims.EXP-claims.IAT)
			}
		})
	}
}

func TestDownloadArtifactStripsAuthOnRedirectAndCachesToken(t *testing.T) {
	_, pemText := generateTestKeyPEM(t, false)
	var tokenExchanges atomic.Int32
	var blobGotAuth atomic.Bool

	blobServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			blobGotAuth.Store(true)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte("PK-fake-zip-bytes"))
	}))
	defer blobServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/app/installations/77/access_tokens":
			tokenExchanges.Add(1)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"token":      "ghs_test_token",
				"expires_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
			})
		case "/repos/maplibre/mln-rs/actions/artifacts/99/zip":
			if r.Header.Get("Authorization") != "Bearer ghs_test_token" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			http.Redirect(w, r, blobServer.URL+"/blob.zip?sig=sas", http.StatusFound)
		case "/repos/maplibre/mln-rs/actions/artifacts/100/zip":
			w.WriteHeader(http.StatusGone)
		case "/repos/maplibre/mln-rs/contents/tools/device-farm/android.yml":
			if r.URL.Query().Get("ref") != "main" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte("version: 0.1\n"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer apiServer.Close()

	c := NewClient(staticCreds{creds: secrets.AppCredentials{
		AppID:         "123",
		PrivateKeyPEM: pemText,
	}})
	c.apiBaseURL = apiServer.URL

	dst := filepath.Join(t.TempDir(), "outer.zip")
	if err := c.DownloadArtifact(context.Background(), 77, "maplibre", "mln-rs", 99, dst, 1024); err != nil {
		t.Fatalf("DownloadArtifact() error = %v", err)
	}
	if blobGotAuth.Load() {
		t.Fatal("Authorization header was leaked to redirect target!")
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read downloaded artifact: %v", err)
	}
	if string(got) != "PK-fake-zip-bytes" {
		t.Fatalf("downloaded = %q, want PK-fake-zip-bytes", string(got))
	}

	// Expired artifact returns ErrArtifactExpired.
	err = c.DownloadArtifact(context.Background(), 77, "maplibre", "mln-rs", 100, dst, 1024)
	if !errors.Is(err, ErrArtifactExpired) {
		t.Fatalf("expected ErrArtifactExpired, got %v", err)
	}

	// GetRawFile uses cached installation token and ?ref=main.
	raw, err := c.GetRawFile(context.Background(), 77, "maplibre", "mln-rs", "tools/device-farm/android.yml", "main")
	if err != nil {
		t.Fatalf("GetRawFile() error = %v", err)
	}
	if string(raw) != "version: 0.1\n" {
		t.Fatalf("raw = %q, want version: 0.1\\n", string(raw))
	}
	if tokenExchanges.Load() != 1 {
		t.Fatalf("token exchanges = %d, want 1 (cached)", tokenExchanges.Load())
	}
}
