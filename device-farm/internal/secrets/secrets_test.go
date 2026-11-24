package secrets

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

type fakeSecretsAPI struct {
	payload string
	calls   int
}

func (f *fakeSecretsAPI) GetSecretValue(_ context.Context, _ *secretsmanager.GetSecretValueInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	f.calls++
	return &secretsmanager.GetSecretValueOutput{
		SecretString: aws.String(f.payload),
	}, nil
}

func TestLoaderCachesAndParsesStringOrNumberAppID(t *testing.T) {
	fake := &fakeSecretsAPI{
		payload: `{"app_id": 123456, "private_key_pem": "PEM", "webhook_secret": "shh"}`,
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	loader := NewLoader(fake, "arn:aws:secretsmanager:us-west-2:123:secret:app")
	loader.now = func() time.Time { return now }

	creds, err := loader.Get(context.Background())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if creds.AppID != "123456" || creds.PrivateKeyPEM != "PEM" || creds.WebhookSecret != "shh" {
		t.Fatalf("unexpected creds: %+v", creds)
	}

	// Second call within TTL hits cache.
	now = now.Add(time.Minute)
	if _, err := loader.Get(context.Background()); err != nil {
		t.Fatalf("cached Get() error = %v", err)
	}
	if fake.calls != 1 {
		t.Fatalf("calls = %d, want 1", fake.calls)
	}

	// After TTL, refreshes.
	fake.payload = `{"app_id": "999", "private_key_pem": "PEM2", "webhook_secret": "shh2"}`
	now = now.Add(10 * time.Minute)
	creds, err = loader.Get(context.Background())
	if err != nil {
		t.Fatalf("refreshed Get() error = %v", err)
	}
	if fake.calls != 2 || creds.AppID != "999" || creds.WebhookSecret != "shh2" {
		t.Fatalf("unexpected refreshed result: calls=%d creds=%+v", fake.calls, creds)
	}
}
