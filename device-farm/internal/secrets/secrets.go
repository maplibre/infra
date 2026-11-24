package secrets

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

// AppCredentials holds the GitHub App credentials stored in Secrets Manager.
type AppCredentials struct {
	AppID         string
	PrivateKeyPEM string
	WebhookSecret string
}

type rawCredentials struct {
	AppID         json.RawMessage `json:"app_id"`
	PrivateKeyPEM string          `json:"private_key_pem"`
	WebhookSecret string          `json:"webhook_secret"`
}

// SecretsAPI is the subset of the Secrets Manager client needed by Loader.
type SecretsAPI interface {
	GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}

// Loader fetches and caches GitHub App credentials across warm Lambda invocations.
type Loader struct {
	client    SecretsAPI
	secretARN string
	ttl       time.Duration
	now       func() time.Time

	mu        sync.Mutex
	cached    AppCredentials
	expiresAt time.Time
}

// NewLoader creates a Secrets Manager credential loader with a 5-minute warm cache.
func NewLoader(client SecretsAPI, secretARN string) *Loader {
	return &Loader{
		client:    client,
		secretARN: secretARN,
		ttl:       5 * time.Minute,
		now:       time.Now,
	}
}

// Get returns cached credentials or fetches them from Secrets Manager.
func (l *Loader) Get(ctx context.Context) (AppCredentials, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.expiresAt.IsZero() && l.now().Before(l.expiresAt) {
		return l.cached, nil
	}

	out, err := l.client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(l.secretARN),
	})
	if err != nil {
		return AppCredentials{}, fmt.Errorf("get secret %s: %w", l.secretARN, err)
	}
	if out.SecretString == nil || strings.TrimSpace(*out.SecretString) == "" {
		return AppCredentials{}, fmt.Errorf("secret %s has empty SecretString", l.secretARN)
	}

	creds, err := parseCredentials([]byte(*out.SecretString))
	if err != nil {
		return AppCredentials{}, fmt.Errorf("parse secret %s: %w", l.secretARN, err)
	}

	l.cached = creds
	l.expiresAt = l.now().Add(l.ttl)
	return creds, nil
}

func parseCredentials(data []byte) (AppCredentials, error) {
	var raw rawCredentials
	if err := json.Unmarshal(data, &raw); err != nil {
		return AppCredentials{}, err
	}

	appID, err := parseAppID(raw.AppID)
	if err != nil {
		return AppCredentials{}, fmt.Errorf("app_id: %w", err)
	}

	return AppCredentials{
		AppID:         appID,
		PrivateKeyPEM: raw.PrivateKeyPEM,
		WebhookSecret: raw.WebhookSecret,
	}, nil
}

func parseAppID(raw json.RawMessage) (string, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return "", nil
	}
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		return strings.TrimSpace(asString), nil
	}
	var asInt int64
	if err := json.Unmarshal(raw, &asInt); err == nil {
		return strconv.FormatInt(asInt, 10), nil
	}
	return "", fmt.Errorf("expected string or integer, got %s", trimmed)
}
