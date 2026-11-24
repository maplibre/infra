package github

import (
	"bytes"
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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/maplibre/infra/device-farm/internal/secrets"
)

const (
	defaultAPIBaseURL  = "https://api.github.com"
	githubAPIVersion   = "2022-11-28"
	userAgent          = "maplibre-device-farm-orchestrator"
	maxRawFileBytes    = 2 << 20 // 2 MiB
	maxAPIErrorBodyLen = 1024
)

// ErrArtifactExpired indicates a GitHub Actions artifact is missing or expired (404/410).
var ErrArtifactExpired = errors.New("github actions artifact expired or not found")

// CredentialsProvider supplies GitHub App credentials.
type CredentialsProvider interface {
	Get(ctx context.Context) (secrets.AppCredentials, error)
}

// CheckRunOutput is the Markdown output attached to a GitHub Check Run.
type CheckRunOutput struct {
	Title   string `json:"title"`
	Summary string `json:"summary"`
	Text    string `json:"text,omitempty"`
}

// CreateCheckRunRequest is the payload for POST /repos/{owner}/{repo}/check-runs.
type CreateCheckRunRequest struct {
	Name        string          `json:"name"`
	HeadSHA     string          `json:"head_sha"`
	DetailsURL  string          `json:"details_url,omitempty"`
	ExternalID  string          `json:"external_id,omitempty"`
	Status      string          `json:"status,omitempty"`
	StartedAt   string          `json:"started_at,omitempty"`
	Conclusion  string          `json:"conclusion,omitempty"`
	CompletedAt string          `json:"completed_at,omitempty"`
	Output      *CheckRunOutput `json:"output,omitempty"`
}

// UpdateCheckRunRequest is the payload for PATCH /repos/{owner}/{repo}/check-runs/{id}.
type UpdateCheckRunRequest struct {
	Name        string          `json:"name,omitempty"`
	DetailsURL  string          `json:"details_url,omitempty"`
	ExternalID  string          `json:"external_id,omitempty"`
	Status      string          `json:"status,omitempty"`
	Conclusion  string          `json:"conclusion,omitempty"`
	CompletedAt string          `json:"completed_at,omitempty"`
	Output      *CheckRunOutput `json:"output,omitempty"`
}

// CheckRun represents a GitHub Check Run resource.
type CheckRun struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	HeadSHA     string `json:"head_sha"`
	ExternalID  string `json:"external_id"`
	Status      string `json:"status"`
	Conclusion  string `json:"conclusion"`
	DetailsURL  string `json:"details_url"`
	HTMLURL     string `json:"html_url"`
	StartedAt   string `json:"started_at"`
	CompletedAt string `json:"completed_at"`
}

// WorkflowRun represents a GitHub Actions workflow run.
type WorkflowRun struct {
	ID             int64     `json:"id"`
	RunAttempt     int       `json:"run_attempt"`
	Name           string    `json:"name"`
	HeadBranch     string    `json:"head_branch"`
	HeadSHA        string    `json:"head_sha"`
	Event          string    `json:"event"`
	Status         string    `json:"status"`
	Conclusion     string    `json:"conclusion"`
	CreatedAt      time.Time `json:"created_at"`
	HTMLURL        string    `json:"html_url"`
	HeadRepository struct {
		FullName string `json:"full_name"`
	} `json:"head_repository"`
}

// WorkflowJob represents a single job within a GitHub Actions workflow run.
type WorkflowJob struct {
	ID         int64  `json:"id"`
	RunID      int64  `json:"run_id"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	HTMLURL    string `json:"html_url"`
}

// Artifact represents a GitHub Actions artifact.
type Artifact struct {
	ID          int64     `json:"id"`
	Name        string    `json:"name"`
	SizeInBytes int64     `json:"size_in_bytes"`
	Expired     bool      `json:"expired"`
	CreatedAt   time.Time `json:"created_at"`
}

type cachedToken struct {
	token     string
	expiresAt time.Time
}

// Client talks to the GitHub REST API as a GitHub App installation.
type Client struct {
	apiBaseURL       string
	creds            CredentialsProvider
	httpClient       *http.Client
	noRedirectClient *http.Client
	downloadClient   *http.Client
	now              func() time.Time

	mu     sync.Mutex
	tokens map[int64]cachedToken
}

// NewClient constructs a GitHub REST API client backed by a GitHub App.
func NewClient(creds CredentialsProvider) *Client {
	return &Client{
		apiBaseURL: defaultAPIBaseURL,
		creds:      creds,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		noRedirectClient: &http.Client{
			Timeout: 30 * time.Second,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		downloadClient: &http.Client{
			Timeout: 3 * time.Minute,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 10 {
					return errors.New("stopped after 10 redirects")
				}
				// Never forward GitHub Authorization headers on artifact downloads.
				req.Header.Del("Authorization")
				return nil
			},
		},
		now:    time.Now,
		tokens: make(map[int64]cachedToken),
	}
}

// SignAppJWT signs a short-lived RS256 JWT for GitHub App authentication using stdlib crypto/rsa.
func SignAppJWT(appID, privateKeyPEM string, now time.Time) (string, error) {
	if appID == "" {
		return "", errors.New("empty GitHub App ID")
	}
	key, err := parseRSAPrivateKey(privateKeyPEM)
	if err != nil {
		return "", err
	}

	headerBytes, err := json.Marshal(struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}{Alg: "RS256", Typ: "JWT"})
	if err != nil {
		return "", err
	}

	claimsBytes, err := json.Marshal(struct {
		IAT int64  `json:"iat"`
		EXP int64  `json:"exp"`
		ISS string `json:"iss"`
	}{
		IAT: now.Add(-60 * time.Second).Unix(),
		EXP: now.Add(9 * time.Minute).Unix(),
		ISS: appID,
	})
	if err != nil {
		return "", err
	}

	signingInput := base64.RawURLEncoding.EncodeToString(headerBytes) + "." +
		base64.RawURLEncoding.EncodeToString(claimsBytes)

	digest := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return "", fmt.Errorf("sign jwt: %w", err)
	}

	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func parseRSAPrivateKey(pemText string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemText))
	if block == nil {
		return nil, errors.New("failed to decode private key PEM")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse RSA private key (PKCS#1 and PKCS#8 failed): %w", err)
	}
	rsaKey, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("private key is %T, want *rsa.PrivateKey", parsed)
	}
	return rsaKey, nil
}

func (c *Client) installationToken(ctx context.Context, installationID int64) (string, error) {
	if installationID <= 0 {
		return "", fmt.Errorf("invalid installation_id %d", installationID)
	}

	c.mu.Lock()
	if cached, ok := c.tokens[installationID]; ok && c.now().Add(5*time.Minute).Before(cached.expiresAt) {
		tok := cached.token
		c.mu.Unlock()
		return tok, nil
	}
	c.mu.Unlock()

	creds, err := c.creds.Get(ctx)
	if err != nil {
		return "", err
	}
	jwtTok, err := SignAppJWT(creds.AppID, creds.PrivateKeyPEM, c.now())
	if err != nil {
		return "", err
	}

	endpoint := fmt.Sprintf("%s/app/installations/%d/access_tokens", c.apiBaseURL, installationID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return "", err
	}
	setCommonHeaders(req, jwtTok, "application/vnd.github+json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("exchange installation token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return "", readAPIError(resp)
	}

	var parsed struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", fmt.Errorf("decode installation token response: %w", err)
	}
	if parsed.Token == "" {
		return "", errors.New("empty installation token in GitHub response")
	}
	if parsed.ExpiresAt.IsZero() {
		parsed.ExpiresAt = c.now().Add(55 * time.Minute)
	}

	c.mu.Lock()
	c.tokens[installationID] = cachedToken{token: parsed.Token, expiresAt: parsed.ExpiresAt}
	c.mu.Unlock()

	return parsed.Token, nil
}

func setCommonHeaders(req *http.Request, bearerToken, accept string) {
	req.Header.Set("Authorization", "Bearer "+bearerToken)
	req.Header.Set("Accept", accept)
	req.Header.Set("X-GitHub-Api-Version", githubAPIVersion)
	req.Header.Set("User-Agent", userAgent)
}

func readAPIError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxAPIErrorBodyLen))
	return fmt.Errorf("github API %s %s returned %d: %s", resp.Request.Method, resp.Request.URL.Path, resp.StatusCode, strings.TrimSpace(string(body)))
}

func (c *Client) doJSON(ctx context.Context, installationID int64, method, path string, reqBody, respBody any) error {
	tok, err := c.installationToken(ctx, installationID)
	if err != nil {
		return err
	}

	var bodyReader io.Reader
	if reqBody != nil {
		data, err := json.Marshal(reqBody)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(data)
	}

	endpoint := c.apiBaseURL + path
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bodyReader)
	if err != nil {
		return err
	}
	setCommonHeaders(req, tok, "application/vnd.github+json")
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return readAPIError(resp)
	}
	if respBody != nil {
		if err := json.NewDecoder(resp.Body).Decode(respBody); err != nil {
			return fmt.Errorf("decode %s %s: %w", method, path, err)
		}
	}
	return nil
}

// CreateCheckRun creates a new GitHub Check Run on head_sha.
func (c *Client) CreateCheckRun(ctx context.Context, installationID int64, owner, repo string, in CreateCheckRunRequest) (CheckRun, error) {
	var out CheckRun
	path := fmt.Sprintf("/repos/%s/%s/check-runs", url.PathEscape(owner), url.PathEscape(repo))
	err := c.doJSON(ctx, installationID, http.MethodPost, path, in, &out)
	return out, err
}

// UpdateCheckRun updates an existing GitHub Check Run by ID.
func (c *Client) UpdateCheckRun(ctx context.Context, installationID int64, owner, repo string, checkRunID int64, in UpdateCheckRunRequest) (CheckRun, error) {
	var out CheckRun
	path := fmt.Sprintf("/repos/%s/%s/check-runs/%d", url.PathEscape(owner), url.PathEscape(repo), checkRunID)
	err := c.doJSON(ctx, installationID, http.MethodPatch, path, in, &out)
	return out, err
}

// ListCheckRunsForRef lists check runs matching checkName for a git ref/SHA.
func (c *Client) ListCheckRunsForRef(ctx context.Context, installationID int64, owner, repo, ref, checkName string) ([]CheckRun, error) {
	var all []CheckRun
	for page := 1; page <= 10; page++ {
		q := url.Values{}
		if checkName != "" {
			q.Set("check_name", checkName)
		}
		q.Set("filter", "all")
		q.Set("per_page", "100")
		q.Set("page", fmt.Sprintf("%d", page))

		path := fmt.Sprintf("/repos/%s/%s/commits/%s/check-runs?%s",
			url.PathEscape(owner), url.PathEscape(repo), url.PathEscape(ref), q.Encode())

		var resp struct {
			TotalCount int        `json:"total_count"`
			CheckRuns  []CheckRun `json:"check_runs"`
		}
		if err := c.doJSON(ctx, installationID, http.MethodGet, path, nil, &resp); err != nil {
			return nil, err
		}
		all = append(all, resp.CheckRuns...)
		if len(resp.CheckRuns) < 100 || len(all) >= resp.TotalCount {
			break
		}
	}
	return all, nil
}

// GetWorkflowRun fetches metadata for a GitHub Actions workflow run.
func (c *Client) GetWorkflowRun(ctx context.Context, installationID int64, owner, repo string, runID int64) (WorkflowRun, error) {
	var out WorkflowRun
	path := fmt.Sprintf("/repos/%s/%s/actions/runs/%d", url.PathEscape(owner), url.PathEscape(repo), runID)
	err := c.doJSON(ctx, installationID, http.MethodGet, path, nil, &out)
	return out, err
}

// ListWorkflowRunJobs returns all jobs belonging to a GitHub Actions workflow run
// (scoped to attempt when attempt > 0).
func (c *Client) ListWorkflowRunJobs(ctx context.Context, installationID int64, owner, repo string, runID int64, attempt int) ([]WorkflowJob, error) {
	var all []WorkflowJob
	for page := 1; page <= 10; page++ {
		var path string
		if attempt > 0 {
			path = fmt.Sprintf("/repos/%s/%s/actions/runs/%d/attempts/%d/jobs?per_page=100&page=%d",
				url.PathEscape(owner), url.PathEscape(repo), runID, attempt, page)
		} else {
			path = fmt.Sprintf("/repos/%s/%s/actions/runs/%d/jobs?per_page=100&page=%d",
				url.PathEscape(owner), url.PathEscape(repo), runID, page)
		}
		var resp struct {
			TotalCount int           `json:"total_count"`
			Jobs       []WorkflowJob `json:"jobs"`
		}
		if err := c.doJSON(ctx, installationID, http.MethodGet, path, nil, &resp); err != nil {
			return nil, err
		}
		all = append(all, resp.Jobs...)
		if len(resp.Jobs) < 100 || len(all) >= resp.TotalCount {
			break
		}
	}
	return all, nil
}

// ListWorkflowRunArtifacts lists artifacts matching name for a workflow run.
func (c *Client) ListWorkflowRunArtifacts(ctx context.Context, installationID int64, owner, repo string, runID int64, name string) ([]Artifact, error) {
	q := url.Values{}
	if name != "" {
		q.Set("name", name)
	}
	q.Set("per_page", "100")
	path := fmt.Sprintf("/repos/%s/%s/actions/runs/%d/artifacts?%s",
		url.PathEscape(owner), url.PathEscape(repo), runID, q.Encode())

	var resp struct {
		Artifacts []Artifact `json:"artifacts"`
	}
	if err := c.doJSON(ctx, installationID, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Artifacts, nil
}

// DownloadArtifact streams a GitHub Actions artifact zip to dstPath.
// Redirects to Azure Blob Storage are followed explicitly without sending the GitHub
// Authorization header.
func (c *Client) DownloadArtifact(ctx context.Context, installationID int64, owner, repo string, artifactID int64, dstPath string, maxBytes int64) error {
	tok, err := c.installationToken(ctx, installationID)
	if err != nil {
		return err
	}

	endpoint := fmt.Sprintf("%s/repos/%s/%s/actions/artifacts/%d/zip",
		c.apiBaseURL, url.PathEscape(owner), url.PathEscape(repo), artifactID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	setCommonHeaders(req, tok, "application/vnd.github+json")

	resp, err := c.noRedirectClient.Do(req)
	if err != nil {
		return fmt.Errorf("request artifact download: %w", err)
	}

	switch resp.StatusCode {
	case http.StatusGone, http.StatusNotFound:
		resp.Body.Close()
		return ErrArtifactExpired
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		loc := resp.Header.Get("Location")
		resp.Body.Close()
		if loc == "" {
			return errors.New("artifact redirect missing Location header")
		}
		dlReq, err := http.NewRequestWithContext(ctx, http.MethodGet, loc, nil)
		if err != nil {
			return fmt.Errorf("build artifact redirect request: %w", err)
		}
		dlReq.Header.Set("User-Agent", userAgent)
		dlResp, err := c.downloadClient.Do(dlReq)
		if err != nil {
			return fmt.Errorf("download redirected artifact: %w", err)
		}
		defer dlResp.Body.Close()
		if dlResp.StatusCode == http.StatusGone || dlResp.StatusCode == http.StatusNotFound {
			return ErrArtifactExpired
		}
		if dlResp.StatusCode < 200 || dlResp.StatusCode >= 300 {
			return fmt.Errorf("artifact blob download returned %d", dlResp.StatusCode)
		}
		return writeLimitedFile(dstPath, dlResp.Body, maxBytes)
	case http.StatusOK:
		defer resp.Body.Close()
		return writeLimitedFile(dstPath, resp.Body, maxBytes)
	default:
		defer resp.Body.Close()
		return readAPIError(resp)
	}
}

// GetRawFile fetches the raw contents of a repository file at ref (e.g. "main").
func (c *Client) GetRawFile(ctx context.Context, installationID int64, owner, repo, filePath, ref string) ([]byte, error) {
	tok, err := c.installationToken(ctx, installationID)
	if err != nil {
		return nil, err
	}

	escapedPath := escapeRepoPath(filePath)
	q := url.Values{}
	if ref != "" {
		q.Set("ref", ref)
	}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/contents/%s?%s",
		c.apiBaseURL, url.PathEscape(owner), url.PathEscape(repo), escapedPath, q.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	setCommonHeaders(req, tok, "application/vnd.github.raw+json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, readAPIError(resp)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxRawFileBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxRawFileBytes {
		return nil, fmt.Errorf("file %s exceeds max size %d bytes", filePath, maxRawFileBytes)
	}
	return data, nil
}

func escapeRepoPath(p string) string {
	parts := strings.Split(strings.TrimPrefix(p, "/"), "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

func writeLimitedFile(dstPath string, r io.Reader, maxBytes int64) error {
	f, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer func() {
		if f != nil {
			f.Close()
		}
	}()

	n, err := io.Copy(f, io.LimitReader(r, maxBytes+1))
	if err != nil {
		os.Remove(dstPath)
		return err
	}
	if n > maxBytes {
		os.Remove(dstPath)
		return fmt.Errorf("download exceeds max size %d bytes", maxBytes)
	}
	if err := f.Close(); err != nil {
		os.Remove(dstPath)
		return err
	}
	f = nil
	return nil
}
