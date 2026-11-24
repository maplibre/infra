package config

import (
	"fmt"
	"os"
	"strings"
)

const (
	ModeWebhook = "webhook"
	ModeWorker  = "worker"
	ModePoller  = "poller"

	DefaultRegion          = "us-west-2"
	DefaultMLNRSProjectARN = "arn:aws:devicefarm:us-west-2:373521797162:project:c5c3f5f6-e21b-41c4-a235-489d30075aeb"
	DeviceGooglePixel9     = "arn:aws:devicefarm:us-west-2::device:6D89F408709B4D3BBBBB17696A24B141"
	DeviceSamsungGalaxyS25 = "arn:aws:devicefarm:us-west-2::device:092D78E7B4694568812D5FF0711CD346"
)

// RepoConfig describes how a single GitHub repository maps to AWS Device Farm.
// To onboard another repository (such as maplibre/maplibre-native), add an entry
// in defaultRepos below.
type RepoConfig struct {
	FullName          string
	Owner             string
	Name              string
	DefaultBranch     string
	ProjectARN        string
	WorkflowName      string
	TriggerJob        string
	ArtifactName      string
	PackageFilename   string
	TestSpecPath      string
	CheckName         string
	JobTimeoutMinutes int32
	Devices           []string
}

// Config holds runtime settings loaded from Lambda environment variables.
type Config struct {
	Mode               string
	Region             string
	GitHubAppSecretARN string
	JobsQueueURL       string
	RunsTableName      string
	ResultsBucketName  string
	ResultsBaseURL     string
	AllowedBranches    map[string]struct{}
	Repos              map[string]RepoConfig
}

// Load reads Config from process environment variables.
func Load() (Config, error) {
	return LoadFromLookup(os.LookupEnv)
}

// LoadFromLookup reads Config using the supplied environment lookup function.
func LoadFromLookup(lookup func(string) (string, bool)) (Config, error) {
	get := func(key string) string {
		v, _ := lookup(key)
		return strings.TrimSpace(v)
	}

	mode := get("MODE")
	switch mode {
	case ModeWebhook, ModeWorker, ModePoller:
	default:
		return Config{}, fmt.Errorf("invalid or missing MODE %q (want %q, %q, or %q)", mode, ModeWebhook, ModeWorker, ModePoller)
	}

	region := get("AWS_REGION")
	if region == "" {
		region = DefaultRegion
	}

	mlnRSProjectARN := get("MLN_RS_PROJECT_ARN")
	if mlnRSProjectARN == "" {
		mlnRSProjectARN = DefaultMLNRSProjectARN
	}

	cfg := Config{
		Mode:               mode,
		Region:             region,
		GitHubAppSecretARN: get("GITHUB_APP_SECRET_ARN"),
		JobsQueueURL:       get("JOBS_QUEUE_URL"),
		RunsTableName:      get("RUNS_TABLE_NAME"),
		ResultsBucketName:  get("RESULTS_BUCKET_NAME"),
		ResultsBaseURL:     strings.TrimRight(get("RESULTS_BASE_URL"), "/"),
		AllowedBranches:    parseBranchSet(get("ALLOWED_BRANCHES")),
		Repos:              defaultRepos(mlnRSProjectARN),
	}

	if cfg.GitHubAppSecretARN == "" {
		return Config{}, fmt.Errorf("GITHUB_APP_SECRET_ARN is required")
	}

	switch cfg.Mode {
	case ModeWebhook:
		if cfg.JobsQueueURL == "" {
			return Config{}, fmt.Errorf("JOBS_QUEUE_URL is required in webhook mode")
		}
	case ModeWorker:
		if cfg.RunsTableName == "" {
			return Config{}, fmt.Errorf("RUNS_TABLE_NAME is required in worker mode")
		}
	case ModePoller:
		if cfg.RunsTableName == "" {
			return Config{}, fmt.Errorf("RUNS_TABLE_NAME is required in poller mode")
		}
		if cfg.ResultsBucketName == "" {
			return Config{}, fmt.Errorf("RESULTS_BUCKET_NAME is required in poller mode")
		}
		if cfg.ResultsBaseURL == "" {
			return Config{}, fmt.Errorf("RESULTS_BASE_URL is required in poller mode")
		}
	}

	return cfg, nil
}

func defaultRepos(mlnRSProjectARN string) map[string]RepoConfig {
	repos := []RepoConfig{
		{
			FullName:          "maplibre/mln-rs",
			Owner:             "maplibre",
			Name:              "mln-rs",
			DefaultBranch:     "main",
			ProjectARN:        mlnRSProjectARN,
			WorkflowName:      "ci",
			TriggerJob:        "prepare-device-tests",
			ArtifactName:      "android-device-tests",
			PackageFilename:   "android-tests.zip",
			TestSpecPath:      "tools/device-farm/android.yml",
			CheckName:         "device-test / android",
			JobTimeoutMinutes: 60,
			Devices: []string{
				DeviceGooglePixel9,     // Google Pixel 9, Mali GPU, Android 16
				DeviceSamsungGalaxyS25, // Samsung Galaxy S25, Adreno GPU, Android 16
			},
		},
	}

	out := make(map[string]RepoConfig, len(repos))
	for _, r := range repos {
		out[strings.ToLower(r.FullName)] = r
	}
	return out
}

func parseBranchSet(raw string) map[string]struct{} {
	if raw == "" {
		return nil
	}
	set := make(map[string]struct{})
	for _, part := range strings.Split(raw, ",") {
		branch := strings.TrimSpace(part)
		if branch != "" {
			set[branch] = struct{}{}
		}
	}
	if len(set) == 0 {
		return nil
	}
	return set
}

// LookupRepo finds the RepoConfig for an "owner/name" repository string.
func (c Config) LookupRepo(fullName string) (RepoConfig, bool) {
	rc, ok := c.Repos[strings.ToLower(strings.TrimSpace(fullName))]
	return rc, ok
}

// BranchAllowed reports whether events for headBranch should be processed.
// When AllowedBranches is empty, all branches are allowed.
func (c Config) BranchAllowed(headBranch string) bool {
	if len(c.AllowedBranches) == 0 {
		return true
	}
	_, ok := c.AllowedBranches[headBranch]
	return ok
}
