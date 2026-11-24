package config

import "testing"

func mapLookup(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestLoadFromLookupModes(t *testing.T) {
	t.Run("webhook defaults and branches", func(t *testing.T) {
		cfg, err := LoadFromLookup(mapLookup(map[string]string{
			"MODE":                  "webhook",
			"GITHUB_APP_SECRET_ARN": "arn:aws:secretsmanager:us-west-2:373521797162:secret:gh-app",
			"JOBS_QUEUE_URL":        "https://sqs.us-west-2.amazonaws.com/373521797162/jobs",
			"ALLOWED_BRANCHES":      " ci/device-farm-lambda , main , ",
		}))
		if err != nil {
			t.Fatalf("LoadFromLookup() error = %v", err)
		}
		if cfg.Region != DefaultRegion {
			t.Fatalf("Region = %q, want %q", cfg.Region, DefaultRegion)
		}
		repo, ok := cfg.LookupRepo("MapLibre/MLN-RS")
		if !ok {
			t.Fatal("expected maplibre/mln-rs to be configured")
		}
		if repo.ProjectARN != DefaultMLNRSProjectARN {
			t.Fatalf("ProjectARN = %q, want %q", repo.ProjectARN, DefaultMLNRSProjectARN)
		}
		if len(repo.Devices) != 2 {
			t.Fatalf("len(Devices) = %d, want 2", len(repo.Devices))
		}
		if !cfg.BranchAllowed("ci/device-farm-lambda") || !cfg.BranchAllowed("main") {
			t.Fatal("expected listed branches to be allowed")
		}
		if cfg.BranchAllowed("other-branch") {
			t.Fatal("expected unlisted branch to be rejected")
		}
	})

	t.Run("empty allowed branches allows all", func(t *testing.T) {
		cfg, err := LoadFromLookup(mapLookup(map[string]string{
			"MODE":                  "worker",
			"GITHUB_APP_SECRET_ARN": "arn:secret",
			"RUNS_TABLE_NAME":       "runs",
			"MLN_RS_PROJECT_ARN":    "arn:aws:devicefarm:us-west-2:373521797162:project:custom",
		}))
		if err != nil {
			t.Fatalf("LoadFromLookup() error = %v", err)
		}
		if !cfg.BranchAllowed("anything") || !cfg.BranchAllowed("") {
			t.Fatal("expected all branches allowed when ALLOWED_BRANCHES is empty")
		}
		repo, _ := cfg.LookupRepo("maplibre/mln-rs")
		if repo.ProjectARN != "arn:aws:devicefarm:us-west-2:373521797162:project:custom" {
			t.Fatalf("ProjectARN = %q, want custom", repo.ProjectARN)
		}
	})

	t.Run("poller trims trailing slash on ResultsBaseURL", func(t *testing.T) {
		cfg, err := LoadFromLookup(mapLookup(map[string]string{
			"MODE":                  "poller",
			"GITHUB_APP_SECRET_ARN": "arn:secret",
			"RUNS_TABLE_NAME":       "runs",
			"RESULTS_BUCKET_NAME":   "bucket",
			"RESULTS_BASE_URL":      "https://d1234.cloudfront.net/",
		}))
		if err != nil {
			t.Fatalf("LoadFromLookup() error = %v", err)
		}
		if cfg.ResultsBaseURL != "https://d1234.cloudfront.net" {
			t.Fatalf("ResultsBaseURL = %q, want trimmed", cfg.ResultsBaseURL)
		}
	})

	t.Run("invalid mode errors", func(t *testing.T) {
		if _, err := LoadFromLookup(mapLookup(map[string]string{"MODE": "bogus"})); err == nil {
			t.Fatal("expected error for bogus mode")
		}
	})
}
