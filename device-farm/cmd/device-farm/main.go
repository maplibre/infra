package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/devicefarm"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/maplibre/infra/device-farm/internal/config"
	dfclient "github.com/maplibre/infra/device-farm/internal/devicefarm"
	"github.com/maplibre/infra/device-farm/internal/github"
	"github.com/maplibre/infra/device-farm/internal/poller"
	"github.com/maplibre/infra/device-farm/internal/secrets"
	"github.com/maplibre/infra/device-farm/internal/store"
	"github.com/maplibre/infra/device-farm/internal/webhook"
	"github.com/maplibre/infra/device-farm/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	ctx := context.Background()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		logger.Error("failed to load AWS config", "err", err)
		os.Exit(1)
	}

	credsLoader := secrets.NewLoader(secretsmanager.NewFromConfig(awsCfg), cfg.GitHubAppSecretARN)

	switch cfg.Mode {
	case config.ModeWebhook:
		h := webhook.NewHandler(cfg, credsLoader, sqs.NewFromConfig(awsCfg), logger)
		lambda.Start(h.Handle)

	case config.ModeWorker:
		gh := github.NewClient(credsLoader)
		st := store.New(dynamodb.NewFromConfig(awsCfg), cfg.RunsTableName)
		df := dfclient.NewClient(devicefarm.NewFromConfig(awsCfg))
		h := worker.NewHandler(cfg, gh, st, df, logger)
		lambda.Start(h.Handle)

	case config.ModePoller:
		gh := github.NewClient(credsLoader)
		st := store.New(dynamodb.NewFromConfig(awsCfg), cfg.RunsTableName)
		df := dfclient.NewClient(devicefarm.NewFromConfig(awsCfg))
		s3Client := s3.NewFromConfig(awsCfg)
		h := poller.NewHandler(cfg, gh, st, df, s3Client, logger)
		lambda.Start(h.Handle)

	default:
		logger.Error("unsupported MODE", "mode", cfg.Mode)
		fmt.Fprintf(os.Stderr, "unsupported MODE %q\n", cfg.Mode)
		os.Exit(1)
	}
}
