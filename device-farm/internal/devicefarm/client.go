package devicefarm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/devicefarm"
	"github.com/aws/aws-sdk-go-v2/service/devicefarm/types"
)

// ErrRunNotFound indicates a Device Farm run ARN does not exist.
var ErrRunNotFound = errors.New("device farm run not found")

// ErrUploadRejected indicates Device Farm permanently rejected an uploaded file.
var ErrUploadRejected = errors.New("device farm rejected upload")

// UploadRejectedError carries the upload name and Device Farm's rejection message.
type UploadRejectedError struct {
	Name    string
	Message string
}

func (e *UploadRejectedError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("device farm rejected upload %s: %s", e.Name, e.Message)
	}
	return fmt.Sprintf("device farm rejected upload %s", e.Name)
}

func (e *UploadRejectedError) Unwrap() error {
	return ErrUploadRejected
}

// API is the subset of AWS Device Farm SDK operations used by Client.
type API interface {
	CreateUpload(ctx context.Context, params *devicefarm.CreateUploadInput, optFns ...func(*devicefarm.Options)) (*devicefarm.CreateUploadOutput, error)
	GetUpload(ctx context.Context, params *devicefarm.GetUploadInput, optFns ...func(*devicefarm.Options)) (*devicefarm.GetUploadOutput, error)
	ScheduleRun(ctx context.Context, params *devicefarm.ScheduleRunInput, optFns ...func(*devicefarm.Options)) (*devicefarm.ScheduleRunOutput, error)
	StopRun(ctx context.Context, params *devicefarm.StopRunInput, optFns ...func(*devicefarm.Options)) (*devicefarm.StopRunOutput, error)
	GetRun(ctx context.Context, params *devicefarm.GetRunInput, optFns ...func(*devicefarm.Options)) (*devicefarm.GetRunOutput, error)
	ListJobs(ctx context.Context, params *devicefarm.ListJobsInput, optFns ...func(*devicefarm.Options)) (*devicefarm.ListJobsOutput, error)
	ListArtifacts(ctx context.Context, params *devicefarm.ListArtifactsInput, optFns ...func(*devicefarm.Options)) (*devicefarm.ListArtifactsOutput, error)
}

// ScheduleInput defines parameters to schedule an APPIUM_WEB_NODE run.
type ScheduleInput struct {
	ProjectARN        string
	RunName           string
	Devices           []string
	TestPackageARN    string
	TestSpecARN       string
	JobTimeoutMinutes int32
}

// Client wraps AWS Device Farm operations.
type Client struct {
	api          API
	httpClient   *http.Client
	pollInterval time.Duration
}

// NewClient constructs a Device Farm helper client.
func NewClient(api API) *Client {
	return &Client{
		api:          api,
		httpClient:   &http.Client{Timeout: 3 * time.Minute},
		pollInterval: 2 * time.Second,
	}
}

// ConsoleURL converts a Device Farm run ARN
// (arn:aws:devicefarm:<region>:<account>:run:<project-id>/<run-id>)
// into its AWS console URL.
func ConsoleURL(runARN string) string {
	// Format: arn:aws:devicefarm:us-west-2:373521797162:run:<project-id>/<run-id>
	parts := strings.Split(runARN, ":")
	if len(parts) < 7 {
		return ""
	}
	region := parts[3]
	ids := parts[6]
	projectID, runID, ok := strings.Cut(ids, "/")
	if !ok || projectID == "" || runID == "" {
		return ""
	}
	return fmt.Sprintf("https://%s.console.aws.amazon.com/devicefarm/home?region=%s#/mobile/projects/%s/runs/%s",
		region, region, projectID, runID)
}

// RunIDFromARN extracts the trailing <run-id> from a Device Farm run ARN.
func RunIDFromARN(runARN string) string {
	idx := strings.LastIndex(runARN, "/")
	if idx < 0 || idx+1 >= len(runARN) {
		return runARN
	}
	return runARN[idx+1:]
}

// UploadFile creates a Device Farm upload, PUTs the file to the presigned URL,
// and waits until Device Farm finishes processing it.
func (c *Client) UploadFile(ctx context.Context, projectARN, name string, uploadType types.UploadType, filePath string) (string, error) {
	created, err := c.api.CreateUpload(ctx, &devicefarm.CreateUploadInput{
		ProjectArn: aws.String(projectARN),
		Name:       aws.String(name),
		Type:       uploadType,
	})
	if err != nil {
		return "", fmt.Errorf("create upload %s (%s): %w", name, uploadType, err)
	}
	if created.Upload == nil || created.Upload.Arn == nil || created.Upload.Url == nil {
		return "", fmt.Errorf("create upload %s returned incomplete upload metadata", name)
	}
	uploadARN := *created.Upload.Arn
	presignedURL := *created.Upload.Url

	if err := c.putFile(ctx, presignedURL, filePath); err != nil {
		return "", fmt.Errorf("upload %s to presigned URL: %w", name, err)
	}

	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		out, err := c.api.GetUpload(ctx, &devicefarm.GetUploadInput{
			Arn: aws.String(uploadARN),
		})
		if err != nil {
			return "", fmt.Errorf("get upload %s: %w", uploadARN, err)
		}
		if out.Upload == nil {
			return "", fmt.Errorf("get upload %s returned nil upload", uploadARN)
		}
		switch out.Upload.Status {
		case types.UploadStatusSucceeded:
			return uploadARN, nil
		case types.UploadStatusFailed:
			msg := aws.ToString(out.Upload.Message)
			if msg == "" {
				msg = aws.ToString(out.Upload.Metadata)
			}
			return "", &UploadRejectedError{Name: name, Message: msg}
		}

		select {
		case <-ctx.Done():
			return "", fmt.Errorf("timed out waiting for Device Farm upload %s: %w", name, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (c *Client) putFile(ctx context.Context, presignedURL, filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, presignedURL, f)
	if err != nil {
		return err
	}
	// S3 presigned URLs require Content-Length and reject mismatched Content-Type headers.
	req.ContentLength = st.Size()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// ScheduleRun schedules an APPIUM_WEB_NODE run on the configured device ARNs.
func (c *Client) ScheduleRun(ctx context.Context, in ScheduleInput) (string, error) {
	runName := in.RunName
	if len(runName) > 256 {
		runName = runName[:256]
	}

	out, err := c.api.ScheduleRun(ctx, &devicefarm.ScheduleRunInput{
		ProjectArn: aws.String(in.ProjectARN),
		Name:       aws.String(runName),
		DeviceSelectionConfiguration: &types.DeviceSelectionConfiguration{
			Filters: []types.DeviceFilter{
				{
					Attribute: types.DeviceFilterAttributeArn,
					Operator:  types.RuleOperatorIn,
					Values:    in.Devices,
				},
			},
			MaxDevices: aws.Int32(int32(len(in.Devices))),
		},
		Test: &types.ScheduleRunTest{
			Type:           types.TestTypeAppiumWebNode,
			TestPackageArn: aws.String(in.TestPackageARN),
			TestSpecArn:    aws.String(in.TestSpecARN),
		},
		ExecutionConfiguration: &types.ExecutionConfiguration{
			JobTimeoutMinutes: aws.Int32(in.JobTimeoutMinutes),
			VideoCapture:      aws.Bool(false),
		},
	})
	if err != nil {
		return "", fmt.Errorf("schedule device farm run: %w", err)
	}
	if out.Run == nil || out.Run.Arn == nil {
		return "", errors.New("schedule run returned nil run ARN")
	}
	return *out.Run.Arn, nil
}

// StopRun requests cancellation of an active Device Farm run.
func (c *Client) StopRun(ctx context.Context, runARN string) error {
	_, err := c.api.StopRun(ctx, &devicefarm.StopRunInput{
		Arn: aws.String(runARN),
	})
	if err != nil {
		var notFound *types.NotFoundException
		if errors.As(err, &notFound) {
			return nil
		}
		return fmt.Errorf("stop run %s: %w", runARN, err)
	}
	return nil
}

// GetRun returns the current state of a Device Farm run.
func (c *Client) GetRun(ctx context.Context, runARN string) (*types.Run, error) {
	out, err := c.api.GetRun(ctx, &devicefarm.GetRunInput{
		Arn: aws.String(runARN),
	})
	if err != nil {
		var notFound *types.NotFoundException
		if errors.As(err, &notFound) {
			return nil, ErrRunNotFound
		}
		return nil, fmt.Errorf("get run %s: %w", runARN, err)
	}
	if out.Run == nil {
		return nil, ErrRunNotFound
	}
	return out.Run, nil
}

// ListJobs lists all device jobs belonging to a run (handling pagination).
func (c *Client) ListJobs(ctx context.Context, runARN string) ([]types.Job, error) {
	var all []types.Job
	var nextToken *string
	for {
		out, err := c.api.ListJobs(ctx, &devicefarm.ListJobsInput{
			Arn:       aws.String(runARN),
			NextToken: nextToken,
		})
		if err != nil {
			return nil, fmt.Errorf("list jobs for %s: %w", runARN, err)
		}
		all = append(all, out.Jobs...)
		if out.NextToken == nil || *out.NextToken == "" {
			break
		}
		nextToken = out.NextToken
	}
	return all, nil
}

// ListFileArtifacts lists all FILE artifacts for a Device Farm job (handling pagination).
func (c *Client) ListFileArtifacts(ctx context.Context, jobARN string) ([]types.Artifact, error) {
	var all []types.Artifact
	var nextToken *string
	for {
		out, err := c.api.ListArtifacts(ctx, &devicefarm.ListArtifactsInput{
			Arn:       aws.String(jobARN),
			Type:      types.ArtifactCategoryFile,
			NextToken: nextToken,
		})
		if err != nil {
			return nil, fmt.Errorf("list artifacts for %s: %w", jobARN, err)
		}
		all = append(all, out.Artifacts...)
		if out.NextToken == nil || *out.NextToken == "" {
			break
		}
		nextToken = out.NextToken
	}
	return all, nil
}
