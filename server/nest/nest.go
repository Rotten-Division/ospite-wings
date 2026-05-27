package nest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"
)

// http endpoint paths registered in router/router_server_nest.go.
const (
	CapturePath = "/nest/capture"
	RestorePath = "/nest/restore"
)

// CaptureRequest is the body wings receives at POST /api/servers/{uuid}/nest/capture.
// the panel issues this to start an eviction. wings answers 202 immediately and
// runs the streaming capture in a goroutine.
type CaptureRequest struct {
	// PresignedUrl is a single PUT url for the s3 object key the panel chose.
	// wings streams the tar.zst body to this url.
	PresignedUrl string `json:"presigned_url" binding:"required,url"`

	// CallbackUrl is the panel endpoint wings POSTs the completion notice to.
	// shape: https://panel.internal/api/remote/servers/{uuid}/nest/captured.
	CallbackUrl string `json:"callback_url" binding:"required,url"`
}

// RestoreRequest is the body wings receives at POST /api/servers/{uuid}/nest/restore.
type RestoreRequest struct {
	// PresignedUrl is a single GET url for the archive object.
	PresignedUrl string `json:"presigned_url" binding:"required,url"`

	// ExpectedSha256 is the sha256 the panel recorded on the original capture.
	// wings verifies after the streamed extract.
	ExpectedSha256 string `json:"expected_sha256" binding:"required,hexadecimal,len=64"`

	// CallbackUrl is the panel endpoint wings POSTs the completion notice to.
	// shape: https://panel.internal/api/remote/servers/{uuid}/nest/restored.
	CallbackUrl string `json:"callback_url" binding:"required,url"`

	// ProgressUrl is the panel endpoint wings POSTs streaming progress to,
	// shape: https://panel/api/remote/servers/{uuid}/nest-progress. optional;
	// empty disables progress emission.
	ProgressUrl string `json:"progress_url" binding:"omitempty,url"`
}

// CallbackPayload is the json body wings POSTs to the panel callback urls. one
// shape covers both capture and restore, the panel discriminates on success.
type CallbackPayload struct {
	Success      bool      `json:"success"`
	ErrorMessage string    `json:"error_message,omitempty"`
	Size         int64     `json:"size,omitempty"`
	Sha256       string    `json:"sha256,omitempty"`
	StartedAt    time.Time `json:"started_at"`
	FinishedAt   time.Time `json:"finished_at"`
}

const (
	// CallbackTimeout caps the wings → panel callback POST. the callback fires
	// once per capture or restore so the budget can be generous.
	CallbackTimeout = 30 * time.Second

	// CaptureUploadTimeout caps how long wings spends pushing to s3. worst
	// case 10 GB at gigabit lan is roughly 50s, this leaves 10x headroom.
	CaptureUploadTimeout = 10 * time.Minute

	// RestoreDownloadTimeout caps how long wings spends pulling from s3.
	RestoreDownloadTimeout = 10 * time.Minute

	// ProgressTimeout caps each best-effort progress POST. progress is
	// non-critical, a slow panel must not stall the restore stream.
	ProgressTimeout = 5 * time.Second
)

// errors returned by capture and restore goroutines, surfaced to the panel
// via CallbackPayload.ErrorMessage strings.
var (
	ErrPresignedUploadFailed   = errors.New("presigned PUT to s3 failed")
	ErrPresignedDownloadFailed = errors.New("presigned GET from s3 failed")
	ErrShaMismatch             = errors.New("sha256 mismatch on restore")
	ErrVolumeAlreadyExists     = errors.New("volume directory already exists at restore destination")
)

// httpClient is shared by the capture upload, the restore download, and the
// panel callback POST. per-request contexts narrow the timeout to the right
// envelope for each leg.
var httpClient = &http.Client{
	Timeout: 0,
}

// ProgressPayload is the streaming progress wings POSTs to the panel during
// a retrieve. step is "downloading" while the archive streams from s3.
type ProgressPayload struct {
	Step       string `json:"step"`
	Bytes      int64  `json:"bytes"`
	TotalBytes int64  `json:"total_bytes"`
}

// postProgress fires a best-effort progress POST. errors are swallowed,
// progress is advisory and must never affect the restore outcome.
func postProgress(progressUrl, auth string, payload ProgressPayload) {
	if progressUrl == "" {
		return
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), ProgressTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, progressUrl, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
}

// progressReader tallies bytes pulled from r and fires a throttled
// downloading-progress POST. wrapped around the s3 response body so the
// count reflects compressed bytes off the wire (matches the archive_size
// the panel recorded at capture).
// the zstd streaming decoder pulls from this reader on its own goroutine
// while the post-tar tail drain pulls from it on the main goroutine, so the
// byte counter and throttle clock are guarded by mu.
type progressReader struct {
	r        io.Reader
	total    int64
	url      string
	auth     string
	step     string
	throttle time.Duration
	mu       sync.Mutex
	read     int64
	lastEmit time.Time
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)

	p.mu.Lock()
	p.read += int64(n)
	read := p.read
	emit := p.url != "" && time.Since(p.lastEmit) >= p.throttle
	if emit {
		p.lastEmit = time.Now()
	}
	p.mu.Unlock()

	if emit {
		go postProgress(p.url, p.auth, ProgressPayload{Step: p.step, Bytes: read, TotalBytes: p.total})
	}
	return n, err
}
