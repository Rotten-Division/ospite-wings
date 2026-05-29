package nest

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

// Controller is the slice of *server.Server the orchestrator needs: start the
// runtime and report whether it has reached running. kept narrow so the
// orchestrator is unit-testable with a fake.
type Controller interface {
	Start(ctx context.Context) error
	Running() bool
}

// RestoreAndBoot streams the archive into the volume, then starts the server
// and waits for it to reach running before posting the completion callback.
// the callback's success therefore means "restored and up", not just
// "restored". a stream, start, or boot-wait failure posts the failure callback
// with a reason. returns the callback POST error only (delivery failure).
func RestoreAndBoot(ctx context.Context, ctrl Controller, volumePath, presignedUrl, expectedSha256, callbackUrl, progressUrl, callbackAuth string) error {
	startedAt := time.Now()

	sha, err := streamRestore(ctx, volumePath, presignedUrl, expectedSha256, progressUrl, callbackAuth)
	if err != nil {
		return postCallback(callbackUrl, callbackAuth, CallbackPayload{
			Success:      false,
			ErrorMessage: err.Error(),
			Sha256:       sha,
			StartedAt:    startedAt,
			FinishedAt:   time.Now(),
		})
	}

	// the stream is on disk; tell the panel we are booting so the lifecycle
	// view advances off the streaming stage. best-effort, never blocks.
	postProgress(progressUrl, callbackAuth, ProgressPayload{Step: "starting"})

	if err := ctrl.Start(ctx); err != nil {
		return postCallback(callbackUrl, callbackAuth, failurePayload(startedAt, fmt.Sprintf("boot failed: %v", err)))
	}

	if err := waitForRunning(ctx, ctrl); err != nil {
		return postCallback(callbackUrl, callbackAuth, failurePayload(startedAt, err.Error()))
	}

	return postCallback(callbackUrl, callbackAuth, CallbackPayload{
		Success:    true,
		Sha256:     sha,
		StartedAt:  startedAt,
		FinishedAt: time.Now(),
	})
}

func waitForRunning(ctx context.Context, ctrl Controller) error {
	deadline := time.Now().Add(BootTimeout)
	ticker := time.NewTicker(BootPollInterval)
	defer ticker.Stop()
	for {
		if ctrl.Running() {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.New("boot wait cancelled before the server reached running")
		case <-ticker.C:
			if time.Now().After(deadline) {
				return errors.New("server did not reach running within the boot timeout")
			}
		}
	}
}

// streamRestore streams a tar.zst archive from presignedUrl into volumePath
// and verifies sha256 matches expectedSha256. bytes flow http body -> zstd.Reader
// -> tar.Reader -> volume files, with the hasher teeing the http body so the
// integrity check covers what came off s3, matching what capture wrote.
//
// progressUrl is optional; when non-empty wings POSTs best-effort downloading
// progress ticks to it during the download leg. errors are swallowed and never
// affect the restore outcome. progressAuth is the Bearer token for those posts.
//
// returns the hex sha256 of the downloaded archive on success, or an error.
// the caller is responsible for posting the completion callback.
func streamRestore(ctx context.Context, volumePath, presignedUrl, expectedSha256, progressUrl, progressAuth string) (string, error) {
	entries, err := os.ReadDir(volumePath)
	if err == nil && len(entries) > 0 {
		return "", fmt.Errorf("%w: %s", ErrVolumeAlreadyExists, volumePath)
	}
	if err != nil && !os.IsNotExist(err) {
		// permission denied, EIO, or anything else short of not exist means
		// we cannot prove the destination is empty. refuse rather than write
		// over whatever might be there.
		return "", fmt.Errorf("stat volume dir %s: %w", volumePath, err)
	}

	if err := os.MkdirAll(volumePath, 0o755); err != nil {
		return "", fmt.Errorf("create volume dir: %w", err)
	}

	dlCtx, cancel := context.WithTimeout(ctx, RestoreDownloadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(dlCtx, http.MethodGet, presignedUrl, nil)
	if err != nil {
		return "", fmt.Errorf("build GET request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrPresignedDownloadFailed, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("%w: status %d", ErrPresignedDownloadFailed, resp.StatusCode)
	}

	total := resp.ContentLength
	if total < 0 {
		total = 0
	}

	pr := &progressReader{
		r:        resp.Body,
		total:    total,
		url:      progressUrl,
		auth:     progressAuth,
		step:     "downloading",
		throttle: 500 * time.Millisecond,
	}

	hasher := sha256.New()
	teeReader := io.TeeReader(pr, hasher)

	zr, err := zstd.NewReader(teeReader)
	if err != nil {
		return "", fmt.Errorf("zstd reader: %w", err)
	}
	defer zr.Close()

	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("tar next: %w", err)
		}

		target := filepath.Join(volumePath, hdr.Name)
		// guard against zip slip: the resolved target must stay inside the
		// volume root. relative paths like ../escape would otherwise write
		// outside the per-server filesystem.
		absVolume, _ := filepath.Abs(volumePath)
		absTarget, _ := filepath.Abs(target)
		rel, err := filepath.Rel(absVolume, absTarget)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return "", fmt.Errorf("tar entry escapes volume: %s", hdr.Name)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode)&0o777); err != nil {
				return "", fmt.Errorf("mkdir %s: %w", target, err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return "", fmt.Errorf("mkdir parent of %s: %w", target, err)
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)&0o777)
			if err != nil {
				return "", fmt.Errorf("open %s: %w", target, err)
			}
			if _, err := io.Copy(f, tr); err != nil {
				_ = f.Close()
				return "", fmt.Errorf("write %s: %w", target, err)
			}
			_ = f.Close()
		default:
			// game server volumes are regular files and directories only,
			// skip symlinks, fifos, devices to avoid surprising the runtime.
		}
	}

	// drain any tail bytes the zstd decoder did not pull, the hasher needs
	// to see every byte that came off the wire to match what s3 holds.
	_, _ = io.Copy(io.Discard, teeReader)

	gotSha := hex.EncodeToString(hasher.Sum(nil))
	if gotSha != expectedSha256 {
		return gotSha, fmt.Errorf("%w: got %s expected %s", ErrShaMismatch, gotSha, expectedSha256)
	}

	return gotSha, nil
}
