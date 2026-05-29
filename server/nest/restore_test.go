package nest

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestRestore_StreamsArchiveIntoVolume(t *testing.T) {
	files := map[string][]byte{
		"server.properties":             []byte("motd=hello"),
		filepath.Join("world", "level"): []byte("level"),
	}
	archive := buildArchive(t, files)
	expectedSha := sha256.Sum256(archive.Bytes())
	expectedShaHex := hex.EncodeToString(expectedSha[:])

	s3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/zstd")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(archive.Bytes())
	}))
	defer s3Server.Close()

	tempDir := t.TempDir()
	volumePath := filepath.Join(tempDir, "newvolume")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sha, err := streamRestore(ctx, volumePath, s3Server.URL, expectedShaHex, "", "")
	if err != nil {
		t.Fatalf("streamRestore returned error: %v", err)
	}
	if sha != expectedShaHex {
		t.Fatalf("expected sha %s, got %s", expectedShaHex, sha)
	}

	for name, expected := range files {
		path := filepath.Join(volumePath, name)
		got, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("read %s: %v", name, err)
			continue
		}
		if !bytes.Equal(got, expected) {
			t.Errorf("%s mismatch: got %q expected %q", name, got, expected)
		}
	}
}

func TestRestore_FailsOnShaMismatch(t *testing.T) {
	archive := buildArchive(t, map[string][]byte{"x": []byte("y")})

	s3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(archive.Bytes())
	}))
	defer s3Server.Close()

	tempDir := t.TempDir()
	volumePath := filepath.Join(tempDir, "newvolume")

	wrongSha := "0000000000000000000000000000000000000000000000000000000000000000"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := streamRestore(ctx, volumePath, s3Server.URL, wrongSha, "", "")
	if err == nil {
		t.Errorf("expected error on sha mismatch, got nil")
	}
	if !errors.Is(err, ErrShaMismatch) {
		t.Errorf("expected ErrShaMismatch in error chain, got: %v", err)
	}
}

func TestRestore_RefusesNonEmptyDestination(t *testing.T) {
	tempDir := t.TempDir()
	volumePath := filepath.Join(tempDir, "newvolume")
	if err := os.MkdirAll(volumePath, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(volumePath, "stale"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	// presigned url is never reached, the destination check fails first
	_, err := streamRestore(context.Background(), volumePath, "http://unused", "00", "", "")
	if err == nil {
		t.Errorf("expected error when destination is non empty")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("expected already exists error, got: %s", err.Error())
	}
}

func TestRestore_RefusesZipSlipEntry(t *testing.T) {
	// craft an archive with a tar entry that escapes the volume via ../
	var buf bytes.Buffer
	zw, _ := zstd.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	_ = tw.WriteHeader(&tar.Header{Name: "../escape", Mode: 0o644, Size: 1, Typeflag: tar.TypeReg})
	_, _ = tw.Write([]byte("x"))
	_ = tw.Close()
	_ = zw.Close()

	expectedSha := sha256.Sum256(buf.Bytes())
	expectedShaHex := hex.EncodeToString(expectedSha[:])

	s3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(buf.Bytes())
	}))
	defer s3Server.Close()

	tempDir := t.TempDir()
	volumePath := filepath.Join(tempDir, "newvolume")

	_, err := streamRestore(context.Background(), volumePath, s3Server.URL, expectedShaHex, "", "")
	if err == nil {
		t.Errorf("expected error on zip slip attempt")
	}
	if !strings.Contains(err.Error(), "escapes volume") {
		t.Errorf("expected escapes volume error, got: %v", err)
	}
}

func TestRestore_EmitsDownloadingProgress(t *testing.T) {
	// build a fixture large enough to guarantee at least one 500ms throttle
	// window elapses during the read loop. 64 KiB of repeated bytes is tiny
	// on disk but the httptest server delivers it synchronously so Read calls
	// will fire at least twice, giving the goroutine time to emit.
	content := bytes.Repeat([]byte("x"), 64*1024)
	files := map[string][]byte{
		"bigfile.bin": content,
	}
	archive := buildArchive(t, files)
	expectedSha := sha256.Sum256(archive.Bytes())
	expectedShaHex := hex.EncodeToString(expectedSha[:])

	presigned := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/zstd")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", archive.Len()))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(archive.Bytes())
	}))
	defer presigned.Close()

	var got []ProgressPayload
	var mu sync.Mutex
	progress := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p ProgressPayload
		_ = json.NewDecoder(r.Body).Decode(&p)
		mu.Lock()
		got = append(got, p)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer progress.Close()

	tempDir := t.TempDir()
	volumePath := filepath.Join(tempDir, "newvolume")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := streamRestore(ctx, volumePath, presigned.URL, expectedShaHex, progress.URL, "")
	require.NoError(t, err)

	// progress posts fire in goroutines; give them a beat to land.
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, got, "expected at least one progress payload")
	require.Equal(t, "downloading", got[0].Step)
	require.Greater(t, got[len(got)-1].Bytes, int64(0))
}

// tarZstFixture builds a minimal tar.zst archive containing a single file and
// returns (expectedShaHex, archiveBytes). shared by tests that need a valid
// archive + matching sha without caring about the contents.
func tarZstFixture(t *testing.T) (string, []byte) {
	t.Helper()
	buf := buildArchive(t, map[string][]byte{"file.txt": []byte("fixture")})
	raw := buf.Bytes()
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), raw
}

type fakeController struct {
	startErr     error
	runningAfter int
	calls        int
	startedCount int
}

func (f *fakeController) Start(_ context.Context) error { f.startedCount++; return f.startErr }
func (f *fakeController) Running() bool {
	f.calls++
	return f.calls >= f.runningAfter
}

func TestRestoreAndBoot_PostsSuccessAfterRunning(t *testing.T) {
	expectedShaHex, archive := tarZstFixture(t)
	s3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(archive) }))
	defer s3Server.Close()

	var steps []string
	progressServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p ProgressPayload
		_ = decodeJsonBody(r, &p)
		steps = append(steps, p.Step)
		w.WriteHeader(204)
	}))
	defer progressServer.Close()

	var cb CallbackPayload
	cbServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = decodeJsonBody(r, &cb); w.WriteHeader(200) }))
	defer cbServer.Close()

	ctrl := &fakeController{runningAfter: 2}
	err := RestoreAndBoot(context.Background(), ctrl, t.TempDir()+"/vol", s3Server.URL, expectedShaHex, cbServer.URL, progressServer.URL, "")
	if err != nil {
		t.Fatalf("RestoreAndBoot returned error: %v", err)
	}
	if !cb.Success {
		t.Fatalf("expected success callback, got error: %s", cb.ErrorMessage)
	}
	if ctrl.startedCount != 1 {
		t.Fatalf("expected one start, got %d", ctrl.startedCount)
	}
	if !slices.Contains(steps, "starting") {
		t.Fatalf("expected a starting progress tick, got %v", steps)
	}
}

func TestRestoreAndBoot_PostsFailureWhenStartErrors(t *testing.T) {
	expectedShaHex, archive := tarZstFixture(t)
	s3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(archive) }))
	defer s3Server.Close()
	var cb CallbackPayload
	cbServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = decodeJsonBody(r, &cb); w.WriteHeader(200) }))
	defer cbServer.Close()

	ctrl := &fakeController{startErr: errors.New("docker said no")}
	err := RestoreAndBoot(context.Background(), ctrl, t.TempDir()+"/vol", s3Server.URL, expectedShaHex, cbServer.URL, "", "")
	if err != nil {
		t.Fatalf("delivery error: %v", err)
	}
	if cb.Success {
		t.Fatalf("expected failure callback on start error")
	}
}

func TestRestoreAndBoot_PostsFailureOnBootTimeout(t *testing.T) {
	expectedShaHex, archive := tarZstFixture(t)
	s3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(archive) }))
	defer s3Server.Close()
	var cb CallbackPayload
	cbServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = decodeJsonBody(r, &cb); w.WriteHeader(200) }))
	defer cbServer.Close()

	ctrl := &fakeController{runningAfter: 1_000_000}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()

	err := RestoreAndBoot(ctx, ctrl, t.TempDir()+"/vol", s3Server.URL, expectedShaHex, cbServer.URL, "", "")
	if err != nil {
		t.Fatalf("delivery error: %v", err)
	}
	if cb.Success {
		t.Fatalf("expected failure callback on boot timeout")
	}
}

func TestPostCallback_RetriesOnTransientFailure(t *testing.T) {
	old := CallbackRetryBackoff
	CallbackRetryBackoff = time.Millisecond
	defer func() { CallbackRetryBackoff = old }()

	var attempts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 2 {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	if err := postCallback(srv.URL, "", CallbackPayload{Success: true}); err != nil {
		t.Fatalf("expected eventual success, got %v", err)
	}
	if attempts < 2 {
		t.Fatalf("expected a retry, only %d attempt(s)", attempts)
	}
}

func TestPostCallback_ReturnsErrorWhenAllAttemptsFail(t *testing.T) {
	old := CallbackRetryBackoff
	CallbackRetryBackoff = time.Millisecond
	defer func() { CallbackRetryBackoff = old }()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	if err := postCallback(srv.URL, "", CallbackPayload{Success: true}); err == nil {
		t.Fatalf("expected an error after exhausting retries")
	}
}

// buildArchive builds an in-memory tar.zst from name -> content map for tests.
func buildArchive(t *testing.T, files map[string][]byte) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(zw)
	for name, content := range files {
		hdr := &tar.Header{
			Name:     name,
			Mode:     0o644,
			Size:     int64(len(content)),
			Typeflag: tar.TypeReg,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(content); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return &buf
}
