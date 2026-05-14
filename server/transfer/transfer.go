package transfer

import (
	"context"
	"time"

	"github.com/apex/log"
	"github.com/mitchellh/colorstring"

	"github.com/pelican-dev/wings/server"
	"github.com/pelican-dev/wings/system"
)

// Status represents the current status of a transfer.
type Status string

// String satisfies the fmt.Stringer interface.
func (s Status) String() string {
	return string(s)
}

// Step represents the current phase within a transfer. Steps progress
// roughly archiving -> uploading -> extracting -> verifying -> cleanup
// across the source and destination nodes, though wings pipelines
// archiving and uploading so they share the StepUploading emission.
type Step string

const (
	StepArchiving  Step = "archiving"
	StepUploading  Step = "uploading"
	StepExtracting Step = "extracting"
	StepVerifying  Step = "verifying"
	StepCleanup    Step = "cleanup"
)

// String satisfies the fmt.Stringer interface.
func (s Step) String() string {
	return string(s)
}

// ProgressPayload is the structured progress event payload emitted from
// the transfer pipeline. Percent is a 0-1 float derived from
// bytes / totalBytes when both are known, otherwise -1 to signal
// indeterminate.
type ProgressPayload struct {
	Step       Step    `json:"step"`
	Bytes      int64   `json:"bytes"`
	TotalBytes int64   `json:"total_bytes"`
	Percent    float64 `json:"percent"`
}

const (
	// StatusPending is the status of a transfer when it is first created.
	StatusPending Status = "pending"
	// StatusProcessing is the status of a transfer when it is currently in
	// progress, such as when the archive is being streamed to the target node.
	StatusProcessing Status = "processing"

	// StatusCancelling is the status of a transfer when it is in the process of
	// being cancelled.
	StatusCancelling Status = "cancelling"

	// StatusCancelled is the final status of a transfer when it has been
	// cancelled.
	StatusCancelled Status = "cancelled"
	// StatusFailed is the final status of a transfer when it has failed.
	StatusFailed Status = "failed"
	// StatusCompleted is the final status of a transfer when it has completed.
	StatusCompleted Status = "completed"
)

// Transfer represents a transfer of a server from one node to another.
type Transfer struct {
	// ctx is the context for the transfer.
	ctx context.Context
	// cancel is used to cancel all ongoing transfer operations for the server.
	cancel *context.CancelFunc

	// Server associated with the transfer.
	Server *server.Server
	// status of the transfer.
	status *system.Atomic[Status]

	// archive is the archive that is being created for the transfer.
	archive *Archive

	// BackupUUIDs is the list of backup UUIDs that should be transferred.
	// If empty, no backups will be transferred.
	BackupUUIDs []string
}

// New returns a new transfer instance for the given server.
func New(ctx context.Context, s *server.Server) *Transfer {
	ctx, cancel := context.WithCancel(ctx)

	return &Transfer{
		ctx:    ctx,
		cancel: &cancel,

		Server: s,
		status: system.NewAtomic(StatusPending),
	}
}

// Context returns the context for the transfer.
func (t *Transfer) Context() context.Context {
	return t.ctx
}

// Cancel cancels the transfer.
func (t *Transfer) Cancel() {
	status := t.Status()
	if status == StatusCancelling ||
		status == StatusCancelled ||
		status == StatusCompleted ||
		status == StatusFailed {
		return
	}

	if t.cancel == nil {
		return
	}

	t.SetStatus(StatusCancelling)
	(*t.cancel)()
}

// Status returns the current status of the transfer.
func (t *Transfer) Status() Status {
	return t.status.Load()
}

// SetStatus sets the status of the transfer.
func (t *Transfer) SetStatus(s Status) {
	// TODO: prevent certain status changes from happening.
	// If we are cancelling, then we can't go back to processing.
	t.status.Store(s)

	t.Server.Events().Publish(server.TransferStatusEvent, s)
}

// newProgressPayload builds a ProgressPayload, computing percent from
// bytes / totalBytes when total is positive and returning -1 to signal
// indeterminate when total is zero or negative.
func newProgressPayload(step Step, bytes, totalBytes int64) ProgressPayload {
	pct := -1.0
	if totalBytes > 0 {
		pct = float64(bytes) / float64(totalBytes)
	}

	return ProgressPayload{
		Step:       step,
		Bytes:      bytes,
		TotalBytes: totalBytes,
		Percent:    pct,
	}
}

// panelProgressTimeout caps the panel POST so a slow or unreachable
// panel never gates the transfer pipeline. The websocket publish
// happens regardless.
const panelProgressTimeout = 5 * time.Second

// PublishProgress emits a structured transfer progress event. callers in
// the source upload goroutine and the destination extract / verify /
// cleanup paths use this to surface phase + byte progress so the panel
// can render the overview page's progress bar without parsing log
// strings.
//
// The websocket publish is synchronous and cheap. The panel POST runs
// in a goroutine with a bounded timeout so a slow panel does not stall
// the transfer pump. POST failures log a warning and are otherwise
// non-fatal — the transfer continues either way.
func (t *Transfer) PublishProgress(step Step, bytes, totalBytes int64) {
	if t.Server == nil {
		return
	}

	t.Server.Events().Publish(server.TransferProgressEvent, newProgressPayload(step, bytes, totalBytes))

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), panelProgressTimeout)
		defer cancel()

		if err := t.Server.SendTransferProgress(ctx, string(step), bytes, totalBytes); err != nil {
			t.Log().WithError(err).Warn("failed to post transfer progress to panel")
		}
	}()
}

// progressEmitter throttles PublishProgress calls so a tight write loop
// does not flood the event bus. Callers call bump on every chunk, the
// emitter fires only when at least throttle has elapsed since the
// previous fire.
type progressEmitter struct {
	transfer *Transfer
	step     Step
	total    int64
	lastEmit time.Time
	throttle time.Duration
}

// newProgressEmitter constructs an emitter with the default 500ms
// throttle.
func newProgressEmitter(t *Transfer, step Step, total int64) *progressEmitter {
	return &progressEmitter{
		transfer: t,
		step:     step,
		total:    total,
		throttle: 500 * time.Millisecond,
	}
}

// shouldEmit returns true if enough time has elapsed since the last
// fire to allow another emission. The first call always returns true
// because lastEmit is the zero value.
func (p *progressEmitter) shouldEmit() bool {
	return p.lastEmit.IsZero() || time.Since(p.lastEmit) >= p.throttle
}

// bump fires a progress event if the throttle window has passed.
func (p *progressEmitter) bump(bytes int64) {
	if !p.shouldEmit() {
		return
	}
	p.lastEmit = time.Now()
	if p.transfer != nil {
		p.transfer.PublishProgress(p.step, bytes, p.total)
	}
}

// flush fires a final progress event regardless of throttle, used to
// publish the closing state of a step (typically bytes == total).
func (p *progressEmitter) flush(bytes int64) {
	p.lastEmit = time.Now()
	if p.transfer != nil {
		p.transfer.PublishProgress(p.step, bytes, p.total)
	}
}

// SendMessage sends a message to the server's console.
func (t *Transfer) SendMessage(v string) {
	t.Server.Events().Publish(
		server.TransferLogsEvent,
		colorstring.Color("[yellow][bold]"+time.Now().Format(time.RFC1123)+" [Transfer System] [Source Node]:[default] "+v),
	)
}

// Error logs an error that occurred on the source node.
func (t *Transfer) Error(err error, v string) {
	t.Log().WithError(err).Error(v)
	t.SendMessage(v)
}

// Log returns a logger for the transfer.
func (t *Transfer) Log() *log.Entry {
	if t.Server == nil {
		return log.WithField("subsystem", "transfer")
	}
	return t.Server.Log().WithField("subsystem", "transfer")
}
