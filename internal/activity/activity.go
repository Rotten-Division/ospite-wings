// Package activity tracks per-server io activity for the nest eviction
// sweep. distinct from server/activity.go which records audit log events,
// this package is purely about "has this server seen any io recently".
//
// signal sources hooked into the tracker:
//   - http file api requests via router/middleware/BumpActivity
//   - sftp session start and end via sftp/server.go Handle
//
// the panel reads the tracker via two endpoints in router/router_activity.go,
// the eviction sweep uses the result to decide which stopped servers are
// eligible for nest capture. the design point is that wings is the only
// honest source of activity truth, panel side traffic does not reach this
// tracker so a user cannot fake activity by spamming the panel api.
package activity

import (
	"sync"
	"time"
)

// Tracker keeps a per server in-memory record of the last io request and the
// active sftp session count.
type Tracker struct {
	mu        sync.RWMutex
	records   map[string]*record
	startedAt time.Time
}

type record struct {
	LastIOAt           time.Time
	ActiveSftpSessions int
}

// New constructs a fresh tracker. the startedAt timestamp is the floor for
// last_io_at lookups, see Snapshot.
func New() *Tracker {
	return &Tracker{
		records:   map[string]*record{},
		startedAt: time.Now(),
	}
}

// RecordIO bumps the last io timestamp for the given server. fire and forget,
// every wings file api handler and sftp request method calls this.
func (t *Tracker) RecordIO(serverUUID string) {
	if serverUUID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.recordFor(serverUUID).LastIOAt = time.Now()
}

// SftpSessionStart increments the active session count for the server. paired
// with SftpSessionEnd via defer at the sftp Handle entry.
func (t *Tracker) SftpSessionStart(serverUUID string) {
	if serverUUID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	r := t.recordFor(serverUUID)
	r.ActiveSftpSessions++
	r.LastIOAt = time.Now()
}

// SftpSessionEnd decrements the active session count. floors at zero so an
// unbalanced call cannot make the count go negative. no-op when no record
// exists for the server, an orphan end before any start should not create
// a tracking record.
func (t *Tracker) SftpSessionEnd(serverUUID string) {
	if serverUUID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	r, ok := t.records[serverUUID]
	if !ok {
		return
	}
	if r.ActiveSftpSessions > 0 {
		r.ActiveSftpSessions--
	}
}

// Snapshot returns the current values for the server. when no record exists
// last_io_at falls back to the tracker start time, the first eviction sweep
// after a wings restart sees every server as having had recent activity for
// one threshold window. trades a 15 min freeze for not falsely evicting a
// server wings has just lost track of.
func (t *Tracker) Snapshot(serverUUID string) (lastIOAt time.Time, activeSftp int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if r, ok := t.records[serverUUID]; ok {
		return r.LastIOAt, r.ActiveSftpSessions
	}
	return t.startedAt, 0
}

// IsIdle returns true when the server has had no io for at least
// idleThreshold and zero active sftp sessions. callers also check container
// state separately, the tracker only knows about io.
func (t *Tracker) IsIdle(serverUUID string, idleThreshold time.Duration) bool {
	last, sftp := t.Snapshot(serverUUID)
	if sftp > 0 {
		return false
	}
	return time.Since(last) >= idleThreshold
}

func (t *Tracker) recordFor(serverUUID string) *record {
	if r, ok := t.records[serverUUID]; ok {
		return r
	}
	r := &record{LastIOAt: t.startedAt}
	t.records[serverUUID] = r
	return r
}

// global is the shared tracker instance the wings router and sftp layers use.
// kept as a singleton so handlers can reach it without plumbing it through
// every constructor, mirrors how config.Get() is used elsewhere in wings.
var (
	globalOnce    sync.Once
	globalTracker *Tracker
)

// Global returns the process wide tracker, initialised on first call.
func Global() *Tracker {
	globalOnce.Do(func() {
		globalTracker = New()
	})
	return globalTracker
}
