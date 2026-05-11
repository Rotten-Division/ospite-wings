package activity

import (
	"sync"
	"testing"
	"time"
)

func TestRecordIO_BumpsTimestamp(t *testing.T) {
	tr := New()
	uuid := "00000000-0000-0000-0000-000000000001"

	before, _ := tr.Snapshot(uuid)
	time.Sleep(2 * time.Millisecond)
	tr.RecordIO(uuid)
	after, _ := tr.Snapshot(uuid)

	if !after.After(before) {
		t.Errorf("RecordIO did not bump last_io_at: before=%v after=%v", before, after)
	}
}

func TestSnapshot_FallsBackToStartTimeForUnknownServer(t *testing.T) {
	tr := New()
	last, sftp := tr.Snapshot("never-seen")

	if last.IsZero() {
		t.Errorf("expected start-time fallback, got zero value")
	}
	if sftp != 0 {
		t.Errorf("expected zero active sftp sessions for unknown server, got %d", sftp)
	}
}

func TestSftpSessionStartEnd_TracksCount(t *testing.T) {
	tr := New()
	uuid := "abc"

	tr.SftpSessionStart(uuid)
	_, n := tr.Snapshot(uuid)
	if n != 1 {
		t.Errorf("expected 1 active session after start, got %d", n)
	}

	tr.SftpSessionStart(uuid)
	_, n = tr.Snapshot(uuid)
	if n != 2 {
		t.Errorf("expected 2 active sessions after second start, got %d", n)
	}

	tr.SftpSessionEnd(uuid)
	tr.SftpSessionEnd(uuid)
	_, n = tr.Snapshot(uuid)
	if n != 0 {
		t.Errorf("expected 0 active sessions after both ends, got %d", n)
	}
}

func TestSftpSessionEnd_FloorsAtZero(t *testing.T) {
	tr := New()
	tr.SftpSessionEnd("xyz")

	_, n := tr.Snapshot("xyz")
	if n != 0 {
		t.Errorf("expected count to floor at zero, got %d", n)
	}
}

func TestIsIdle_HonoursThresholdAndSftpCount(t *testing.T) {
	tr := New()
	uuid := "ddd"

	// fresh tracker: last_io_at = startedAt, very recent, not idle
	if tr.IsIdle(uuid, 1*time.Hour) {
		t.Errorf("expected fresh server to not be idle within 1h threshold")
	}

	// reach back in time: pretend the server's last_io_at is 2h ago
	tr.records[uuid] = &record{LastIOAt: time.Now().Add(-2 * time.Hour)}
	if !tr.IsIdle(uuid, 1*time.Hour) {
		t.Errorf("expected server with 2h old io to be idle on a 1h threshold")
	}

	// active sftp session blocks idle even with stale last_io_at
	tr.records[uuid].ActiveSftpSessions = 1
	if tr.IsIdle(uuid, 1*time.Hour) {
		t.Errorf("expected active sftp session to block idle decision")
	}
}

func TestRecordIO_IsConcurrencySafe(t *testing.T) {
	tr := New()
	const goroutines = 50
	const calls = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			uuid := "concurrent-server"
			for j := 0; j < calls; j++ {
				tr.RecordIO(uuid)
			}
		}(i)
	}
	wg.Wait()

	// no panic, no data race detected by the race detector when run with -race
	last, _ := tr.Snapshot("concurrent-server")
	if last.IsZero() {
		t.Errorf("expected last_io_at to be set after concurrent bumps")
	}
}

func TestEmptyServerUUID_NoOp(t *testing.T) {
	tr := New()
	tr.RecordIO("")
	tr.SftpSessionStart("")
	tr.SftpSessionEnd("")
	// nothing was recorded, shouldn't appear in records
	if _, ok := tr.records[""]; ok {
		t.Errorf("empty uuid should not create a record")
	}
}
