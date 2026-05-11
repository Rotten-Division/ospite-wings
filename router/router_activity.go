package router

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/pelican-dev/wings/environment"
	"github.com/pelican-dev/wings/internal/activity"
	"github.com/pelican-dev/wings/router/middleware"
)

// getServerActivity returns the current activity record for a single server.
// the panel calls this as the eviction preflight inside the per-server lock,
// to confirm eligibility right before issuing the capture request.
func getServerActivity(c *gin.Context) {
	s := middleware.ExtractServer(c)
	if s == nil {
		return
	}

	last, sftp := activity.Global().Snapshot(s.Id())

	c.JSON(http.StatusOK, gin.H{
		"last_io_at":           last.Unix(),
		"active_sftp_sessions": sftp,
		"container_state":      s.Environment.State(),
	})
}

// getNestCandidates returns the uuids of every server on this wings node that
// is eligible for nest eviction right now: container offline, no recorded io
// for at least idle_minutes, zero active sftp sessions. used by the panel
// sweep for candidate discovery.
//
// idle_minutes defaults to 15 and is capped at 1440 (24h) to bound the
// eligibility window an attacker could request.
func getNestCandidates(c *gin.Context) {
	idleMinutes := 15
	if raw := c.Query("idle_minutes"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			idleMinutes = parsed
		}
	}
	if idleMinutes > 1440 {
		idleMinutes = 1440
	}

	threshold := time.Duration(idleMinutes) * time.Minute
	manager := middleware.ExtractManager(c)
	tracker := activity.Global()

	uuids := []string{}
	for _, s := range manager.All() {
		if s.Environment.State() != environment.ProcessOfflineState {
			continue
		}
		if !tracker.IsIdle(s.Id(), threshold) {
			continue
		}
		uuids = append(uuids, s.Id())
	}

	c.JSON(http.StatusOK, gin.H{
		"server_uuids": uuids,
		"idle_minutes": idleMinutes,
	})
}
