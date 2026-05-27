package router

import (
	"context"
	"net/http"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/gin-gonic/gin"

	"github.com/pelican-dev/wings/config"
	"github.com/pelican-dev/wings/environment"
	"github.com/pelican-dev/wings/router/middleware"
	"github.com/pelican-dev/wings/server"
	"github.com/pelican-dev/wings/server/nest"
)

// callbackAuth packs the wings token id and secret into the {id}.{secret}
// pair the panel's DaemonAuthenticate middleware expects as a Bearer token.
func callbackAuth() string {
	t := config.Get().Token
	return t.ID + "." + t.Token
}

// postServerNestCapture handles POST /api/servers/{uuid}/nest/capture. answers
// 202 immediately and runs the streaming capture in a goroutine, the panel
// learns the result via the callback url it provided. refuses with 409 when
// the runtime is not offline, walking a live volume reads inconsistent files
// mid write and os.RemoveAll under a running container would yank files out
// from underneath it.
func postServerNestCapture(c *gin.Context) {
	s := middleware.ExtractServer(c)
	if s == nil {
		return
	}

	if s.Environment.State() != environment.ProcessOfflineState {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{
			"error": "server must be offline before nest capture",
		})
		return
	}

	var req nest.CaptureRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	volumePath := s.Filesystem().Path()
	logger := middleware.ExtractLogger(c)

	go func(logger *log.Entry) {
		if err := nest.Capture(context.Background(), volumePath, req.PresignedUrl, req.CallbackUrl, callbackAuth()); err != nil {
			logger.WithField("error", errors.WithStackIf(err)).Error("router: nest capture callback delivery failed")
		}
	}(logger)

	c.Status(http.StatusAccepted)
}

// postServerNestRestore handles POST /api/servers/{uuid}/nest/restore. the
// panel polls server.view and server.resources during the hydrating window
// and waits for the callback for the terminal state. same offline guard as
// capture, restore writes into a destination directory the runtime would
// otherwise have bind mounted.
//
// registered outside the ServerExists middleware so this handler can fetch
// the server config from panel and register on demand. eviction sets the
// panel's server.node_id to null, the source wings will drop the server
// from its manager on the next config sync, then the panel restore may
// re-bind to this node which has no record of the server.
func postServerNestRestore(c *gin.Context) {
	uuid := c.Param("server")
	manager := middleware.ExtractManager(c)
	logger := middleware.ExtractLogger(c).WithField("server_id", uuid)

	s := manager.Find(func(s *server.Server) bool {
		return s.ID() == uuid
	})
	if s == nil {
		// not in the manager, fetch the server config from panel and
		// register without running install. install would format the
		// volume which we are about to overwrite from s3 anyway, the
		// fetch+register path here is the minimum that lets the restore
		// goroutine reach the volume path.
		cfg, err := manager.Client().GetServerConfiguration(c.Request.Context(), uuid)
		if err != nil {
			logger.WithField("error", errors.WithStackIf(err)).Error("router: nest restore failed to fetch unknown server config")
			c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
				"error": "server not registered on this daemon and panel returned no config",
			})
			return
		}

		registered, err := manager.InitServer(cfg)
		if err != nil {
			logger.WithField("error", errors.WithStackIf(err)).Error("router: nest restore failed to register fetched server")
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"error": "fetched server config but registration failed",
			})
			return
		}
		manager.Add(registered)
		s = registered
		logger.Info("router: nest restore registered previously-unknown server from panel")
	}

	if s.Environment.State() != environment.ProcessOfflineState {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{
			"error": "server must be offline before nest restore",
		})
		return
	}

	var req nest.RestoreRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	volumePath := s.Filesystem().Path()

	go func(logger *log.Entry) {
		if err := nest.Restore(context.Background(), volumePath, req.PresignedUrl, req.ExpectedSha256, req.CallbackUrl, req.ProgressUrl, callbackAuth()); err != nil {
			logger.WithField("error", errors.WithStackIf(err)).Error("router: nest restore callback delivery failed")
		}
	}(logger)

	c.Status(http.StatusAccepted)
}
