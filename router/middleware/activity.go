package middleware

import (
	"github.com/gin-gonic/gin"

	"github.com/pelican-dev/wings/internal/activity"
)

// BumpActivity records an io event against the resolved server. mounted on
// the file api route group and on the signed file upload route, the eviction
// sweep reads the resulting last_io_at timestamp via internal/activity.
// runs after the request so failed handlers do not bump the clock.
func BumpActivity() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		// only count successful or partial-success responses, an unauthorised
		// or 4xx response should not register as activity from the user.
		if c.Writer.Status() >= 400 {
			return
		}

		s := ExtractServer(c)
		if s == nil {
			return
		}

		activity.Global().RecordIO(s.Id())
	}
}
