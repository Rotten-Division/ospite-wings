package transfer

import (
	"testing"
	"time"

	"github.com/franela/goblin"
)

func TestProgress(t *testing.T) {
	g := goblin.Goblin(t)

	g.Describe("newProgressPayload", func() {
		g.It("computes percent when total is positive", func() {
			p := newProgressPayload(StepUploading, 50, 100)
			g.Assert(p.Step).Equal(StepUploading)
			g.Assert(p.Bytes).Equal(int64(50))
			g.Assert(p.TotalBytes).Equal(int64(100))
			g.Assert(p.Percent).Equal(0.5)
		})

		g.It("returns -1 percent when total is zero", func() {
			p := newProgressPayload(StepExtracting, 50, 0)
			g.Assert(p.Percent).Equal(-1.0)
		})

		g.It("returns -1 percent when total is negative", func() {
			p := newProgressPayload(StepExtracting, 0, -1)
			g.Assert(p.Percent).Equal(-1.0)
		})

		g.It("clamps to >1 when written exceeds total", func() {
			// caller responsibility to cap, payload reflects raw ratio.
			p := newProgressPayload(StepUploading, 150, 100)
			g.Assert(p.Percent).Equal(1.5)
		})
	})

	g.Describe("progressEmitter", func() {
		g.It("shouldEmit returns true on first call (lastEmit zero)", func() {
			e := &progressEmitter{throttle: 1 * time.Second}
			g.Assert(e.shouldEmit()).IsTrue()
		})

		g.It("shouldEmit returns false inside the throttle window", func() {
			e := &progressEmitter{
				throttle: 1 * time.Second,
				lastEmit: time.Now(),
			}
			g.Assert(e.shouldEmit()).IsFalse()
		})

		g.It("shouldEmit returns true once throttle has elapsed", func() {
			e := &progressEmitter{
				throttle: 10 * time.Millisecond,
				lastEmit: time.Now().Add(-1 * time.Hour),
			}
			g.Assert(e.shouldEmit()).IsTrue()
		})

		g.It("bump advances lastEmit when shouldEmit allows", func() {
			e := &progressEmitter{throttle: 1 * time.Second}
			before := e.lastEmit
			e.bump(10)
			g.Assert(e.lastEmit.Equal(before)).IsFalse()
		})

		g.It("bump leaves lastEmit untouched inside the throttle window", func() {
			e := &progressEmitter{
				throttle: 1 * time.Second,
				lastEmit: time.Now(),
			}
			recordedAt := e.lastEmit
			e.bump(20)
			g.Assert(e.lastEmit.Equal(recordedAt)).IsTrue()
		})

		g.It("flush always advances lastEmit", func() {
			e := &progressEmitter{
				throttle: 1 * time.Hour,
				lastEmit: time.Now(),
			}
			before := e.lastEmit
			time.Sleep(1 * time.Millisecond)
			e.flush(40)
			g.Assert(e.lastEmit.After(before)).IsTrue()
		})
	})
}
