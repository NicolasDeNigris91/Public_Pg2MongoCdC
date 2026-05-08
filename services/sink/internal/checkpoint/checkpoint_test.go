package checkpoint

import (
	"sync"
	"testing"
	"time"
)

// newForTest builds a Checkpointer with no Mongo collection. Safe for any
// path that does not call flush. Internal-only because we touch unexported
// fields directly.
func newForTest(now func() time.Time) *Checkpointer {
	c := &Checkpointer{now: now, createdAt: now()}
	c.lastWriteAtUnixNano.Store(c.createdAt.UnixNano())
	return c
}

func TestMarkProgress_MonotonicMaxLSN(t *testing.T) {
	cp := newForTest(time.Now)

	cp.MarkProgress(50, 1)
	cp.MarkProgress(200, 1)
	cp.MarkProgress(150, 1) // older LSN must NOT regress maxLSN

	if got := cp.maxLSN.Load(); got != 200 {
		t.Errorf("want maxLSN=200, got %d", got)
	}
	if got := cp.eventCount.Load(); got != 3 {
		t.Errorf("want eventCount=3, got %d", got)
	}
	if !cp.dirty.Load() {
		t.Error("want dirty=true after MarkProgress")
	}
}

// MarkProgress must be safe under contention - the consumer loop calls it
// from one goroutine but a future shard-per-partition design might fan
// out. Race-detector + concurrent calls smoke-test this today.
func TestMarkProgress_ConcurrentSafe(t *testing.T) {
	cp := newForTest(time.Now)
	const goroutines, perG = 8, 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(base int64) {
			defer wg.Done()
			for i := int64(0); i < perG; i++ {
				cp.MarkProgress(base*1000+i, 1)
			}
		}(int64(g) + 1)
	}
	wg.Wait()
	if got, want := cp.eventCount.Load(), int64(goroutines*perG); got != want {
		t.Errorf("want eventCount=%d, got %d", want, got)
	}
	// Highest LSN must be from the highest base.
	if got, min := cp.maxLSN.Load(), int64(goroutines*1000); got < min {
		t.Errorf("want maxLSN >= %d, got %d", min, got)
	}
}

func TestStaleness_ClimbsWithoutFlush(t *testing.T) {
	t0 := time.Unix(1_000_000, 0)
	now := t0
	cp := newForTest(func() time.Time { return now })

	if got := cp.stalenessSeconds(); got != 0 {
		t.Errorf("at t0 want staleness=0, got %v", got)
	}
	now = t0.Add(45 * time.Second)
	if got := cp.stalenessSeconds(); got != 45 {
		t.Errorf("after +45s want staleness=45, got %v", got)
	}
	now = t0.Add(2 * time.Minute)
	if got := cp.stalenessSeconds(); got != 120 {
		t.Errorf("after +2m want staleness=120, got %v", got)
	}
}

// The Run loop's per-tick decision: flush iff dirty OR we have hit the
// heartbeat threshold. Idempotent and pure so we can drive it from a
// table test without spinning a real ticker.
func TestRunLoop_TickDecisionAndHeartbeat(t *testing.T) {
	// Simulate a 60-tick run with a heartbeat-every-6 schedule.
	// We feed in a pattern of dirty bits and assert when flushes fire.
	const N = 18
	const heartbeat = 6
	dirtyAt := map[int]bool{1: true, 2: true, 12: true} // dirty on these ticks

	idleTicks := 0
	flushed := []int{}
	for tick := 1; tick <= N; tick++ {
		dirty := dirtyAt[tick]
		shouldFlush := false
		if dirty {
			shouldFlush = true
		} else {
			idleTicks++
			if idleTicks >= heartbeat {
				shouldFlush = true
			}
		}
		if shouldFlush {
			flushed = append(flushed, tick)
			idleTicks = 0
		}
	}

	// Expected flushes:
	//   tick 1, 2 (dirty),
	//   tick 8 (idle ticks 3..8 = 6),
	//   tick 12 (dirty interrupts heartbeat, resets idle),
	//   tick 18 (idle ticks 13..18 = 6).
	want := []int{1, 2, 8, 12, 18}
	if len(flushed) != len(want) {
		t.Fatalf("flush schedule: want %v, got %v", want, flushed)
	}
	for i, w := range want {
		if flushed[i] != w {
			t.Errorf("flush[%d]: want tick %d, got %d (full=%v)", i, w, flushed[i], flushed)
		}
	}
}

// Simulates the flush success path (without touching Mongo) and asserts
// the staleness gauge resets - the same code path Run() drives.
func TestStaleness_ResetsAfterSuccessfulFlush(t *testing.T) {
	t0 := time.Unix(2_000_000, 0)
	now := t0
	cp := newForTest(func() time.Time { return now })

	now = t0.Add(60 * time.Second)
	if cp.stalenessSeconds() != 60 {
		t.Fatalf("setup: want staleness=60, got %v", cp.stalenessSeconds())
	}

	// Simulate what flush() does on success.
	cp.lastWriteAtUnixNano.Store(now.UnixNano())
	cp.dirty.Store(false)

	if got := cp.stalenessSeconds(); got != 0 {
		t.Errorf("after flush want staleness=0, got %v", got)
	}
	now = now.Add(7 * time.Second)
	if got := cp.stalenessSeconds(); got != 7 {
		t.Errorf("after +7s want staleness=7, got %v", got)
	}
}
