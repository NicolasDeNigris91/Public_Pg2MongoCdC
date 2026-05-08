package lag

import (
	"testing"
	"time"
)

func TestAgeSeconds_NeverProbedReportsSentinel(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	p := &Probe{now: func() time.Time { return now }}
	// lastProbeAt is the zero value (not yet probed).
	if got := p.ageSeconds(); got < 3600 {
		t.Errorf("never-probed gauge must be a large sentinel, got %v", got)
	}
}

func TestAgeSeconds_ClimbsAfterSuccess(t *testing.T) {
	t0 := time.Unix(2_000_000, 0)
	now := t0
	p := &Probe{now: func() time.Time { return now }}
	p.lastProbeAt.Store(t0.UnixNano())

	if got := p.ageSeconds(); got != 0 {
		t.Errorf("just-probed: want 0, got %v", got)
	}
	now = t0.Add(45 * time.Second)
	if got := p.ageSeconds(); got != 45 {
		t.Errorf("after +45s: want 45, got %v", got)
	}
}

func TestTotalLag_AtomicReadback(t *testing.T) {
	p := &Probe{}
	p.totalLag.Store(12345)
	if got := p.TotalLag(); got != 12345 {
		t.Errorf("want 12345, got %d", got)
	}
}
