// Package lag polls Kafka for the sink's consumer-group lag and exposes
// it as a Prometheus gauge. We compute lag in-process rather than rely
// on an external `kafka-exporter` for two reasons:
//
//  1. The chaos overlay ships kafka-exporter; the production overlay and
//     the Helm chart do not. The ConsumerLagHigh alert was therefore only
//     functional in the dev compose stack. Emitting lag from the sink
//     itself makes the alert real in every deployment shape.
//  2. Lag is the single most useful liveness signal for a CDC consumer.
//     Co-locating it with the process that produces it removes a
//     dependency hop on the alerting path.
//
// Cost: one DescribeGroups + ListOffsets pair per probe interval (default
// 30s), using the consumer's existing kgo connection pool.
package lag

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Probe runs a periodic admin query and stores the most recent total lag
// in an atomic int64 readable by the registered GaugeFunc.
type Probe struct {
	groupID  string
	interval time.Duration
	probeTO  time.Duration
	logf     func(format string, args ...any)
	now      func() time.Time

	adm *kadm.Client

	totalLag    atomic.Int64
	lastProbeAt atomic.Int64 // unix nano, 0 = never
}

// Config bundles knobs. Zero values become sensible defaults.
type Config struct {
	GroupID       string
	Interval      time.Duration
	ProbeTimeout  time.Duration
	Logf          func(format string, args ...any)
	Now           func() time.Time
}

// New constructs a Probe. The kgo.Client is borrowed (not owned) - the
// caller must keep it alive for the Probe's lifetime. Registers two
// gauges on reg if non-nil:
//
//   - migration_consumer_group_lag      (sum of per-partition lag)
//   - migration_consumer_group_lag_age_seconds (seconds since last probe)
func New(client *kgo.Client, reg prometheus.Registerer, cfg Config) *Probe {
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.ProbeTimeout <= 0 {
		cfg.ProbeTimeout = 5 * time.Second
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	p := &Probe{
		groupID:  cfg.GroupID,
		interval: cfg.Interval,
		probeTO:  cfg.ProbeTimeout,
		logf:     cfg.Logf,
		now:      cfg.Now,
		adm:      kadm.NewClient(client),
	}
	if reg != nil {
		reg.MustRegister(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name: "migration_consumer_group_lag",
				Help: "Sum of per-partition lag across the sink's consumer group, computed by the sink itself.",
			},
			func() float64 { return float64(p.totalLag.Load()) },
		))
		reg.MustRegister(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name: "migration_consumer_group_lag_age_seconds",
				Help: "Seconds since the sink last successfully probed Kafka for its lag. Climbs if Kafka admin calls fail.",
			},
			p.ageSeconds,
		))
	}
	return p
}

// Run drives the probe. Returns when ctx is canceled. Probe failures are
// logged; the lag_age_seconds gauge will climb so an alert can catch a
// chronic broker-side problem.
func (p *Probe) Run(ctx context.Context) {
	t := time.NewTicker(p.interval)
	defer t.Stop()
	// Probe once immediately so the gauges are populated within seconds
	// of process start, not after the first interval.
	if err := p.Probe(ctx); err != nil {
		p.logf("lag probe (initial): %v", err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := p.Probe(ctx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				p.logf("lag probe: %v", err)
			}
		}
	}
}

// Probe runs a single admin call. Public for tests and for the at-startup
// invocation in Run.
func (p *Probe) Probe(ctx context.Context) error {
	probeCtx, cancel := context.WithTimeout(ctx, p.probeTO)
	defer cancel()

	groupLags, err := p.adm.Lag(probeCtx, p.groupID)
	if err != nil {
		return err
	}
	gl, ok := groupLags[p.groupID]
	if !ok {
		// Group not yet known to the broker (cold start before any
		// commits). Treat as lag=0 rather than NaN.
		p.totalLag.Store(0)
		p.lastProbeAt.Store(p.now().UnixNano())
		return nil
	}
	if gl.FetchErr != nil {
		return gl.FetchErr
	}
	if gl.DescribeErr != nil {
		return gl.DescribeErr
	}

	var total int64
	for _, partsByTopic := range gl.Lag {
		for _, l := range partsByTopic { //nolint:gocritic // kadm map values can't be address-taken; copy at probe interval is negligible
			if l.Lag > 0 {
				total += l.Lag
			}
		}
	}
	p.totalLag.Store(total)
	p.lastProbeAt.Store(p.now().UnixNano())
	return nil
}

// TotalLag is what the GaugeFunc reads; exposed for tests.
func (p *Probe) TotalLag() int64 { return p.totalLag.Load() }

func (p *Probe) ageSeconds() float64 {
	last := p.lastProbeAt.Load()
	if last == 0 {
		// Never probed successfully. Surface a large but finite sentinel
		// so the lag-age alert (if configured) can fire if the sink
		// boots but Kafka admin is unreachable. 86400 = 1 day.
		return 86400
	}
	d := p.now().Sub(time.Unix(0, last)).Seconds()
	if d < 0 {
		return 0
	}
	return d
}
