// Package health implements active health-probe runners: stateless
// network checks pointed at a single target address, folded into a
// rolling healthy/unhealthy/unknown verdict.
//
// A Probe runs a single attempt of a configured check — HTTP or TCP —
// and reports pass/fail plus latency. A Runner drives a Probe on a
// fixed period, honours an optional initial delay, and derives a
// rolling Status by stacking consecutive passes/failures against
// success/failure thresholds. Every attempt emits a Result on the
// Runner's channel.
//
// The package is pure-Go, CGO-free, and depends only on the standard
// library (net, net/http, context, time). It has no opinion on what to
// do with a verdict — Healthy vs Unhealthy is just a fact a caller's
// state machine acts on.
package health

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Type identifies which kind of network check a probe performs.
type Type int

const (
	// TypeNone means "no probe configured"; New returns (nil, nil) so a
	// caller can skip starting a Runner without a type assertion.
	TypeNone Type = iota
	// TypeHTTP issues an HTTP request and checks the response status.
	TypeHTTP
	// TypeTCP opens a TCP connection and checks that it succeeds.
	TypeTCP
	// TypeExec would run an in-target command; reserved, not yet
	// supported (it needs an in-target agent channel this runner has no
	// access to).
	TypeExec
)

func (t Type) String() string {
	switch t {
	case TypeHTTP:
		return "http"
	case TypeTCP:
		return "tcp"
	case TypeExec:
		return "exec"
	default:
		return "none"
	}
}

// Status is the rolling health derived from a window of attempts.
//
// A single attempt is reported by a Result's Pass flag; the Runner
// derives Healthy/Unhealthy by stacking N consecutive passes/failures
// against the SuccessThreshold / FailureThreshold knobs.
type Status int

const (
	StatusUnknown   Status = iota // before InitialDelay elapses / a threshold is met
	StatusHealthy                 // SuccessThreshold consecutive passes
	StatusUnhealthy               // FailureThreshold consecutive failures
)

func (s Status) String() string {
	switch s {
	case StatusHealthy:
		return "healthy"
	case StatusUnhealthy:
		return "unhealthy"
	default:
		return "unknown"
	}
}

// Spec is the plain, dependency-free configuration for a probe and its
// runner. Zero values are filled with sensible defaults (path "/",
// method GET, timeout 1s, period 1s, failure threshold 3, success
// threshold 1), so the smallest useful HTTP spec is just
// {Type: TypeHTTP, HTTPPort: 8080} and the smallest TCP spec is
// {Type: TypeTCP, TCPPort: 22}.
type Spec struct {
	Type Type

	// HTTP fields; ignored for TCP.
	HTTPPath   string // default "/"
	HTTPPort   int    // required for HTTP
	HTTPMethod string // default "GET"
	// HTTPStatusOK is the set of acceptable HTTP status codes; empty
	// means "any 2xx".
	HTTPStatusOK []int

	// TCP fields; required for TCP.
	TCPPort int

	// Scheduling fields, read by NewRunnerFromSpec.
	InitialDelay     time.Duration // wait before the first probe
	Period           time.Duration // between probes; default 1s
	Timeout          time.Duration // per-probe deadline; default 1s
	FailureThreshold int           // consecutive failures = unhealthy; default 3
	SuccessThreshold int           // consecutive successes = healthy; default 1
}

// Result is one full state observation emitted by Runner after every
// attempt. Latency carries the attempt's wall-clock duration even on
// failure (a timeout surfaces here as Latency≈Timeout).
type Result struct {
	When    time.Time
	Status  Status
	Pass    bool
	Latency time.Duration
	Err     error
}

// Probe runs a single attempt of the configured check against addr and
// returns its raw pass/fail, the attempt latency, and any error. It is
// side-effect-free apart from the one network call. A Runner calls this
// on its own schedule and folds attempts into the rolling Status.
type Probe interface {
	Attempt(ctx context.Context, addr string) (pass bool, latency time.Duration, err error)
	Kind() Type
}

// New translates a Spec into a runnable Probe. It returns (nil, nil)
// for TypeNone so a caller can treat "no probe configured" as "skip the
// runner" without a type assertion. Unsupported types (TypeExec) return
// an error rather than a silently nil Probe so a misconfiguration
// surfaces immediately.
func New(s *Spec) (Probe, error) {
	if s == nil || s.Type == TypeNone {
		return nil, nil
	}
	switch s.Type {
	case TypeHTTP:
		return newHTTP(s)
	case TypeTCP:
		return newTCP(s)
	case TypeExec:
		return nil, fmt.Errorf("health: EXEC probe not supported (no in-target agent channel)")
	default:
		return nil, fmt.Errorf("health: unknown probe type %v", s.Type)
	}
}

// ---- HTTP ---------------------------------------------------------

type httpProbe struct {
	path       string
	port       int
	method     string
	statusOK   map[int]struct{}
	timeout    time.Duration
	client     *http.Client
	defaultOK2 bool // when statusOK is empty, accept any 2xx
}

func newHTTP(s *Spec) (Probe, error) {
	if s.HTTPPort <= 0 {
		return nil, fmt.Errorf("health: HTTP probe requires HTTPPort > 0")
	}
	path := s.HTTPPath
	if path == "" {
		path = "/"
	}
	method := s.HTTPMethod
	if method == "" {
		method = http.MethodGet
	}
	timeout := s.Timeout
	if timeout <= 0 {
		timeout = time.Second
	}
	// Per-probe Transport so connection pools don't bleed between
	// targets watched concurrently. DisableKeepAlives because a healthy
	// peer plus frequent probes would otherwise pin idle FDs open.
	tr := &http.Transport{
		DisableKeepAlives: true,
		MaxIdleConns:      1,
		IdleConnTimeout:   timeout,
	}
	hp := &httpProbe{
		path:       path,
		port:       s.HTTPPort,
		method:     method,
		timeout:    timeout,
		defaultOK2: len(s.HTTPStatusOK) == 0,
		statusOK:   make(map[int]struct{}, len(s.HTTPStatusOK)),
	}
	for _, c := range s.HTTPStatusOK {
		hp.statusOK[c] = struct{}{}
	}
	hp.client = &http.Client{
		Transport: tr,
		Timeout:   timeout,
		// Don't follow redirects: we're checking whether *this* endpoint
		// is alive, not whatever it forwards to.
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return hp, nil
}

func (h *httpProbe) Kind() Type { return TypeHTTP }

func (h *httpProbe) Attempt(ctx context.Context, addr string) (bool, time.Duration, error) {
	url := "http://" + net.JoinHostPort(addr, strconv.Itoa(h.port)) + h.path
	req, err := http.NewRequestWithContext(ctx, h.method, url, nil)
	if err != nil {
		return false, 0, fmt.Errorf("health: build request: %w", err)
	}
	start := time.Now()
	resp, err := h.client.Do(req)
	latency := time.Since(start)
	if err != nil {
		return false, latency, err
	}
	defer resp.Body.Close()
	if h.defaultOK2 {
		return resp.StatusCode >= 200 && resp.StatusCode < 300, latency, nil
	}
	if _, ok := h.statusOK[resp.StatusCode]; !ok {
		return false, latency, fmt.Errorf("health: HTTP %d not in accepted set", resp.StatusCode)
	}
	return true, latency, nil
}

// ---- TCP ----------------------------------------------------------

type tcpProbe struct {
	port    int
	timeout time.Duration
}

func newTCP(s *Spec) (Probe, error) {
	if s.TCPPort <= 0 {
		return nil, fmt.Errorf("health: TCP probe requires TCPPort > 0")
	}
	timeout := s.Timeout
	if timeout <= 0 {
		timeout = time.Second
	}
	return &tcpProbe{port: s.TCPPort, timeout: timeout}, nil
}

func (t *tcpProbe) Kind() Type { return TypeTCP }

func (t *tcpProbe) Attempt(ctx context.Context, addr string) (bool, time.Duration, error) {
	dialer := net.Dialer{Timeout: t.timeout}
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	start := time.Now()
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(addr, strconv.Itoa(t.port)))
	latency := time.Since(start)
	if err != nil {
		return false, latency, err
	}
	_ = conn.Close()
	return true, latency, nil
}

// ---- Runner -------------------------------------------------------

// Runner periodically calls Probe.Attempt against a target address and
// emits a Result on every attempt. Consecutive pass/fail counts roll
// into Status per the SuccessThreshold / FailureThreshold knobs. The
// InitialDelay is honoured exactly once at start.
//
// Run one Runner per target being watched. Stop() drains the goroutine
// without losing the final Result.
type Runner struct {
	probe   Probe
	addr    string
	period  time.Duration
	initial time.Duration

	failureK int
	successK int

	results chan Result
	stop    chan struct{}
	once    sync.Once

	// Injectable clock so tests need no wall time; defaults to time.Now.
	now func() time.Time

	mu      sync.Mutex
	curr    Status
	consecP int
	consecF int
}

// RunnerOptions are the scheduling knobs the Runner reads, made explicit
// so a caller can build one without a *Spec (tests, custom integrations).
type RunnerOptions struct {
	Period           time.Duration
	InitialDelay     time.Duration
	FailureThreshold int
	SuccessThreshold int
	Now              func() time.Time // optional; defaults to time.Now
}

// NewRunner builds a runner for probe at addr. The Result channel is
// buffered (32) so a promptly-consuming caller normally sees an empty
// channel; a wedged consumer drops rather than freezing the probe.
func NewRunner(probe Probe, addr string, opts RunnerOptions) *Runner {
	if opts.Period <= 0 {
		opts.Period = time.Second
	}
	if opts.FailureThreshold <= 0 {
		opts.FailureThreshold = 3
	}
	if opts.SuccessThreshold <= 0 {
		opts.SuccessThreshold = 1
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Runner{
		probe:    probe,
		addr:     addr,
		period:   opts.Period,
		initial:  opts.InitialDelay,
		failureK: opts.FailureThreshold,
		successK: opts.SuccessThreshold,
		results:  make(chan Result, 32),
		stop:     make(chan struct{}),
		now:      now,
		curr:     StatusUnknown,
	}
}

// NewRunnerFromSpec is the sugar most callers use: map a Spec's
// scheduling knobs onto RunnerOptions in one call.
func NewRunnerFromSpec(probe Probe, addr string, s *Spec) *Runner {
	return NewRunner(probe, addr, RunnerOptions{
		Period:           s.Period,
		InitialDelay:     s.InitialDelay,
		FailureThreshold: s.FailureThreshold,
		SuccessThreshold: s.SuccessThreshold,
	})
}

// Results returns the channel Result observations land on. The channel
// is closed when the Runner exits.
func (r *Runner) Results() <-chan Result { return r.results }

// Status returns the rolling status the Runner has observed. Cheap and
// safe to call from any goroutine.
func (r *Runner) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.curr
}

// Run blocks until ctx is cancelled OR Stop() is called. The Result
// channel is closed when Run returns.
func (r *Runner) Run(ctx context.Context) {
	defer close(r.results)
	if r.initial > 0 {
		select {
		case <-time.After(r.initial):
		case <-ctx.Done():
			return
		case <-r.stop:
			return
		}
	}
	t := time.NewTicker(r.period)
	defer t.Stop()
	// Fire one attempt immediately so the first Result lands without
	// waiting a full period.
	r.runOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stop:
			return
		case <-t.C:
			r.runOnce(ctx)
		}
	}
}

// Stop signals the Runner to exit. Safe to call multiple times.
func (r *Runner) Stop() { r.once.Do(func() { close(r.stop) }) }

func (r *Runner) runOnce(ctx context.Context) {
	pass, latency, err := r.probe.Attempt(ctx, r.addr)
	r.mu.Lock()
	if pass {
		r.consecP++
		r.consecF = 0
		if r.consecP >= r.successK {
			r.curr = StatusHealthy
		}
	} else {
		r.consecF++
		r.consecP = 0
		if r.consecF >= r.failureK {
			r.curr = StatusUnhealthy
		}
	}
	status := r.curr
	r.mu.Unlock()
	res := Result{
		When:    r.now(),
		Status:  status,
		Pass:    pass,
		Latency: latency,
		Err:     err,
	}
	// Non-blocking send: a consumer that wedged would otherwise freeze
	// every probe. Drops here are a deliberate safety valve.
	select {
	case r.results <- res:
	default:
	}
}

// IsTimeout reports whether err is a probe timeout ("never reached the
// target") as opposed to the target actively refusing. Callers that
// treat the two differently — e.g. a grace period that applies only to
// unreachable targets — use this to tell them apart.
func IsTimeout(err error) bool {
	if err == nil {
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded)
}
