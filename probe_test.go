package health

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeProbe is a deterministic Probe for exercising Runner logic without
// real network timing.
type fakeProbe struct {
	pass bool
	err  error
	kind Type
	hits int64
}

func (f *fakeProbe) Kind() Type { return f.kind }
func (f *fakeProbe) Attempt(context.Context, string) (bool, time.Duration, error) {
	atomic.AddInt64(&f.hits, 1)
	return f.pass, time.Millisecond, f.err
}

// hostPort extracts the loopback host and integer port of a test server.
func hostPort(t *testing.T, rawURL string) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(strings.TrimPrefix(rawURL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}
	return host, port
}

func TestType_String(t *testing.T) {
	for _, tc := range []struct {
		typ  Type
		want string
	}{
		{TypeHTTP, "http"},
		{TypeTCP, "tcp"},
		{TypeExec, "exec"},
		{TypeNone, "none"},
		{Type(99), "none"},
	} {
		if got := tc.typ.String(); got != tc.want {
			t.Errorf("Type(%d).String() = %q ; want %q", tc.typ, got, tc.want)
		}
	}
}

func TestStatus_String(t *testing.T) {
	for _, tc := range []struct {
		s    Status
		want string
	}{
		{StatusHealthy, "healthy"},
		{StatusUnhealthy, "unhealthy"},
		{StatusUnknown, "unknown"},
		{Status(99), "unknown"},
	} {
		if got := tc.s.String(); got != tc.want {
			t.Errorf("Status(%d).String() = %q ; want %q", tc.s, got, tc.want)
		}
	}
}

func TestNew_NoneReturnsNil(t *testing.T) {
	p, err := New(nil)
	if err != nil || p != nil {
		t.Errorf("nil Spec -> p=%v err=%v ; want nil,nil", p, err)
	}
	p, err = New(&Spec{Type: TypeNone})
	if err != nil || p != nil {
		t.Errorf("TypeNone -> p=%v err=%v ; want nil,nil", p, err)
	}
}

func TestNew_ExecUnsupported(t *testing.T) {
	if _, err := New(&Spec{Type: TypeExec}); err == nil {
		t.Error("TypeExec should error ; got nil")
	}
}

func TestNew_UnknownType(t *testing.T) {
	if _, err := New(&Spec{Type: Type(99)}); err == nil {
		t.Error("unknown type should error ; got nil")
	}
}

func TestNewHTTP_PortRequired(t *testing.T) {
	if _, err := New(&Spec{Type: TypeHTTP}); err == nil {
		t.Error("HTTP with no port should error ; got nil")
	}
}

func TestHTTPProbe_Defaults(t *testing.T) {
	// Omit path/method/timeout to exercise the default branches.
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Method + " " + r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	host, port := hostPort(t, srv.URL)

	p, err := New(&Spec{Type: TypeHTTP, HTTPPort: port})
	if err != nil {
		t.Fatal(err)
	}
	if p.Kind() != TypeHTTP {
		t.Errorf("Kind() = %v ; want http", p.Kind())
	}
	pass, _, err := p.Attempt(context.Background(), host)
	if !pass || err != nil {
		t.Errorf("Attempt = (%v,%v) ; want (true,nil)", pass, err)
	}
	if line := <-got; line != "GET /" {
		t.Errorf("server saw %q ; want default \"GET /\"", line)
	}
}

func TestHTTPProbe_2xxAccepted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	host, port := hostPort(t, srv.URL)
	p, _ := New(&Spec{Type: TypeHTTP, HTTPPort: port, HTTPPath: "/", HTTPMethod: "GET", Timeout: 500 * time.Millisecond})
	pass, _, err := p.Attempt(context.Background(), host)
	if err != nil || !pass {
		t.Errorf("Attempt = (%v,%v) ; want (true,nil)", pass, err)
	}
}

func TestHTTPProbe_500FailsByDefault(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	host, port := hostPort(t, srv.URL)
	p, _ := New(&Spec{Type: TypeHTTP, HTTPPort: port, Timeout: 500 * time.Millisecond})
	pass, _, _ := p.Attempt(context.Background(), host)
	if pass {
		t.Error("500 should fail default 2xx check ; got pass")
	}
}

func TestHTTPProbe_ExplicitStatusOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable) // 503 — would normally fail
	}))
	defer srv.Close()
	host, port := hostPort(t, srv.URL)
	p, _ := New(&Spec{Type: TypeHTTP, HTTPPort: port, HTTPStatusOK: []int{503}, Timeout: 500 * time.Millisecond})
	pass, _, err := p.Attempt(context.Background(), host)
	if !pass || err != nil {
		t.Errorf("503 in HTTPStatusOK should pass ; got (%v,%v)", pass, err)
	}
}

func TestHTTPProbe_ExplicitStatusOK_NotInSet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError) // 500, not in {200}
	}))
	defer srv.Close()
	host, port := hostPort(t, srv.URL)
	p, _ := New(&Spec{Type: TypeHTTP, HTTPPort: port, HTTPStatusOK: []int{200}, Timeout: 500 * time.Millisecond})
	pass, _, err := p.Attempt(context.Background(), host)
	if pass || err == nil {
		t.Errorf("500 not in accepted set should fail with error ; got (%v,%v)", pass, err)
	}
}

func TestHTTPProbe_BuildRequestError(t *testing.T) {
	// An invalid method makes http.NewRequestWithContext fail before any
	// network call, exercising the build-request error branch.
	p, _ := New(&Spec{Type: TypeHTTP, HTTPPort: 8080, HTTPMethod: "BAD METHOD", Timeout: 500 * time.Millisecond})
	pass, _, err := p.Attempt(context.Background(), "127.0.0.1")
	if pass || err == nil {
		t.Errorf("invalid method should error ; got (%v,%v)", pass, err)
	}
	if !strings.Contains(err.Error(), "build request") {
		t.Errorf("err = %v ; want a build-request error", err)
	}
}

func TestHTTPProbe_RedirectIsNotFollowed(t *testing.T) {
	hits := int64(0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&hits, 1)
		w.Header().Set("Location", "/elsewhere")
		w.WriteHeader(http.StatusFound) // 302 — not 2xx
	}))
	defer srv.Close()
	host, port := hostPort(t, srv.URL)
	p, _ := New(&Spec{Type: TypeHTTP, HTTPPort: port, Timeout: 500 * time.Millisecond})
	pass, _, _ := p.Attempt(context.Background(), host)
	if pass {
		t.Error("302 should fail default 2xx check (and not follow redirect)")
	}
	if got := atomic.LoadInt64(&hits); got != 1 {
		t.Errorf("server hit %d times ; redirect was followed", got)
	}
}

func TestHTTPProbe_TimeoutSurfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer srv.Close()
	host, port := hostPort(t, srv.URL)
	p, _ := New(&Spec{Type: TypeHTTP, HTTPPort: port, Timeout: 50 * time.Millisecond})
	pass, _, err := p.Attempt(context.Background(), host)
	if pass {
		t.Error("slow server should time out ; got pass")
	}
	if !IsTimeout(err) {
		t.Errorf("IsTimeout(%v) = false ; want true", err)
	}
}

func TestNewTCP_PortRequired(t *testing.T) {
	if _, err := New(&Spec{Type: TypeTCP}); err == nil {
		t.Error("TCP with no port should error ; got nil")
	}
}

func TestTCPProbe_AcceptsOpenPort_DefaultTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)
	// Timeout omitted -> default 1s branch.
	p, _ := New(&Spec{Type: TypeTCP, TCPPort: port})
	if p.Kind() != TypeTCP {
		t.Errorf("Kind() = %v ; want tcp", p.Kind())
	}
	pass, _, err := p.Attempt(context.Background(), "127.0.0.1")
	if !pass || err != nil {
		t.Errorf("Attempt open port = (%v,%v) ; want (true,nil)", pass, err)
	}
}

func TestTCPProbe_FailsClosedPort(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)
	ln.Close() // now guaranteed closed
	p, _ := New(&Spec{Type: TypeTCP, TCPPort: port, Timeout: 100 * time.Millisecond})
	pass, _, _ := p.Attempt(context.Background(), "127.0.0.1")
	if pass {
		t.Error("closed port should fail ; got pass")
	}
}

func TestNewRunner_Defaults(t *testing.T) {
	r := NewRunner(&fakeProbe{}, "127.0.0.1", RunnerOptions{})
	if r.period != time.Second {
		t.Errorf("period = %v ; want 1s default", r.period)
	}
	if r.failureK != 3 {
		t.Errorf("failureK = %d ; want 3 default", r.failureK)
	}
	if r.successK != 1 {
		t.Errorf("successK = %d ; want 1 default", r.successK)
	}
	if r.now == nil {
		t.Error("now func should default to time.Now ; got nil")
	}
	if r.Status() != StatusUnknown {
		t.Errorf("initial Status = %v ; want unknown", r.Status())
	}
}

func TestRunner_HealthyAfterSuccessThreshold(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	host, port := hostPort(t, srv.URL)
	s := &Spec{
		Type: TypeHTTP, HTTPPort: port, Timeout: 200 * time.Millisecond,
		Period: 10 * time.Millisecond, FailureThreshold: 2, SuccessThreshold: 1,
	}
	probe, _ := New(s)
	r := NewRunnerFromSpec(probe, host, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)
	deadline := time.After(3 * time.Second)
	for {
		select {
		case res, ok := <-r.Results():
			if !ok {
				t.Fatal("results channel closed before healthy")
			}
			if res.Status == StatusHealthy {
				if r.Status() != StatusHealthy {
					t.Errorf("Status() = %v ; want healthy", r.Status())
				}
				r.Stop()
				return
			}
		case <-deadline:
			t.Fatal("never went healthy in 3s")
		}
	}
}

func TestRunner_UnhealthyAfterFailureThreshold(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)
	ln.Close()
	s := &Spec{
		Type: TypeTCP, TCPPort: port, Timeout: 50 * time.Millisecond,
		Period: 10 * time.Millisecond, FailureThreshold: 2, SuccessThreshold: 1,
	}
	probe, _ := New(s)
	r := NewRunnerFromSpec(probe, "127.0.0.1", s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)
	deadline := time.After(3 * time.Second)
	for {
		select {
		case res, ok := <-r.Results():
			if !ok {
				t.Fatal("results closed before unhealthy")
			}
			if res.Status == StatusUnhealthy {
				r.Stop()
				return
			}
		case <-deadline:
			t.Fatal("never went unhealthy in 3s")
		}
	}
}

func TestRunner_InitialDelayDelaysFirstAttempt(t *testing.T) {
	fp := &fakeProbe{pass: true, kind: TypeTCP}
	r := NewRunner(fp, "127.0.0.1", RunnerOptions{
		Period: 5 * time.Millisecond, InitialDelay: 200 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)
	time.Sleep(80 * time.Millisecond) // well under the initial delay
	if got := atomic.LoadInt64(&fp.hits); got != 0 {
		t.Errorf("probe hit %d times before initial delay elapsed ; want 0", got)
	}
	r.Stop() // exercises the stop branch inside the initial-delay select
}

func TestRunner_InitialDelayThenRuns(t *testing.T) {
	fp := &fakeProbe{pass: true, kind: TypeTCP}
	r := NewRunner(fp, "127.0.0.1", RunnerOptions{
		Period: 5 * time.Millisecond, InitialDelay: 10 * time.Millisecond, SuccessThreshold: 1,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)
	select {
	case res, ok := <-r.Results():
		if !ok {
			t.Fatal("results closed before first attempt")
		}
		if res.Status != StatusHealthy {
			t.Errorf("first result after delay = %v ; want healthy", res.Status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no result after initial delay elapsed")
	}
	r.Stop()
}

func TestRunner_CtxCancelDuringInitialDelay(t *testing.T) {
	fp := &fakeProbe{pass: true, kind: TypeTCP}
	r := NewRunner(fp, "127.0.0.1", RunnerOptions{
		Period: time.Second, InitialDelay: 10 * time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel during initial delay")
	}
	if got := atomic.LoadInt64(&fp.hits); got != 0 {
		t.Errorf("probe ran %d times despite cancel during initial delay", got)
	}
}

func TestRunner_CtxCancelInMainLoop(t *testing.T) {
	fp := &fakeProbe{pass: true, kind: TypeTCP}
	r := NewRunner(fp, "127.0.0.1", RunnerOptions{Period: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	// The immediate first attempt has fired; now cancel to hit the
	// ctx.Done branch of the main loop.
	<-r.Results()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel in main loop")
	}
}

func TestRunner_StopInMainLoop(t *testing.T) {
	fp := &fakeProbe{pass: false, kind: TypeTCP}
	r := NewRunner(fp, "127.0.0.1", RunnerOptions{Period: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	<-r.Results()
	r.Stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after Stop in main loop")
	}
}

func TestRunner_StopIsIdempotent(t *testing.T) {
	r := NewRunner(&fakeProbe{kind: TypeTCP}, "127.0.0.1", RunnerOptions{Period: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)
	r.Stop()
	r.Stop() // must not panic
}

func TestRunner_InjectedClock(t *testing.T) {
	fixed := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	fp := &fakeProbe{pass: true, kind: TypeTCP}
	r := NewRunner(fp, "127.0.0.1", RunnerOptions{
		Period: time.Hour, SuccessThreshold: 1, Now: func() time.Time { return fixed },
	})
	r.runOnce(context.Background())
	select {
	case res := <-r.Results():
		if !res.When.Equal(fixed) {
			t.Errorf("Result.When = %v ; want injected %v", res.When, fixed)
		}
		if res.Status != StatusHealthy || !res.Pass {
			t.Errorf("result = %+v ; want healthy pass", res)
		}
	default:
		t.Fatal("no result emitted by runOnce")
	}
}

func TestRunner_DropsWhenBufferFull(t *testing.T) {
	// With a 32-slot buffer and no consumer, the 33rd send must hit the
	// non-blocking default (drop) branch without blocking.
	fp := &fakeProbe{pass: true, kind: TypeTCP}
	r := NewRunner(fp, "127.0.0.1", RunnerOptions{Period: time.Hour})
	done := make(chan struct{})
	go func() {
		for i := 0; i < 40; i++ {
			r.runOnce(context.Background())
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runOnce blocked when buffer full instead of dropping")
	}
}

func TestIsTimeout_DistinguishesError(t *testing.T) {
	if IsTimeout(nil) {
		t.Error("nil err -> IsTimeout=true")
	}
	if !IsTimeout(context.DeadlineExceeded) {
		t.Error("DeadlineExceeded -> IsTimeout=false")
	}
	if IsTimeout(errors.New("connection refused")) {
		t.Error("plain error -> IsTimeout=true")
	}
}
