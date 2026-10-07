<p align="center"><img src="https://raw.githubusercontent.com/go-net-health/brand/main/social/go-net-health-health.png" alt="go-net-health/health" width="720"></p>

# health — go-net-health

[![Docs](https://img.shields.io/badge/docs-mkdocs--material-0D9488)](https://go-net-health.github.io/docs/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.27.1%2B-00ADD8)](https://go.dev/dl/)
[![Coverage](https://img.shields.io/badge/coverage-100%25-1a7f37)](#tests--coverage)

**A pure-Go (no cgo) active health-probe runner** — stateless TCP/HTTP network
checks folded into a rolling `healthy` / `unhealthy` / `unknown` verdict.

A `Probe` runs a single attempt of a configured check against a target address
and reports pass/fail plus latency. A `Runner` drives a `Probe` on a fixed
period, honours an optional initial delay, and derives a rolling `Status` by
stacking consecutive passes/failures against success/failure thresholds — a
Kubernetes-style liveness/readiness probe as a small, embeddable library.

It has **no opinion** on what to do with a verdict: `Healthy` vs `Unhealthy` is
just a fact your own state machine (a supervisor, a load-balancer, a respawn
loop) acts on.

## Features

- **Two probe kinds** — `TypeHTTP` (request + status-code check) and `TypeTCP`
  (connect check). `TypeExec` is reserved.
- **HTTP semantics that match a real health check** — configurable path, method,
  per-probe timeout; accepts any `2xx` by default or an explicit status set;
  **does not follow redirects** (it checks *this* endpoint, not where it points).
- **Rolling status** — `SuccessThreshold` consecutive passes → `Healthy`,
  `FailureThreshold` consecutive failures → `Unhealthy`, `Unknown` until a
  threshold is met or the initial delay elapses.
- **A `Runner` per target** — fixed period, one-shot initial delay, a buffered
  `Result` stream, and a clean `Stop()` that never loses the final observation.
- **`IsTimeout`** distinguishes "never reached the target" from "the target said
  no", so a grace period can apply only to unreachable targets.
- **Injectable clock** for deterministic tests — no wall time on the hot path.

Pure Go, `CGO_ENABLED=0`, **zero third-party dependencies** (only the standard
library: `net`, `net/http`, `context`, `time`), **100% test coverage**, `gofmt`
+ `go vet` clean, and green across the six 64-bit Go targets (amd64, arm64,
riscv64, loong64, ppc64le, s390x).

## Install

```sh
go get github.com/go-net-health/health
```

## Usage

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-net-health/health"
)

func main() {
	// Build an HTTP probe: GET /healthz on port 8080, any 2xx is healthy.
	spec := &health.Spec{
		Type:             health.TypeHTTP,
		HTTPPort:         8080,
		HTTPPath:         "/healthz",
		Timeout:          time.Second,
		Period:           2 * time.Second,
		InitialDelay:     time.Second,
		FailureThreshold: 3,
		SuccessThreshold: 1,
	}
	probe, err := health.New(spec)
	if err != nil {
		panic(err)
	}

	// A one-off attempt:
	pass, latency, err := probe.Attempt(context.Background(), "10.0.0.7")
	fmt.Println(pass, latency, err)

	// Or drive it on a schedule and consume the rolling verdict:
	r := health.NewRunnerFromSpec(probe, "10.0.0.7", spec)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)
	for res := range r.Results() {
		fmt.Printf("%s  pass=%v  latency=%s\n", res.Status, res.Pass, res.Latency)
		if res.Status == health.StatusHealthy {
			r.Stop()
		}
	}
}
```

A minimal TCP probe is just `&health.Spec{Type: health.TypeTCP, TCPPort: 22}` —
zero values fill in path `/`, method `GET`, a 1s timeout, a 1s period, a failure
threshold of 3 and a success threshold of 1.

## API

```go
type Type int
const (
	TypeNone Type = iota // no probe configured; New returns (nil, nil)
	TypeHTTP             // HTTP request + status-code check
	TypeTCP              // TCP connect check
	TypeExec             // reserved, not yet supported
)

type Status int
const (
	StatusUnknown Status = iota
	StatusHealthy
	StatusUnhealthy
)

// Spec is the dependency-free configuration for a probe and its runner.
type Spec struct {
	Type             Type
	HTTPPath         string // default "/"
	HTTPPort         int    // required for HTTP
	HTTPMethod       string // default "GET"
	HTTPStatusOK     []int  // empty = any 2xx
	TCPPort          int    // required for TCP
	InitialDelay     time.Duration
	Period           time.Duration // default 1s
	Timeout          time.Duration // default 1s
	FailureThreshold int           // default 3
	SuccessThreshold int           // default 1
}

// New translates a Spec into a runnable Probe. (nil, nil) for TypeNone.
func New(s *Spec) (Probe, error)

type Probe interface {
	Attempt(ctx context.Context, addr string) (pass bool, latency time.Duration, err error)
	Kind() Type
}

type Result struct {
	When    time.Time
	Status  Status
	Pass    bool
	Latency time.Duration
	Err     error
}

func NewRunner(probe Probe, addr string, opts RunnerOptions) *Runner
func NewRunnerFromSpec(probe Probe, addr string, s *Spec) *Runner
func (r *Runner) Run(ctx context.Context)   // blocks until ctx done / Stop()
func (r *Runner) Results() <-chan Result    // closed when Run returns
func (r *Runner) Status() Status            // rolling verdict, goroutine-safe
func (r *Runner) Stop()                     // idempotent

func IsTimeout(err error) bool // "never reached" vs "target refused"
```

## Tests & coverage

The suite spins up `127.0.0.1` loopback servers in-process (`httptest` and
`net.Listen`) plus a deterministic fake probe, exercising every branch —
including the timeout, connection-refused, redirect-not-followed and
buffer-drop paths — with the race detector on.

```sh
COVERPKG=$(go list ./... | paste -sd, -)
go test -race -coverpkg="$COVERPKG" -coverprofile=cover.out ./...
go tool cover -func=cover.out | tail -1   # 100.0%
```

The loopback probe servers run under `qemu-user`, so the cross-arch lanes run the
full suite on all six 64-bit Go targets.

## License

BSD-3-Clause — see [LICENSE](LICENSE). Copyright the go-net-health/health authors.
