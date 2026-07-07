package health_test

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/go-net-health/health"
)

// ExampleNew builds a TCP probe from a Spec and runs a single attempt
// against a freshly-opened loopback listener.
func ExampleNew() {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)

	probe, err := health.New(&health.Spec{
		Type:    health.TypeTCP,
		TCPPort: port,
		Timeout: time.Second,
	})
	if err != nil {
		panic(err)
	}
	pass, _, _ := probe.Attempt(context.Background(), "127.0.0.1")
	fmt.Println("open port healthy:", pass)
	// Output: open port healthy: true
}
