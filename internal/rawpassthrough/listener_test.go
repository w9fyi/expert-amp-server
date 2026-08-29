package rawpassthrough

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/FtlC-ian/expert-amp-server/internal/runtime"
	"github.com/FtlC-ian/expert-amp-server/internal/serial"
)

// fakePort is a serial.Port backed by in-memory pipes, so a test can act as the
// amplifier on the far end of the lease.
type fakePort struct {
	toHost   chan []byte
	fromHost chan []byte

	mu     sync.Mutex
	closed bool
	done   chan struct{}
}

func newFakePort() *fakePort {
	return &fakePort{
		toHost:   make(chan []byte, 16),
		fromHost: make(chan []byte, 16),
		done:     make(chan struct{}),
	}
}

func (p *fakePort) Read(buf []byte) (int, error) {
	select {
	case chunk := <-p.toHost:
		return copy(buf, chunk), nil
	case <-p.done:
		return 0, errors.New("port closed")
	}
}

func (p *fakePort) Write(buf []byte) (int, error) {
	chunk := append([]byte(nil), buf...)
	select {
	case p.fromHost <- chunk:
		return len(buf), nil
	case <-p.done:
		return 0, errors.New("port closed")
	}
}

func (p *fakePort) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		close(p.done)
	}
	return nil
}

func (p *fakePort) SetReadTimeout(time.Duration) error { return nil }
func (p *fakePort) SetDTR(bool) error                  { return nil }
func (p *fakePort) SetRTS(bool) error                  { return nil }

type fakeOpener struct {
	mu    sync.Mutex
	ports []*fakePort
}

func (o *fakeOpener) Open(string, int) (serial.Port, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	port := newFakePort()
	o.ports = append(o.ports, port)
	return port, nil
}

func (o *fakeOpener) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.ports)
}

// latest is the most recently opened port. While a passthrough session is live
// that is the leased one, because the internal read loop is stopped and cannot
// open another.
func (o *fakeOpener) latest() *fakePort {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.ports) == 0 {
		return nil
	}
	return o.ports[len(o.ports)-1]
}

// waitForNewPort blocks until a port beyond baseline has been opened.
func (o *fakeOpener) waitForNewPort(t *testing.T, baseline int) *fakePort {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if o.count() > baseline {
			return o.latest()
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("passthrough never leased a serial port")
	return nil
}

func newTestController(t *testing.T) (*Controller, *fakeOpener, func()) {
	t.Helper()
	opener := &fakeOpener{}
	src := runtime.NewSerialSource(runtime.SerialSourceConfig{
		Port:             "/dev/ttyTEST0",
		BaudRate:         115200,
		ReadTimeout:      10 * time.Millisecond,
		ReadSize:         512,
		MinFrameLen:      64,
		MaxBuffer:        8192,
		IOTimeout:        2 * time.Second,
		ReconnectBackoff: 50 * time.Millisecond,
	}, opener, runtime.Update{})

	ctx, cancel := context.WithCancel(context.Background())
	src.Start(ctx)

	controller := New(Config{Enabled: true, ListenAddress: "127.0.0.1:0", Source: src})
	if controller == nil {
		cancel()
		t.Fatal("expected a controller for an enabled config")
	}
	return controller, opener, cancel
}

// startListener runs the controller on an ephemeral port and returns the bound
// address, without dialing it (a probe connection would consume a lease).
func startListener(t *testing.T, ctx context.Context, c *Controller) string {
	t.Helper()
	go func() { _ = c.Start(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if addr := c.Addr(); addr != "" {
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("listener never bound")
	return ""
}

func TestPassthroughPipesBytesBothDirections(t *testing.T) {
	controller, opener, cancel := newTestController(t)
	defer cancel()

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	addr := startListener(t, ctx, controller)

	baseline := opener.count()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	port := opener.waitForNewPort(t, baseline)

	// client -> amplifier
	if _, err := conn.Write([]byte{0x55, 0x55, 0x55, 0x01, 0x90, 0x90}); err != nil {
		t.Fatalf("write to socket: %v", err)
	}
	select {
	case got := <-port.fromHost:
		if len(got) != 6 || got[4] != 0x90 {
			t.Fatalf("amplifier received %x, want the status poll verbatim", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bytes written by the client never reached the serial port")
	}

	// amplifier -> client
	reply := []byte{0xAA, 0xAA, 0xAA, 0x01, 0x0C, 0x0C}
	port.toHost <- reply
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, len(reply))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read from socket: %v", err)
	}
	for i := range reply {
		if buf[i] != reply[i] {
			t.Fatalf("client received %x, want %x forwarded verbatim", buf, reply)
		}
	}
}

func TestPassthroughRejectsSecondConcurrentClient(t *testing.T) {
	controller, opener, cancel := newTestController(t)
	defer cancel()

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	addr := startListener(t, ctx, controller)

	baseline := opener.count()
	first, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial first: %v", err)
	}
	defer first.Close()

	opener.waitForNewPort(t, baseline)

	second, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial second: %v", err)
	}
	defer second.Close()

	// The server closes a rejected client immediately rather than queueing it.
	_ = second.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := second.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("expected the second client to be closed immediately, got %v", err)
	}
}

func TestNewReturnsNilWhenDisabled(t *testing.T) {
	if c := New(Config{Enabled: false, ListenAddress: ":7388"}); c != nil {
		t.Fatal("expected nil controller when passthrough is disabled")
	}
	if c := New(Config{Enabled: true, ListenAddress: ""}); c != nil {
		t.Fatal("expected nil controller without a listen address")
	}
	if c := New(Config{Enabled: true, ListenAddress: ":7388", Source: nil}); c != nil {
		t.Fatal("expected nil controller without a serial source")
	}
}
