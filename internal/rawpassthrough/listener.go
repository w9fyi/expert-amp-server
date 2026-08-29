// Package rawpassthrough exposes the amplifier's serial link over TCP so a
// single external client can drive it directly.
//
// The amplifier tolerates one serial master at a time, so this is an exclusive
// lease, not a multiplexer: while a client is connected the server's own
// polling is stopped and its writes are refused. Bytes are forwarded verbatim
// in both directions. The amplifier->client direction is additionally tapped
// (without altering it) so telemetry keeps flowing and overtemperature
// protection stays engaged.
package rawpassthrough

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/FtlC-ian/expert-amp-server/internal/runtime"
	"github.com/FtlC-ian/expert-amp-server/internal/transport"
)

const (
	// keepAlivePeriod reaps a client that has gone away without closing its
	// socket. There is deliberately no data-idle timeout: a legitimate client
	// may sit connected without polling.
	keepAlivePeriod = 30 * time.Second

	// copyBufferSize is a single serial read's worth of bytes. The display
	// frame this amplifier emits is 371 bytes.
	copyBufferSize = 4096
)

// Lease is the subset of the actuation coordinator this package needs.
type Lease interface {
	Acquire() bool
	Release()
}

// Config configures a Controller.
type Config struct {
	Enabled       bool
	ListenAddress string
	Source        *runtime.SerialSource
	Lease         Lease
}

// Controller accepts raw TCP clients and leases them the serial port.
type Controller struct {
	cfg Config

	mu       sync.Mutex
	listener net.Listener
	conn     net.Conn
	handle   *runtime.RawPassthroughHandle
	since    time.Time
	lastTrip *runtime.TripReason
}

// New builds a Controller. It returns nil when passthrough is not usable, so
// callers can treat "disabled" and "no serial port" identically.
func New(cfg Config) *Controller {
	if !cfg.Enabled || cfg.Source == nil || cfg.ListenAddress == "" {
		return nil
	}
	return &Controller{cfg: cfg}
}

// Start listens and serves until ctx is canceled. It blocks.
func (c *Controller) Start(ctx context.Context) error {
	if c == nil {
		return nil
	}
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", c.cfg.ListenAddress)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.listener = listener
	c.mu.Unlock()

	log.Printf("raw serial passthrough listening on %s (serial port is leased exclusively while a client is connected)", c.cfg.ListenAddress)

	go func() {
		<-ctx.Done()
		_ = listener.Close()
		// Drop any live session so the port is returned on shutdown. A readLoop
		// parked on lifecycleMu cannot observe cancellation on its own.
		c.closeActiveConn()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.Printf("raw passthrough accept failed: %v", err)
			continue
		}
		go c.serve(ctx, conn)
	}
}

func (c *Controller) serve(ctx context.Context, conn net.Conn) {
	remote := conn.RemoteAddr().String()

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(keepAlivePeriod)
	}

	// Refuse rather than interrupt an automatic transaction that is already
	// driving the amplifier. Matches the coordinator's reject-don't-queue rule.
	if c.cfg.Lease != nil && !c.cfg.Lease.Acquire() {
		log.Printf("raw passthrough refused %s: %s", remote, transport.ActuationBusyError("").Error())
		_ = conn.Close()
		return
	}

	handle, err := c.cfg.Source.BeginRawPassthrough(ctx)
	if err != nil {
		if c.cfg.Lease != nil {
			c.cfg.Lease.Release()
		}
		log.Printf("raw passthrough refused %s: %v", remote, err)
		_ = conn.Close()
		return
	}

	c.mu.Lock()
	c.conn = conn
	c.handle = handle
	c.since = time.Now()
	c.mu.Unlock()

	log.Printf("raw passthrough client %s connected; server polling paused", remote)

	var once sync.Once
	done := make(chan struct{})
	finish := func() {
		once.Do(func() {
			close(done)
			_ = conn.Close()
			handle.Close()
			if c.cfg.Lease != nil {
				c.cfg.Lease.Release()
			}
			c.mu.Lock()
			c.conn = nil
			c.handle = nil
			c.mu.Unlock()
		})
	}
	defer finish()

	// End the session when the tap says overtemperature control must be
	// restored. Closing the socket unblocks both copies through their normal
	// error paths; the server then reclaims the port and its own authorized
	// safety path acts.
	go func() {
		select {
		case reason, ok := <-handle.Trip():
			if !ok {
				return
			}
			c.mu.Lock()
			c.lastTrip = &reason
			c.mu.Unlock()
			log.Printf("raw passthrough disconnecting %s: %s (%.1fC >= %.1fC)", remote, reason.Reason, reason.TemperatureC, reason.ThresholdC)
			finish()
		case <-done:
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	// client -> amplifier, forwarded verbatim.
	go func() {
		defer wg.Done()
		defer finish()
		_, _ = io.Copy(handle.Port(), conn)
	}()

	// amplifier -> client, forwarded verbatim and tapped.
	go func() {
		defer wg.Done()
		defer finish()
		buf := make([]byte, copyBufferSize)
		port := handle.Port()
		for {
			n, err := port.Read(buf)
			if n > 0 {
				chunk := buf[:n]
				if _, werr := conn.Write(chunk); werr != nil {
					return
				}
				// Observe after forwarding so the client is never delayed by
				// decoding, and never affected by it.
				handle.ObserveFromAmp(chunk)
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					continue
				}
				return
			}
			select {
			case <-done:
				return
			default:
			}
		}
	}()

	wg.Wait()
	log.Printf("raw passthrough client %s disconnected; server polling resumed", remote)
}

// Addr is the address the listener actually bound to, or "" before Start binds.
func (c *Controller) Addr() string {
	if c == nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.listener == nil {
		return ""
	}
	return c.listener.Addr().String()
}

func (c *Controller) closeActiveConn() {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// Status describes the passthrough listener for the diagnostics API.
type Status struct {
	Enabled                  bool   `json:"enabled"`
	ListenAddress            string `json:"listenAddress,omitempty"`
	ClientConnected          bool   `json:"clientConnected"`
	ClientAddress            string `json:"clientAddress,omitempty"`
	ConnectedSince           string `json:"connectedSince,omitempty"`
	StatusFramesObserved     int64  `json:"statusFramesObserved,omitempty"`
	DisplayFramesObserved    int64  `json:"displayFramesObserved,omitempty"`
	OvertemperatureProtected bool   `json:"overtemperatureProtected"`
	Note                     string `json:"note,omitempty"`
}

// Status snapshots the listener state.
func (c *Controller) Status() Status {
	if c == nil {
		return Status{Enabled: false}
	}
	c.mu.Lock()
	conn := c.conn
	handle := c.handle
	since := c.since
	c.mu.Unlock()

	out := Status{Enabled: true, ListenAddress: c.cfg.ListenAddress}
	if conn == nil || handle == nil {
		out.Note = "no raw client connected; the server owns the serial port"
		return out
	}
	stats := handle.Stats()
	out.ClientConnected = true
	out.ClientAddress = conn.RemoteAddr().String()
	out.ConnectedSince = since.UTC().Format(time.RFC3339)
	out.StatusFramesObserved = stats.StatusFramesSeen
	out.DisplayFramesObserved = stats.DisplayFrames
	out.OvertemperatureProtected = handle.OvertemperatureProtected()
	if out.OvertemperatureProtected {
		out.Note = "a raw client holds the serial port; server writes are refused, and overtemperature protection is engaged through the passthrough tap"
	} else {
		out.Note = "a raw client holds the serial port and is not polling protocol status; overtemperature protection is blind to protocol-native temperature"
	}
	return out
}
