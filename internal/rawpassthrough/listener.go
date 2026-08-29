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
	"strconv"
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

	// writeTimeout bounds a write to the client. A peer that stops reading but
	// keeps its socket open holds its receive window at zero; keepalive probes
	// are still answered, so the connection looks healthy while the forwarding
	// goroutine blocks forever. That would silently freeze the tap, and with it
	// overtemperature protection, for as long as the client stays wedged.
	writeTimeout = 10 * time.Second

	// acceptBackoff paces the accept loop after a temporary error such as fd
	// exhaustion, instead of spinning at full speed.
	acceptBackoff = 100 * time.Millisecond
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

	// setupMu serializes session setup so the lease and the port are claimed as
	// one step. Without it two clients can both pass ActuationCoordinator's
	// Acquire, which is re-entrant for the same owner name, and the loser's
	// Release would then clear the winner's lease.
	setupMu sync.Mutex

	mu       sync.Mutex
	listener net.Listener
	conn     net.Conn
	handle   *runtime.RawPassthroughHandle
	since    time.Time
	// lastTrip is kept across sessions on purpose: a client that reconnects
	// after a thermal trip still needs to be able to see why it was dropped.
	// lastTripAt is what keeps that from reading as a property of the current
	// session.
	lastTrip   *runtime.TripReason
	lastTripAt time.Time
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
			// Back off rather than spin: under fd exhaustion Accept returns
			// immediately and a bare continue would flood the log at full speed.
			log.Printf("raw passthrough accept failed: %v", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(acceptBackoff):
			}
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

	// Claim the lease and the port as one atomic step; see setupMu.
	c.setupMu.Lock()

	c.mu.Lock()
	busy := c.handle != nil
	c.mu.Unlock()
	if busy {
		c.setupMu.Unlock()
		log.Printf("raw passthrough refused %s: %v", remote, runtime.ErrRawPassthroughBusy)
		_ = conn.Close()
		return
	}

	// Refuse rather than interrupt an automatic transaction that is already
	// driving the amplifier. Matches the coordinator's reject-don't-queue rule.
	if c.cfg.Lease != nil && !c.cfg.Lease.Acquire() {
		c.setupMu.Unlock()
		log.Printf("raw passthrough refused %s: %s", remote, transport.ActuationBusyError("").Error())
		_ = conn.Close()
		return
	}

	handle, err := c.cfg.Source.BeginRawPassthrough(ctx)
	if err != nil {
		if c.cfg.Lease != nil {
			c.cfg.Lease.Release()
		}
		c.setupMu.Unlock()
		log.Printf("raw passthrough refused %s: %v", remote, err)
		_ = conn.Close()
		return
	}

	c.mu.Lock()
	c.conn = conn
	c.handle = handle
	c.since = time.Now()
	c.mu.Unlock()
	c.setupMu.Unlock()

	if reason := handle.ProtectionUnavailableReason(); reason != "" {
		log.Printf("raw passthrough %s: overtemperature protection is not engaged for this session: %s", remote, reason)
	}

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
			// Compare before clearing: a reconnecting client can have already
			// installed its own session by the time this teardown runs, and
			// blindly nulling would report "no client connected" while that
			// session is live and would leave it unreachable on shutdown.
			if c.handle == handle {
				c.conn = nil
				c.handle = nil
			}
			c.mu.Unlock()
		})
	}
	defer finish()

	// serve must watch ctx itself. The listener's shutdown watcher takes a
	// one-shot snapshot of the active connection, so a session that is still
	// being established when cancellation lands would otherwise never be torn
	// down, leaking the lifecycle lock and leaving the port with the client.
	go func() {
		select {
		case <-ctx.Done():
			log.Printf("raw passthrough disconnecting %s: server is shutting down", remote)
			finish()
		case <-done:
		}
	}()

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
			c.lastTripAt = time.Now()
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
			select {
			case <-done:
				return
			default:
			}

			n, err := port.Read(buf)
			if n > 0 {
				chunk := buf[:n]
				_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
				if _, werr := conn.Write(chunk); werr != nil {
					log.Printf("raw passthrough write to client failed, ending session: %v", werr)
					return
				}
				// Observe after forwarding so the client is never delayed by
				// decoding, and never affected by it. Both stream decoders copy
				// out of chunk, so reusing buf across iterations is safe.
				handle.ObserveFromAmp(chunk)
			}
			if err != nil && !errors.Is(err, io.EOF) {
				return
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
	ProtectionGapReason      string `json:"protectionGapReason,omitempty"`
	LastTripReason           string `json:"lastTripReason,omitempty"`
	LastTripTemperatureC     string `json:"lastTripTemperatureC,omitempty"`
	LastTripAt               string `json:"lastTripAt,omitempty"`
	LastTripInThisSession    bool   `json:"lastTripInThisSession,omitempty"`
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
	lastTrip := c.lastTrip
	lastTripAt := c.lastTripAt
	c.mu.Unlock()

	out := Status{Enabled: true, ListenAddress: c.cfg.ListenAddress}
	if lastTrip != nil {
		out.LastTripReason = lastTrip.Reason
		out.LastTripTemperatureC = strconv.FormatFloat(lastTrip.TemperatureC, 'f', 1, 64)
		out.LastTripAt = lastTripAt.UTC().Format(time.RFC3339)
		// Without this an old trip reads as if it described the live session.
		out.LastTripInThisSession = conn != nil && lastTripAt.After(since)
	}
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
	out.ProtectionGapReason = handle.ProtectionUnavailableReason()
	if out.OvertemperatureProtected {
		out.Note = "a raw client holds the serial port; server writes are refused, and overtemperature protection is engaged through the passthrough tap"
	} else {
		out.Note = "a raw client holds the serial port and overtemperature protection is NOT engaged for this session"
	}
	return out
}
