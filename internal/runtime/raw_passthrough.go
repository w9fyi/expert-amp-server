package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/FtlC-ian/expert-amp-server/internal/api"
	"github.com/FtlC-ian/expert-amp-server/internal/protocol"
	"github.com/FtlC-ian/expert-amp-server/internal/serial"
	"github.com/FtlC-ian/expert-amp-server/internal/tempunit"
)

// ErrRawPassthroughBusy reports that another raw client already holds the port.
var ErrRawPassthroughBusy = errors.New("a raw passthrough client is already connected")

// ErrRawPassthroughUnavailable reports that no serial port is configured.
var ErrRawPassthroughUnavailable = errors.New("raw passthrough requires a configured serial port")

// ProvenancePassthroughTap marks status observed by copying another client's
// traffic during a raw lease. It is deliberately distinct from "status-poll":
// the amplifier answered a question the server did not ask, so the reply is
// good enough to display but is never authority to actuate. Every automatic
// actuation gate tests for "status-poll" exactly, so this value is refused by
// fan policy, overtemperature standby, and menu debug without further work.
const ProvenancePassthroughTap = "passthrough-tap"

// RawPassthroughHandle is one exclusive raw session over the physical serial
// port. The read loop is fully stopped for its lifetime: the caller owns the
// port until Close.
//
// The amplifier tolerates a single serial master, so this is a lease rather
// than a multiplexer. The server keeps observing the amp->client direction
// (see ObserveFromAmp) but never writes while the lease is held, and never
// ends the session on what it observes.
type RawPassthroughHandle struct {
	source *SerialSource
	port   serial.Port

	closeOnce sync.Once
	closeFn   func()

	statusDecoder  *protocol.StatusStreamDecoder
	displayDecoder *protocol.DisplayStreamDecoder

	mu               sync.Mutex
	startedAt        time.Time
	statusFramesSeen int64
	displayFrames    int64
	lastStatusAt     time.Time
}

// Port is the exclusive serial handle for the duration of the session.
func (h *RawPassthroughHandle) Port() serial.Port { return h.port }

// BeginRawPassthrough stops the internal read loop, waits for it to unwind, and
// returns an exclusive handle to the serial port.
//
// This mirrors SendWake's steal-and-restore sequence with one deliberate
// difference: a passthrough session lasts as long as its TCP client stays
// connected, so writeMu is released as soon as the steal completes and only
// lifecycleMu is held for the full duration. rawPassthroughActive, set under
// writeMu, is what makes concurrent server writes fail fast instead of blocking
// on that lifecycleMu.
func (s *SerialSource) BeginRawPassthrough(ctx context.Context) (*RawPassthroughHandle, error) {
	if s == nil {
		return nil, ErrRawPassthroughUnavailable
	}
	if s.cfg.Port == "" {
		return nil, ErrRawPassthroughUnavailable
	}
	if !s.rawPassthroughClaim.TryLock() {
		return nil, ErrRawPassthroughBusy
	}

	release := func() { s.rawPassthroughClaim.Unlock() }

	s.writeMu.Lock()
	s.lifecycleMu.Lock()

	if err := ctx.Err(); err != nil {
		s.lifecycleMu.Unlock()
		s.writeMu.Unlock()
		release()
		return nil, fmt.Errorf("raw passthrough not started, server is shutting down: %w", err)
	}

	if err := s.retireCurrentSessionForReason(ctx, "raw passthrough"); err != nil {
		s.lifecycleMu.Unlock()
		s.writeMu.Unlock()
		// The retire already cleared s.port even though it could not confirm the
		// read loop unwound, so without this the loop would sit out its full
		// backoff with no polling and no safety observation.
		s.signalReconnect()
		release()
		return nil, err
	}

	opener := s.opener
	if opener == nil {
		opener = serial.OpenRealPort{}
	}
	port, err := opener.Open(s.cfg.Port, s.cfg.BaudRate)
	if err != nil {
		// Nothing was handed over, so restore normal operation immediately.
		s.lifecycleMu.Unlock()
		s.writeMu.Unlock()
		s.signalReconnect()
		release()
		return nil, fmt.Errorf("open serial %s for raw passthrough: %w", s.cfg.Port, err)
	}

	// Match the live read loop's port setup exactly. Skipping this would
	// silently change line-state behavior relative to normal operation.
	if err := port.SetReadTimeout(s.cfg.ReadTimeout); err != nil {
		_ = port.Close()
		s.lifecycleMu.Unlock()
		s.writeMu.Unlock()
		s.signalReconnect()
		release()
		return nil, fmt.Errorf("set read timeout for raw passthrough: %w", err)
	}
	if s.cfg.AssertDTR {
		if err := port.SetDTR(true); err != nil {
			_ = port.Close()
			s.lifecycleMu.Unlock()
			s.writeMu.Unlock()
			s.signalReconnect()
			release()
			return nil, fmt.Errorf("set DTR for raw passthrough: %w", err)
		}
	}
	if s.cfg.AssertRTS {
		if err := port.SetRTS(true); err != nil {
			_ = port.Close()
			s.lifecycleMu.Unlock()
			s.writeMu.Unlock()
			s.signalReconnect()
			release()
			return nil, fmt.Errorf("set RTS for raw passthrough: %w", err)
		}
	}

	s.rawPassthroughActive = true
	s.writeMu.Unlock()
	// lifecycleMu stays held until Close: it is what keeps readLoop parked.

	handle := &RawPassthroughHandle{
		source:         s,
		port:           port,
		statusDecoder:  protocol.NewStatusStreamDecoder(),
		displayDecoder: protocol.NewDisplayStreamDecoder(protocol.StreamDecoderConfig{MinFrameLen: s.cfg.MinFrameLen, MaxBuffer: s.cfg.MaxBuffer}),
		startedAt:      time.Now(),
	}
	handle.closeFn = func() {
		_ = port.Close()
		// Release lifecycleMu before clearing the flag. The other order leaves a
		// window where the flag reads false while lifecycleMu is still held, so a
		// concurrent SendWake passes the gate and then blocks on lifecycleMu --
		// while holding the actuation coordinator's mutex.
		s.lifecycleMu.Unlock()
		s.writeMu.Lock()
		s.rawPassthroughActive = false
		s.writeMu.Unlock()
		s.signalReconnect()
		release()
	}
	return handle, nil
}

// StatusPollingActive reports whether the server will poll for protocol-native
// status once it owns the port again. Overtemperature protection is reachable
// only through those replies: applyStatusFrameFromSession is the sole caller of
// monitoring.Controller.Observe, and the display path never reaches it.
func (s *SerialSource) StatusPollingActive() bool {
	if s == nil {
		return false
	}
	switch s.pollingMode() {
	case "both", "status":
		return s.statusPollEnabled()
	default:
		return false
	}
}

// Close ends the session, returns the port to the internal read loop and lets
// polling resume. It is safe to call more than once.
func (h *RawPassthroughHandle) Close() {
	if h == nil {
		return
	}
	h.closeOnce.Do(h.closeFn)
}

// ObserveFromAmp decodes a chunk of the amplifier->client byte stream without
// consuming or altering it. The caller still forwards the same bytes verbatim.
//
// This is display evidence only. Frames observed here are labelled
// ProvenancePassthroughTap and are never delivered to the safety, fan or
// menu-debug controllers, and never end the session. Server-side automatic
// control is not degraded during a lease, it is refused up front: a session
// cannot start while automatic fan control or overtemperature standby is armed
// (see ArmedAutomaticControls), so there is no protection here to preserve.
func (h *RawPassthroughHandle) ObserveFromAmp(chunk []byte) {
	if h == nil || len(chunk) == 0 || h.source == nil {
		return
	}
	s := h.source

	for _, frame := range h.statusDecoder.Push(chunk) {
		temperatureUnit := tempunit.Celsius
		if s.cfg.TemperatureUnitFn != nil {
			temperatureUnit = s.cfg.TemperatureUnitFn()
		}
		status, err := protocol.StatusFromFrameWithTemperatureUnit(frame, "serial", temperatureUnit)
		if err != nil {
			s.decodeErrors.Add(1)
			continue
		}
		h.mu.Lock()
		h.statusFramesSeen++
		h.lastStatusAt = time.Now()
		h.mu.Unlock()

		// Relabel before publishing. protocol decoding stamps "status-poll",
		// which is the exact string every actuation gate treats as authority,
		// and the server did not send this poll.
		status.Provenance = ProvenancePassthroughTap
		if s.statusState != nil {
			s.statusState.UpdateProtocolNative(status)
		}
	}

	for _, frame := range h.displayDecoder.Push(chunk) {
		state, err := protocol.StateFromFrame(frame)
		if err != nil {
			s.decodeErrors.Add(1)
			continue
		}
		h.mu.Lock()
		h.displayFrames++
		h.mu.Unlock()

		telemetry := protocol.TelemetryFromDisplayState(state, "serial")
		s.mu.Lock()
		s.latest = Update{
			State:     state,
			Telemetry: telemetry,
			Frame:     api.FrameInfo{Source: "serial", Length: len(frame), StartOffset: protocol.LCDDataOffset(frame)},
			FrameKind: "serial",
			Source:    "serial",
		}
		s.mu.Unlock()
		s.framesSeen.Add(1)
		s.lastFrameLen.Store(int64(len(frame)))
		s.lastFrameAt.Store(time.Now().Unix())
	}
}

// RawPassthroughStats describes an active passthrough session for status reporting.
type RawPassthroughStats struct {
	StartedAt        time.Time
	StatusFramesSeen int64
	DisplayFrames    int64
	LastStatusAt     time.Time
}

// Stats snapshots the tap counters.
func (h *RawPassthroughHandle) Stats() RawPassthroughStats {
	if h == nil {
		return RawPassthroughStats{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return RawPassthroughStats{
		StartedAt:        h.startedAt,
		StatusFramesSeen: h.statusFramesSeen,
		DisplayFrames:    h.displayFrames,
		LastStatusAt:     h.lastStatusAt,
	}
}

// TapIsFresh reports whether the connected client is currently supplying
// protocol status often enough for tapped telemetry to be worth displaying.
// It says nothing about protection: tapped state is never authority to
// actuate, however fresh it is.
func (h *RawPassthroughHandle) TapIsFresh() bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.lastStatusAt.IsZero() && time.Since(h.lastStatusAt) <= RecentContactWindow
}
