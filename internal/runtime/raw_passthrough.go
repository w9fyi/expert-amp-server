package runtime

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/FtlC-ian/expert-amp-server/internal/api"
	"github.com/FtlC-ian/expert-amp-server/internal/monitoring"
	"github.com/FtlC-ian/expert-amp-server/internal/protocol"
	"github.com/FtlC-ian/expert-amp-server/internal/serial"
	"github.com/FtlC-ian/expert-amp-server/internal/tempunit"
)

// ErrRawPassthroughBusy reports that another raw client already holds the port.
var ErrRawPassthroughBusy = errors.New("a raw passthrough client is already connected")

// ErrRawPassthroughUnavailable reports that no serial port is configured.
var ErrRawPassthroughUnavailable = errors.New("raw passthrough requires a configured serial port")

// TripReason explains why the passthrough tap asked for the session to end.
type TripReason struct {
	Reason         string
	TemperatureC   float64
	ThresholdC     float64
	ProtocolNative bool
}

// RawPassthroughHandle is one exclusive raw session over the physical serial
// port. The read loop is fully stopped for its lifetime: the caller owns the
// port until Close.
//
// The amplifier tolerates a single serial master, so this is a lease rather
// than a multiplexer. The server keeps observing the amp->client direction
// (see ObserveFromAmp) but never writes while the lease is held.
type RawPassthroughHandle struct {
	source *SerialSource
	port   serial.Port

	closeOnce sync.Once
	closeFn   func()

	statusDecoder  *protocol.StatusStreamDecoder
	displayDecoder *protocol.DisplayStreamDecoder

	trip     chan TripReason
	tripOnce sync.Once

	mu               sync.Mutex
	startedAt        time.Time
	statusFramesSeen int64
	displayFrames    int64
	lastStatusAt     time.Time
}

// Port is the exclusive serial handle for the duration of the session.
func (h *RawPassthroughHandle) Port() serial.Port { return h.port }

// Trip fires when the tap concludes the session must end so the server can
// reclaim the port. It is never used to actuate the amplifier directly.
func (h *RawPassthroughHandle) Trip() <-chan TripReason { return h.trip }

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

	if err := s.retireCurrentSessionForReason(ctx, "raw passthrough"); err != nil {
		s.lifecycleMu.Unlock()
		s.writeMu.Unlock()
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
		trip:           make(chan TripReason, 1),
		startedAt:      time.Now(),
	}
	handle.closeFn = func() {
		_ = port.Close()
		s.writeMu.Lock()
		s.rawPassthroughActive = false
		s.writeMu.Unlock()
		s.lifecycleMu.Unlock()
		s.signalReconnect()
		release()
	}
	return handle, nil
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
// Frames observed this way update telemetry, but are deliberately NOT delivered
// to the safety, fan or menu-debug controllers: those act on what they see, and
// monitoring.Controller.Observe latches before attempting its toggle, so a
// tapped frame would burn its single no-retry attempt on a write that cannot
// succeed while the port is leased. Instead the tap only ever decides to end
// the session, after which the normal authorized path runs unmodified.
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

		s.markHealthy()
		if s.statusState != nil {
			s.statusState.UpdateProtocolNative(status)
		}
		h.evaluate(status, true)
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

		s.markHealthy()
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

		// Display-derived temperature is not trusted to actuate the amplifier,
		// and never does here. It is only good enough to decide to hand the
		// port back so the fully authorized path can look for itself.
		displayStatus := api.Status{Telemetry: telemetry}
		displayStatus.Source = "serial"
		displayStatus.Provenance = "display-frame"
		h.evaluate(displayStatus, false)
	}
}

// evaluate ends the session if temperature has reached the configured trip
// threshold. It never writes to the amplifier.
func (h *RawPassthroughHandle) evaluate(status api.Status, protocolNative bool) {
	s := h.source
	s.mu.RLock()
	safetySettings := s.safetySettings
	s.mu.RUnlock()
	if safetySettings == nil {
		return
	}
	settings := safetySettings()
	if !settings.Enabled || !settings.Armed || settings.Thresholds.TemperatureTripC <= 0 {
		return
	}
	observed := monitoring.Evaluate(status, settings.Enabled, settings.Thresholds).Observations.MaximumTemperatureC
	if observed == nil || *observed < settings.Thresholds.TemperatureTripC {
		return
	}
	h.signalTrip(TripReason{
		Reason:         "temperature reached the overtemperature trip threshold",
		TemperatureC:   *observed,
		ThresholdC:     settings.Thresholds.TemperatureTripC,
		ProtocolNative: protocolNative,
	})
}

func (h *RawPassthroughHandle) signalTrip(reason TripReason) {
	h.tripOnce.Do(func() {
		log.Printf("raw passthrough: ending session to restore overtemperature control (%.1fC >= %.1fC, protocolNative=%t)", reason.TemperatureC, reason.ThresholdC, reason.ProtocolNative)
		h.trip <- reason
		close(h.trip)
	})
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

// OvertemperatureProtected reports whether the tap is currently seeing the
// protocol-native status frames that overtemperature protection depends on.
// When false, the client is not polling status and the server is flying blind
// on anything but display-derived readings.
func (h *RawPassthroughHandle) OvertemperatureProtected() bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.lastStatusAt.IsZero() && time.Since(h.lastStatusAt) <= RecentContactWindow
}
