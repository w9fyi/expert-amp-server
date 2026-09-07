package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/FtlC-ian/expert-amp-server/internal/api"
	"github.com/FtlC-ian/expert-amp-server/internal/fanpolicy"
	"github.com/FtlC-ian/expert-amp-server/internal/monitoring"
	"github.com/FtlC-ian/expert-amp-server/internal/protocol"
	"github.com/FtlC-ian/expert-amp-server/internal/serial"
	"github.com/FtlC-ian/expert-amp-server/internal/transport"
)

func newPassthroughTestSource(t *testing.T, opener serial.PortOpener) *SerialSource {
	t.Helper()
	return newPassthroughTestSourceWithPolling(t, opener, "both", true)
}

func newPassthroughTestSourceWithPolling(t *testing.T, opener serial.PortOpener, mode string, statusPoll bool) *SerialSource {
	t.Helper()
	return NewSerialSource(SerialSourceConfig{
		Port:                     "/dev/ttyTEST0",
		BaudRate:                 115200,
		ReadTimeout:              10 * time.Millisecond,
		ReadSize:                 512,
		MinFrameLen:              64,
		MaxBuffer:                8192,
		IOTimeout:                2 * time.Second,
		ReconnectBackoff:         100 * time.Millisecond,
		PollingMode:              mode,
		StatusPollCommandEnabled: statusPoll,
	}, opener, Update{})
}

// The live read loop must be fully stopped, not merely told to stop, before the
// caller is handed the port. Mirrors the wake test's guarantee.
func TestBeginRawPassthroughQuiescesLiveSessionAndReconnectsOnClose(t *testing.T) {
	live := &mockSerialPort{blockRead: true}
	leased := &mockSerialPort{blockRead: true}
	reconnected := &mockSerialPort{chunks: [][]byte{displayStreamChunk(t)}}
	opener := &sequenceSerialOpener{
		ports:  []serial.Port{live, leased, reconnected},
		opened: make(chan int, 3),
	}
	src := newPassthroughTestSource(t, opener)

	ctx, cancel := context.WithCancel(context.Background())
	src.Start(ctx)
	defer cancel()
	waitForCondition(t, time.Second, func() bool { return opener.openCount() == 1 })

	beginCtx, beginCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer beginCancel()
	handle, err := src.BeginRawPassthrough(beginCtx)
	if err != nil {
		t.Fatalf("BeginRawPassthrough error: %v", err)
	}
	if !live.isClosed() {
		t.Fatal("passthrough did not retire the live reader before leasing the port")
	}
	if handle.Port() != serial.Port(leased) {
		t.Fatal("passthrough did not return the newly opened port")
	}

	handle.Close()

	waitForCondition(t, 2*time.Second, func() bool {
		diag := src.Diagnostics()
		return opener.openCount() >= 3 && diag.Connected
	})
	if !leased.isClosed() {
		t.Fatal("closing the handle did not close the leased port")
	}

	// Close is idempotent: a second call must not unlock lifecycleMu twice.
	handle.Close()
}

// Regression test for the deadlock this feature's first design would have
// caused. SendWake takes writeMu and then lifecycleMu, and it is called with
// the actuation coordinator's mutex held, so blocking it on the lifecycleMu a
// passthrough session holds for its whole duration would wedge every actuation
// path until the external client disconnected. Both calls must fail fast.
func TestRawPassthroughRefusesServerWritesWithoutBlocking(t *testing.T) {
	live := &mockSerialPort{blockRead: true}
	leased := &mockSerialPort{blockRead: true}
	opener := &sequenceSerialOpener{
		ports:  []serial.Port{live, leased, &mockSerialPort{blockRead: true}},
		opened: make(chan int, 3),
	}
	src := newPassthroughTestSource(t, opener)

	ctx, cancel := context.WithCancel(context.Background())
	src.Start(ctx)
	defer cancel()
	waitForCondition(t, time.Second, func() bool { return opener.openCount() == 1 })

	handle, err := src.BeginRawPassthrough(context.Background())
	if err != nil {
		t.Fatalf("BeginRawPassthrough error: %v", err)
	}
	defer handle.Close()

	type outcome struct {
		err error
	}

	wakeDone := make(chan outcome, 1)
	go func() {
		_, err := src.SendWake(context.Background())
		wakeDone <- outcome{err: err}
	}()

	buttonDone := make(chan outcome, 1)
	go func() {
		_, err := src.SendButton(context.Background(), api.ButtonAction{Name: "operate"})
		buttonDone <- outcome{err: err}
	}()

	for _, tc := range []struct {
		name string
		ch   chan outcome
	}{
		{"SendWake", wakeDone},
		{"SendButton", buttonDone},
	} {
		select {
		case got := <-tc.ch:
			if got.err == nil {
				t.Fatalf("%s: expected refusal while passthrough holds the port", tc.name)
			}
			if transport.ButtonStatusCode(got.err) != 409 {
				t.Fatalf("%s: expected HTTP 409, got %d (%v)", tc.name, transport.ButtonStatusCode(got.err), got.err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s blocked while a raw passthrough session held the port", tc.name)
		}
	}
}

// A second concurrent client is rejected outright rather than queued, matching
// the actuation coordinator's reject-don't-queue rule.
func TestBeginRawPassthroughRejectsSecondClient(t *testing.T) {
	opener := &sequenceSerialOpener{
		ports:  []serial.Port{&mockSerialPort{blockRead: true}, &mockSerialPort{blockRead: true}, &mockSerialPort{blockRead: true}},
		opened: make(chan int, 3),
	}
	src := newPassthroughTestSource(t, opener)

	ctx, cancel := context.WithCancel(context.Background())
	src.Start(ctx)
	defer cancel()
	waitForCondition(t, time.Second, func() bool { return opener.openCount() == 1 })

	first, err := src.BeginRawPassthrough(context.Background())
	if err != nil {
		t.Fatalf("first BeginRawPassthrough error: %v", err)
	}
	defer first.Close()

	second, err := src.BeginRawPassthrough(context.Background())
	if !errors.Is(err, ErrRawPassthroughBusy) {
		t.Fatalf("expected ErrRawPassthroughBusy, got %v", err)
	}
	if second != nil {
		t.Fatal("expected no handle for the rejected second client")
	}
}

// A tapped frame is display evidence only. It must be labelled passthrough-tap,
// must never reach the safety controller, and must never end the session -- the
// operator keeps their live control link, and server-side automatic control was
// already refused before the lease started.
func TestRawPassthroughTapIsDisplayOnlyAndNeverActuates(t *testing.T) {
	opener := &sequenceSerialOpener{
		ports:  []serial.Port{&mockSerialPort{blockRead: true}, &mockSerialPort{blockRead: true}, &mockSerialPort{blockRead: true}},
		opened: make(chan int, 3),
	}
	src := newPassthroughTestSource(t, opener)

	safety := monitoring.NewController(nil)
	src.ConfigureSafetyController(safety, func() monitoring.ControlSettings {
		return monitoring.ControlSettings{
			Enabled:    true,
			Armed:      true,
			Thresholds: monitoring.Thresholds{TemperatureTripC: 50, TemperatureWarningC: 40, TemperatureResetC: 40},
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	src.Start(ctx)
	defer cancel()
	waitForCondition(t, time.Second, func() bool { return opener.openCount() == 1 })

	handle, err := src.BeginRawPassthrough(context.Background())
	if err != nil {
		t.Fatalf("BeginRawPassthrough error: %v", err)
	}
	defer handle.Close()

	// Well past the 50C trip threshold: under the old tap-and-drop behavior
	// this ended the session.
	handle.ObserveFromAmp(statusFrameAtTemperature(t, 80))

	if safety.Apply(monitoring.Result{}, true).Latched {
		t.Fatal("tapped frames must not reach the safety controller")
	}

	tapped := src.statusState.CurrentProtocolNative()
	if tapped.Provenance != ProvenancePassthroughTap {
		t.Fatalf("tapped status provenance = %q, want %q", tapped.Provenance, ProvenancePassthroughTap)
	}
	if !handle.TapIsFresh() {
		t.Fatal("tap should report fresh after observing a status frame")
	}

	// The session is still the operator's; nothing ended it.
	if _, err := src.BeginRawPassthrough(context.Background()); !errors.Is(err, ErrRawPassthroughBusy) {
		t.Fatalf("session should still be held after a hot tapped frame, got %v", err)
	}
}

// The boundary this feature turns on: tapped status is visible to the display
// path but is refused by every automatic actuation gate. Both gates test the
// provenance string exactly, so this pins the string as much as the behavior.
func TestPassthroughTapVisibleForDisplayRejectedForActuation(t *testing.T) {
	hot := 80.0
	tapped := api.Status{
		Telemetry: api.Telemetry{
			Provenance:     ProvenancePassthroughTap,
			OperatingState: "operate",
			TX:             new(bool),
			TemperatureC:   &hot,
			Source:         "serial",
		},
		RecentContact: true,
	}

	// Rejected as authority: overtemperature standby must not latch on it.
	safety := monitoring.NewController(nil)
	safety.Observe(context.Background(), tapped, monitoring.ControlSettings{
		Enabled:    true,
		Armed:      true,
		Thresholds: monitoring.Thresholds{TemperatureTripC: 50, TemperatureWarningC: 40, TemperatureResetC: 40},
	})
	if safety.Apply(monitoring.Result{}, true).Latched {
		t.Fatal("overtemperature standby latched on passthrough-tap status")
	}

	// Rejected as authority: fan policy must report it as unavailable.
	fanResult := fanpolicy.Evaluate(tapped, fanpolicy.Settings{
		Enabled:            true,
		HighTemperatureC:   60,
		NormalTemperatureC: 40,
	}, fanpolicy.PolicyUnknown)
	if fanResult.State != fanpolicy.StateUnavailable {
		t.Fatalf("fan policy state = %q, want %q for passthrough-tap status", fanResult.State, fanpolicy.StateUnavailable)
	}

	// Visible for display: Resolve merges it and keeps the label attached, so a
	// consumer can always tell what it is looking at.
	state := NewStatusState(api.Status{})
	state.UpdateProtocolNative(tapped)
	resolved := state.Resolve(Snapshot{})
	if resolved.Provenance != ProvenancePassthroughTap {
		t.Fatalf("resolved provenance = %q, want the tap label to survive the merge", resolved.Provenance)
	}
	if resolved.TemperatureC == nil || *resolved.TemperatureC != hot {
		t.Fatalf("tapped temperature was not merged for display: %+v", resolved.TemperatureC)
	}
}

// statusFrameAtTemperature rewrites the documented status fixture to report
// temp on all three sensors in OPERATE/RX, keeping every field width identical
// so only the trailing checksum needs recomputing.
func statusFrameAtTemperature(t *testing.T, temp int) []byte {
	t.Helper()
	frame, err := os.ReadFile("../protocol/testdata/status_response_example.bin")
	if err != nil {
		t.Fatalf("read status fixture: %v", err)
	}
	if len(frame) != 75 {
		t.Fatalf("unexpected status fixture length %d", len(frame))
	}
	// Data is frame[4:71] (statusDataLen 67) with the checksum pair right after.
	const dataStart, dataEnd = 4, 71

	fields := strings.Split(string(frame[dataStart:dataEnd]), ",")
	if len(fields) != 19 {
		t.Fatalf("unexpected status fixture field count %d", len(fields))
	}
	fields[1] = "O" // OPERATE
	fields[2] = "R" // RX
	value := fmt.Sprintf("%03d", temp)
	if len(value) != 3 {
		t.Fatalf("temperature %d does not fit the fixture's 3-character field", temp)
	}
	fields[14], fields[15], fields[16] = value, value, value

	data := strings.Join(fields, ",")
	if len(data) != dataEnd-dataStart {
		t.Fatalf("rewritten status data changed length: got %d want %d", len(data), dataEnd-dataStart)
	}

	out := append([]byte(nil), frame...)
	copy(out[dataStart:dataEnd], data)
	sum := 0
	for _, b := range out[dataStart:dataEnd] {
		sum += int(b)
	}
	out[dataEnd] = byte(sum % 256)
	out[dataEnd+1] = byte(sum / 256)

	if _, err := protocol.ParseStatusFrame(out); err != nil {
		t.Fatalf("synthesized status frame is invalid: %v", err)
	}
	return out
}

// Tapped telemetry must go stale on its own when the client stops polling. The
// guarantee cannot depend on any particular client's goodwill about how often
// it asks for status.
func TestRawPassthroughTapGoesStaleWhenClientStopsPolling(t *testing.T) {
	opener := &sequenceSerialOpener{
		ports:  []serial.Port{&mockSerialPort{blockRead: true}, &mockSerialPort{blockRead: true}, &mockSerialPort{blockRead: true}},
		opened: make(chan int, 3),
	}
	src := newPassthroughTestSource(t, opener)

	ctx, cancel := context.WithCancel(context.Background())
	src.Start(ctx)
	defer cancel()
	waitForCondition(t, time.Second, func() bool { return opener.openCount() == 1 })

	handle, err := src.BeginRawPassthrough(context.Background())
	if err != nil {
		t.Fatalf("BeginRawPassthrough error: %v", err)
	}
	defer handle.Close()

	if handle.TapIsFresh() {
		t.Fatal("tap must not report fresh before any status frame is observed")
	}

	handle.ObserveFromAmp(statusFrameAtTemperature(t, 40))
	if !handle.TapIsFresh() {
		t.Fatal("tap should report fresh immediately after a status frame")
	}

	// Age the last observation past the contact window without waiting it out.
	handle.mu.Lock()
	handle.lastStatusAt = time.Now().Add(-2 * RecentContactWindow)
	handle.mu.Unlock()

	if handle.TapIsFresh() {
		t.Fatal("tap must go stale once the client stops supplying status frames")
	}
}

// A rejected second client must not disturb the first client's actuation lease.
// Before v0.4.8 Acquire was re-entrant for the same owner name, so both
// sessions passed it and the loser's Release cleared the winner's ownership.
// The coordinator now refuses the second acquisition outright and scopes
// Release to the exact lease instance; this asserts both halves.
func TestRawPassthroughRejectedClientKeepsFirstLease(t *testing.T) {
	opener := &sequenceSerialOpener{
		ports:  []serial.Port{&mockSerialPort{blockRead: true}, &mockSerialPort{blockRead: true}, &mockSerialPort{blockRead: true}},
		opened: make(chan int, 3),
	}
	src := newPassthroughTestSource(t, opener)
	ctx, cancel := context.WithCancel(context.Background())
	src.Start(ctx)
	defer cancel()
	waitForCondition(t, time.Second, func() bool { return opener.openCount() == 1 })

	coordinator := transport.NewActuationCoordinator(src)
	owner := coordinator.Owner(transport.ActuationOwnerRawPassthrough, false)

	firstLease := owner.Acquire()
	if firstLease == nil {
		t.Fatal("first lease acquire failed")
	}
	first, err := src.BeginRawPassthrough(context.Background())
	if err != nil {
		t.Fatalf("BeginRawPassthrough error: %v", err)
	}
	defer first.Close()

	// The same owner name must not acquire twice. If it did, the second
	// lease's Release would be free to clear the first session's ownership.
	if second := owner.Acquire(); second != nil {
		second.Release()
		t.Fatal("second acquire returned a lease while the first was still held")
	}

	// The second client is rejected by the claim, and must not touch the lease.
	if _, err := src.BeginRawPassthrough(context.Background()); !errors.Is(err, ErrRawPassthroughBusy) {
		t.Fatalf("expected ErrRawPassthroughBusy, got %v", err)
	}

	if _, err := coordinator.SendButton(context.Background(), api.ButtonAction{Name: "operate"}); transport.ButtonStatusCode(err) != 409 {
		t.Fatalf("first client's lease was lost: expected 409 from the coordinator, got %v", err)
	}
}
