package server

import (
	"encoding/json"
	"net/http"
	"reflect"
	"time"

	"github.com/FtlC-ian/expert-amp-server/internal/api"
	"github.com/FtlC-ian/expert-amp-server/internal/runtime"
	"github.com/gorilla/websocket"
)

var statusWebsocketUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func handleStatusWebsocket(opts Options) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !allowMethodAPI(w, r, http.MethodGet) {
			return
		}

		conn, err := statusWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		_ = conn.SetReadDeadline(time.Time{})
		conn.SetPongHandler(func(string) error { return nil })

		// Subscribe before resolving the first payload, never after. Authority
		// can change between the two -- a lease invalidating the retained frame,
		// or the first poll after one restoring it -- and a transition published
		// into that gap reaches nobody, because this connection is not a
		// subscriber yet. The socket then waits for the next wake-up that may
		// never come: a static display publishes nothing, and a steady-state
		// amplifier repeats its status bytes, so it can serve the wrong
		// authority for the life of the connection.
		//
		// Subscribing first cannot lose it. The channels are buffered and
		// pushStatus overwrites rather than drops, so a transition that lands
		// before the initial Resolve stays queued and the loop immediately
		// rechecks; if that Resolve already saw the new state, sendIfChanged
		// finds nothing to send. Either order of arrival ends up correct.
		statusUpdates, unsubscribeStatus := subscribeStatus(opts)
		defer unsubscribeStatus()
		snapshotUpdates, unsubscribeSnapshots := subscribeSnapshots(opts.Store)
		defer unsubscribeSnapshots()

		status := selectedStatus(opts)
		if err := writeStatusWebsocketMessage(conn, status); err != nil {
			return
		}
		last := status

		pingTicker := time.NewTicker(30 * time.Second)
		defer pingTicker.Stop()

		sendIfChanged := func() error {
			status := selectedStatus(opts)
			if reflect.DeepEqual(status, last) {
				return nil
			}
			if err := writeStatusWebsocketMessage(conn, status); err != nil {
				return err
			}
			last = status
			return nil
		}

		for {
			select {
			case <-r.Context().Done():
				return
			case <-pingTicker.C:
				_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			case _, ok := <-statusUpdates:
				if !ok {
					statusUpdates = nil
					continue
				}
				if err := sendIfChanged(); err != nil {
					return
				}
			case _, ok := <-snapshotUpdates:
				if !ok {
					snapshotUpdates = nil
					continue
				}
				if err := sendIfChanged(); err != nil {
					return
				}
			}
		}
	}
}

func subscribeStatus(opts Options) (<-chan api.Status, func()) {
	if opts.StatusState == nil {
		return nil, func() {}
	}
	return opts.StatusState.Subscribe(2)
}

func subscribeSnapshots(store *runtime.Store) (<-chan runtime.Snapshot, func()) {
	if store == nil {
		return nil, func() {}
	}
	return store.Subscribe(2)
}

func writeStatusWebsocketMessage(conn *websocket.Conn, status api.Status) error {
	payload, err := json.Marshal(status)
	if err != nil {
		return err
	}
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return conn.WriteMessage(websocket.TextMessage, payload)
}
