// party-stream – Hue Entertainment streaming for Darren's Party Mode (2026-10-12).
//
// The house's Party ear (the hall tablet, www/house/party-ear.js) hears the music and fires
// Home Assistant events; this program turns each one into a flash on the Hue lights of an
// Entertainment area, streamed straight to the bridge over DTLS at ~40 frames a second – so
// the lights pulse with the kick and go dark between beats, instead of waiting on the Hue
// REST API (~250 ms and 10 commands/s).
//
//	events (type "party_stream", data.cmd):
//	  start           activate the area and begin streaming (black until the first beat)
//	  stop            fade out, stop streaming, the bridge puts the lights back
//	  beat            the next pair of the two-step chase flashes (pair_a / pair_b alternate)
//	  hit             every channel flashes one colour (an accent)
//	state: binary_sensor.party_stream (on while streaming; attributes paired/area/beats/fps)
//
// Pairing: on first run it asks the bridge for an application key WITH a client key (the
// Entertainment PSK) – press the bridge's link button within five minutes. Keys are kept in
// <data>/hue.json (the add-on's own config folder, /addon_configs/<slug>/hue.json; never in git).
//
// Runs as a Home Assistant add-on (options from /data/options.json, HA API through the
// Supervisor) or on a Mac for testing: -options opts.json -data ./data -ha-url -ha-token.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/dtls/v2"
)

// ── configuration ──────────────────────────────────────────────────────────

type Options struct {
	Bridge    string   `json:"bridge"`
	Area      string   `json:"area"`
	FPS       int      `json:"fps"`
	PairA     []string `json:"pair_a"`
	PairB     []string `json:"pair_b"`
	Followers []string `json:"followers"`
	HoldMs    int      `json:"hold_ms"`
	DecayMs   int      `json:"decay_ms"`
	// every Nth beat the followers flash too (0 = only on hits)
	FollowEvery int `json:"follow_every"`
	// beats per pair of colours
	Phrase int `json:"phrase"`
}

type Keys struct {
	Username  string `json:"username"`
	ClientKey string `json:"clientkey"`
	AppID     string `json:"application_id"`
}

func loadOptions(path string) Options {
	o := Options{Bridge: "192.168.1.14", Area: "Music area", FPS: 40, HoldMs: 60, DecayMs: 260, FollowEvery: 4, Phrase: 8}
	if b, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(b, &o); err != nil {
			log.Printf("options: %v (using defaults)", err)
		}
	} else {
		log.Printf("options: %v (using defaults)", err)
	}
	if o.FPS < 10 {
		o.FPS = 10
	}
	if o.FPS > 50 {
		o.FPS = 50
	}
	if o.Phrase < 1 {
		o.Phrase = 8
	}
	return o
}

// ── Hue bridge: pairing + CLIP v2 ──────────────────────────────────────────

type Bridge struct {
	host string
	keys Keys
	http *http.Client
}

func newBridge(host string, keys Keys) *Bridge {
	return &Bridge{host: host, keys: keys, http: &http.Client{
		Timeout:   8 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, // the bridge's own cert
	}}
}

func (b *Bridge) do(method, path string, body any) ([]byte, http.Header, error) {
	var rd io.Reader
	if body != nil {
		j, _ := json.Marshal(body)
		rd = bytes.NewReader(j)
	}
	req, err := http.NewRequest(method, "https://"+b.host+path, rd)
	if err != nil {
		return nil, nil, err
	}
	if b.keys.Username != "" {
		req.Header.Set("hue-application-key", b.keys.Username)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := b.http.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer res.Body.Close()
	out, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, nil, err
	}
	if res.StatusCode >= 300 {
		return out, res.Header, fmt.Errorf("%s %s: HTTP %d %s", method, path, res.StatusCode, strings.TrimSpace(string(out)))
	}
	return out, res.Header, nil
}

// pair asks for an application key + client key; the link button has to be pressed.
func (b *Bridge) pair(ctx context.Context, notify func(string)) error {
	notify("Press the link button on the Hue bridge – Party stream is waiting to pair (5 minutes).")
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		out, _, err := b.do("POST", "/api", map[string]any{"devicetype": "party_stream#green", "generateclientkey": true})
		if err == nil {
			var res []struct {
				Success *Keys `json:"success"`
				Error   *struct {
					Type        int    `json:"type"`
					Description string `json:"description"`
				} `json:"error"`
			}
			if json.Unmarshal(out, &res) == nil && len(res) > 0 {
				if res[0].Success != nil && res[0].Success.Username != "" {
					b.keys = *res[0].Success
					notify("Paired with the Hue bridge – Party stream is ready.")
					return nil
				}
				if res[0].Error != nil && res[0].Error.Type != 101 {
					return fmt.Errorf("bridge: %s", res[0].Error.Description)
				}
			}
		} else {
			log.Printf("pair: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
	return errors.New("link button not pressed within 5 minutes")
}

// applicationID is the DTLS PSK identity for Entertainment v2 (the hue-application-id header).
func (b *Bridge) applicationID() (string, error) {
	_, hdr, err := b.do("GET", "/auth/v1", nil)
	if err != nil && hdr == nil {
		return "", err
	}
	if id := hdr.Get("hue-application-id"); id != "" {
		return id, nil
	}
	return "", errors.New("no hue-application-id header")
}

type clipList struct {
	Data []json.RawMessage `json:"data"`
}

func (b *Bridge) list(resource string) ([]json.RawMessage, error) {
	out, _, err := b.do("GET", "/clip/v2/resource/"+resource, nil)
	if err != nil {
		return nil, err
	}
	var l clipList
	if err := json.Unmarshal(out, &l); err != nil {
		return nil, err
	}
	return l.Data, nil
}

type Area struct {
	ID       string
	Name     string
	Status   string
	Channels []Channel
}

type Channel struct {
	ID    int
	Names []string // device names of its members
}

// area resolves the Entertainment area by name and maps its channels to device names.
func (b *Bridge) area(name string) (*Area, error) {
	devs := map[string]string{}
	if raw, err := b.list("device"); err == nil {
		for _, r := range raw {
			var d struct {
				ID       string `json:"id"`
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
			}
			if json.Unmarshal(r, &d) == nil {
				devs[d.ID] = d.Metadata.Name
			}
		}
	} else {
		return nil, err
	}
	ents := map[string]string{}
	raw, err := b.list("entertainment")
	if err != nil {
		return nil, err
	}
	for _, r := range raw {
		var e struct {
			ID    string `json:"id"`
			Owner struct {
				RID string `json:"rid"`
			} `json:"owner"`
		}
		if json.Unmarshal(r, &e) == nil {
			ents[e.ID] = devs[e.Owner.RID]
		}
	}
	raw, err = b.list("entertainment_configuration")
	if err != nil {
		return nil, err
	}
	for _, r := range raw {
		var c struct {
			ID       string `json:"id"`
			Status   string `json:"status"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Channels []struct {
				ChannelID int `json:"channel_id"`
				Members   []struct {
					Service struct {
						RID string `json:"rid"`
					} `json:"service"`
				} `json:"members"`
			} `json:"channels"`
		}
		if json.Unmarshal(r, &c) != nil || !strings.EqualFold(c.Metadata.Name, name) {
			continue
		}
		a := &Area{ID: c.ID, Name: c.Metadata.Name, Status: c.Status}
		for _, ch := range c.Channels {
			cc := Channel{ID: ch.ChannelID}
			for _, m := range ch.Members {
				if n := ents[m.Service.RID]; n != "" && !contains(cc.Names, n) {
					cc.Names = append(cc.Names, n)
				}
			}
			a.Channels = append(a.Channels, cc)
		}
		return a, nil
	}
	return nil, fmt.Errorf("no Entertainment area called %q on the bridge", name)
}

func (b *Bridge) setStreaming(areaID string, on bool) error {
	action := "stop"
	if on {
		action = "start"
	}
	_, _, err := b.do("PUT", "/clip/v2/resource/entertainment_configuration/"+areaID, map[string]any{"action": action})
	return err
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if strings.EqualFold(x, s) {
			return true
		}
	}
	return false
}

// ── the stream: DTLS + HueStream v2 frames + the flash renderer ────────────

type chanState struct {
	colour [3]float64 // 0..1 linear-ish
	level  float64    // flash envelope 0..1
	holdTo time.Time
}

type Streamer struct {
	opts   Options
	bridge *Bridge
	area   *Area
	conn   *dtls.Conn

	mu      sync.Mutex
	state   map[int]*chanState // by channel id
	pairA   []int
	pairB   []int
	follow  []int
	seq     uint8
	beats   int
	hits    int
	turnA   bool
	colours [2][3]float64
	started time.Time
	running bool
	stopCh  chan struct{}
}

var palette = [][3]float64{{1, 0, 0}, {1, 0.35, 0}, {1, 0.82, 0}, {0, 1, 0.24}, {0, 0.78, 1}, {0, 0.24, 1}, {0.67, 0, 1}, {1, 0, 0.67}}

func pickColour(not [3]float64) [3]float64 {
	for {
		c := palette[rand.Intn(len(palette))]
		if c != not {
			return c
		}
	}
}

func newStreamer(opts Options, b *Bridge, a *Area) *Streamer {
	s := &Streamer{opts: opts, bridge: b, area: a, state: map[int]*chanState{}}
	for _, ch := range a.Channels {
		s.state[ch.ID] = &chanState{}
		for _, n := range ch.Names {
			if contains(opts.PairA, n) {
				s.pairA = appendUnique(s.pairA, ch.ID)
			}
			if contains(opts.PairB, n) {
				s.pairB = appendUnique(s.pairB, ch.ID)
			}
			if contains(opts.Followers, n) {
				s.follow = appendUnique(s.follow, ch.ID)
			}
		}
	}
	s.colours = [2][3]float64{pickColour([3]float64{}), {}}
	s.colours[1] = pickColour(s.colours[0])
	return s
}

func appendUnique(l []int, v int) []int {
	for _, x := range l {
		if x == v {
			return l
		}
	}
	return append(l, v)
}

// connect activates the area and opens the DTLS session (PSK = the client key).
func (s *Streamer) connect(ctx context.Context) error {
	if err := s.bridge.setStreaming(s.area.ID, true); err != nil {
		return fmt.Errorf("start streaming: %w", err)
	}
	key, err := hex.DecodeString(s.bridge.keys.ClientKey)
	if err != nil {
		return fmt.Errorf("clientkey: %w", err)
	}
	addr := &net.UDPAddr{IP: net.ParseIP(s.bridge.host), Port: 2100}
	if addr.IP == nil {
		ips, err := net.LookupIP(s.bridge.host)
		if err != nil || len(ips) == 0 {
			return fmt.Errorf("resolve %s: %v", s.bridge.host, err)
		}
		addr.IP = ips[0]
	}
	// Entertainment v2 wants the application id as the PSK identity; older bridges the username.
	identities := []string{s.bridge.keys.AppID, s.bridge.keys.Username}
	var lastErr error
	for _, ident := range identities {
		if ident == "" {
			continue
		}
		cfg := &dtls.Config{
			PSK:                  func(hint []byte) ([]byte, error) { return key, nil },
			PSKIdentityHint:      []byte(ident),
			CipherSuites:         []dtls.CipherSuiteID{dtls.TLS_PSK_WITH_AES_128_GCM_SHA256},
			ExtendedMasterSecret: dtls.DisableExtendedMasterSecret,
		}
		dctx, cancel := context.WithTimeout(ctx, 6*time.Second)
		conn, err := dtls.DialWithContext(dctx, "udp", addr, cfg)
		cancel()
		if err == nil {
			s.conn = conn
			log.Printf("stream: DTLS up to %s (identity %s…)", addr, ident[:min(8, len(ident))])
			return nil
		}
		lastErr = err
		log.Printf("stream: DTLS with identity %s… failed: %v", ident[:min(8, len(ident))], err)
	}
	_ = s.bridge.setStreaming(s.area.ID, false)
	return fmt.Errorf("DTLS handshake: %w", lastErr)
}

// frame builds one HueStream 2.0 message: header, area id, then id + RGB16 per channel.
func (s *Streamer) frame() []byte {
	buf := make([]byte, 0, 16+36+len(s.area.Channels)*7)
	buf = append(buf, []byte("HueStream")...)
	buf = append(buf, 0x02, 0x00) // version 2.0
	buf = append(buf, s.seq)
	buf = append(buf, 0x00, 0x00) // reserved
	buf = append(buf, 0x00)       // colour space RGB
	buf = append(buf, 0x00)       // reserved
	buf = append(buf, []byte(s.area.ID)...)
	s.seq++
	for _, ch := range s.area.Channels {
		st := s.state[ch.ID]
		lvl := math.Pow(st.level, 1.6) // a sharper pulse than linear
		buf = append(buf, byte(ch.ID))
		for i := 0; i < 3; i++ {
			v := uint16(math.Round(st.colour[i] * lvl * 65535))
			buf = append(buf, byte(v>>8), byte(v))
		}
	}
	return buf
}

func (s *Streamer) run(ctx context.Context, status func()) {
	dt := time.Second / time.Duration(s.opts.FPS)
	tau := float64(s.opts.DecayMs) / 1000 / math.Log(20) // level 1 → 0.05 in decay_ms
	decay := math.Exp(-dt.Seconds() / tau)
	tick := time.NewTicker(dt)
	defer tick.Stop()
	s.started = time.Now()
	s.running = true
	n := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case now := <-tick.C:
			s.mu.Lock()
			for _, st := range s.state {
				if now.After(st.holdTo) {
					st.level *= decay
					if st.level < 0.004 {
						st.level = 0
					}
				}
			}
			f := s.frame()
			s.mu.Unlock()
			if _, err := s.conn.Write(f); err != nil {
				log.Printf("stream: write failed: %v", err)
				return
			}
			n++
			if n%(s.opts.FPS*5) == 0 {
				status()
			}
		}
	}
}

func (s *Streamer) flash(ids []int, colour [3]float64, hold time.Duration) {
	until := time.Now().Add(hold)
	for _, id := range ids {
		if st := s.state[id]; st != nil {
			st.colour = colour
			st.level = 1
			st.holdTo = until
		}
	}
}

// beat: the two-step chase – pair_a and pair_b alternate, one colour each for a phrase.
func (s *Streamer) beat() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beats++
	if s.beats%s.opts.Phrase == 1 {
		s.colours[0] = pickColour(s.colours[0])
		s.colours[1] = pickColour(s.colours[0])
	}
	hold := time.Duration(s.opts.HoldMs) * time.Millisecond
	s.turnA = !s.turnA
	if s.turnA {
		s.flash(s.pairA, s.colours[0], hold)
	} else {
		s.flash(s.pairB, s.colours[1], hold)
	}
	if s.opts.FollowEvery > 0 && s.beats%s.opts.FollowEvery == 0 {
		c := s.colours[1]
		if s.turnA {
			c = s.colours[0]
		}
		s.flash(s.follow, c, hold)
	}
}

// hit: everything, one colour, held a little longer.
func (s *Streamer) hit() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hits++
	c := pickColour(s.colours[0])
	s.colours = [2][3]float64{c, pickColour(c)}
	all := make([]int, 0, len(s.state))
	for id := range s.state {
		all = append(all, id)
	}
	s.flash(all, c, 2*time.Duration(s.opts.HoldMs)*time.Millisecond)
}

func (s *Streamer) close() {
	s.running = false
	if s.conn != nil {
		// a few black frames so nothing is left mid-flash, then the bridge restores the lights
		s.mu.Lock()
		for _, st := range s.state {
			st.level = 0
		}
		for i := 0; i < 3; i++ {
			_, _ = s.conn.Write(s.frame())
			time.Sleep(25 * time.Millisecond)
		}
		s.mu.Unlock()
		_ = s.conn.Close()
		s.conn = nil
	}
	if err := s.bridge.setStreaming(s.area.ID, false); err != nil {
		log.Printf("stream: stop: %v", err)
	}
}

// ── Home Assistant: events in, a binary_sensor out ─────────────────────────

type HA struct {
	wsURL, restURL, token string
}

func (h *HA) setState(on bool, attrs map[string]any) {
	state := "off"
	if on {
		state = "on"
	}
	attrs["friendly_name"] = "Party stream"
	attrs["icon"] = "mdi:led-strip-variant"
	body, _ := json.Marshal(map[string]any{"state": state, "attributes": attrs})
	req, _ := http.NewRequest("POST", h.restURL+"/api/states/binary_sensor.party_stream", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+h.token)
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("ha: state: %v", err)
		return
	}
	res.Body.Close()
}

func (h *HA) notify(msg string) {
	body, _ := json.Marshal(map[string]any{"notification_id": "party_stream", "title": "Party stream", "message": msg})
	req, _ := http.NewRequest("POST", h.restURL+"/api/services/persistent_notification/create", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+h.token)
	req.Header.Set("Content-Type", "application/json")
	if res, err := http.DefaultClient.Do(req); err == nil {
		res.Body.Close()
	}
	log.Print(msg)
}

type wsMsg struct {
	ID    int             `json:"id,omitempty"`
	Type  string          `json:"type"`
	Event json.RawMessage `json:"event,omitempty"`
}

type eventData struct {
	EventType string `json:"event_type"`
	Data      struct {
		Cmd      string `json:"cmd"`
		EntityID string `json:"entity_id"`
		NewState *struct {
			State string `json:"state"`
		} `json:"new_state"`
	} `json:"data"`
}

// listen subscribes to party_stream events (+ show_room clearing as a safety net) and hands
// them to handle; it reconnects until ctx ends.
func (h *HA) listen(ctx context.Context, handle func(cmd string)) {
	for ctx.Err() == nil {
		if err := h.listenOnce(ctx, handle); err != nil && ctx.Err() == nil {
			log.Printf("ha: websocket: %v (retrying in 5 s)", err)
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
			}
		}
	}
}

func (h *HA) listenOnce(ctx context.Context, handle func(cmd string)) error {
	c, _, err := websocket.DefaultDialer.DialContext(ctx, h.wsURL, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	// auth_required → auth → auth_ok
	if _, _, err := c.ReadMessage(); err != nil {
		return err
	}
	if err := c.WriteJSON(map[string]string{"type": "auth", "access_token": h.token}); err != nil {
		return err
	}
	var ok wsMsg
	if err := c.ReadJSON(&ok); err != nil || ok.Type != "auth_ok" {
		return fmt.Errorf("auth failed: %s", ok.Type)
	}
	if err := c.WriteJSON(map[string]any{"id": 1, "type": "subscribe_events", "event_type": "party_stream"}); err != nil {
		return err
	}
	if err := c.WriteJSON(map[string]any{"id": 2, "type": "subscribe_trigger", "trigger": map[string]any{
		"platform": "state", "entity_id": "input_text.show_room", "to": ""}}); err != nil {
		return err
	}
	log.Print("ha: listening for party_stream events")
	go func() {
		<-ctx.Done()
		c.Close()
	}()
	for {
		var m wsMsg
		if err := c.ReadJSON(&m); err != nil {
			return err
		}
		if m.Type != "event" {
			continue
		}
		if m.ID == 2 {
			handle("stop")
			continue
		}
		var ev eventData
		if json.Unmarshal(m.Event, &ev) == nil && ev.Data.Cmd != "" {
			handle(ev.Data.Cmd)
		}
	}
}

// ── main ───────────────────────────────────────────────────────────────────

func main() {
	optPath := flag.String("options", "/data/options.json", "options file")
	dataDir := flag.String("data", "/config", "where hue.json lives (the add-on's own config folder, /addon_configs/<slug> on the host)")
	haURL := flag.String("ha-url", "", "HA base URL (default: the Supervisor proxy)")
	haToken := flag.String("ha-token", "", "HA token (default: SUPERVISOR_TOKEN)")
	demo := flag.Bool("demo", false, "no HA: pair, stream a 20 s demo pattern, exit")
	flag.Parse()
	log.SetFlags(log.Ltime)

	opts := loadOptions(*optPath)
	ha := &HA{token: *haToken}
	if ha.token == "" {
		ha.token = os.Getenv("SUPERVISOR_TOKEN")
	}
	if *haURL != "" {
		ha.restURL = strings.TrimRight(*haURL, "/")
		ha.wsURL = strings.Replace(ha.restURL, "http", "ws", 1) + "/api/websocket"
	} else {
		ha.restURL = "http://supervisor/core"
		ha.wsURL = "ws://supervisor/core/websocket"
	}
	notify := func(msg string) {
		if ha.token != "" && !*demo {
			ha.notify(msg)
		} else {
			log.Print(msg)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// keys
	keysPath := filepath.Join(*dataDir, "hue.json")
	var keys Keys
	if b, err := os.ReadFile(keysPath); err == nil {
		_ = json.Unmarshal(b, &keys)
	}
	bridge := newBridge(opts.Bridge, keys)
	if bridge.keys.ClientKey == "" {
		if err := bridge.pair(ctx, notify); err != nil {
			log.Fatalf("pairing: %v", err)
		}
		if id, err := bridge.applicationID(); err == nil {
			bridge.keys.AppID = id
		} else {
			log.Printf("application id: %v", err)
		}
		b, _ := json.MarshalIndent(bridge.keys, "", "  ")
		_ = os.MkdirAll(*dataDir, 0o755)
		if err := os.WriteFile(keysPath, b, 0o600); err != nil {
			log.Fatalf("saving keys: %v", err)
		}
	} else if bridge.keys.AppID == "" {
		if id, err := bridge.applicationID(); err == nil {
			bridge.keys.AppID = id
			b, _ := json.MarshalIndent(bridge.keys, "", "  ")
			_ = os.WriteFile(keysPath, b, 0o600)
		}
	}

	area, err := bridge.area(opts.Area)
	if err != nil {
		log.Fatalf("area: %v", err)
	}
	log.Printf("area %q (%s): %d channels, status %s", area.Name, area.ID, len(area.Channels), area.Status)
	for _, ch := range area.Channels {
		log.Printf("  channel %d: %s", ch.ID, strings.Join(ch.Names, " + "))
	}

	var (
		mu sync.Mutex
		st *Streamer
	)
	status := func() {
		mu.Lock()
		defer mu.Unlock()
		attrs := map[string]any{"paired": true, "area": area.Name, "channels": len(area.Channels), "fps": opts.FPS}
		on := st != nil && st.running
		if on {
			attrs["beats"] = st.beats
			attrs["hits"] = st.hits
			attrs["since"] = st.started.Format(time.RFC3339)
		}
		if ha.token != "" && !*demo {
			ha.setState(on, attrs)
		}
	}
	start := func() {
		mu.Lock()
		if st != nil && st.running {
			mu.Unlock()
			return
		}
		s := newStreamer(opts, bridge, area)
		log.Printf("stream: pair_a %v pair_b %v followers %v", s.pairA, s.pairB, s.follow)
		if err := s.connect(ctx); err != nil {
			mu.Unlock()
			log.Printf("stream: %v", err)
			notify("Party stream couldn't start: " + err.Error())
			return
		}
		s.stopCh = make(chan struct{})
		st = s
		mu.Unlock() // status() takes the lock itself
		go func() {
			s.run(ctx, status)
			mu.Lock()
			if st == s {
				s.close()
				st = nil
			}
			mu.Unlock()
			status()
		}()
		status()
	}
	stop := func() {
		mu.Lock()
		s := st
		mu.Unlock()
		if s != nil && s.running {
			close(s.stopCh)
			for i := 0; i < 40 && s.running; i++ {
				time.Sleep(25 * time.Millisecond)
			}
			mu.Lock()
			if st == s {
				s.close()
				st = nil
			}
			mu.Unlock()
			log.Printf("stream: stopped after %d beats, %d hits", s.beats, s.hits)
		}
		status()
	}
	handle := func(cmd string) {
		switch cmd {
		case "start":
			start()
		case "stop":
			stop()
		case "beat", "hit":
			mu.Lock()
			s := st
			mu.Unlock()
			if s == nil || !s.running {
				return
			}
			if cmd == "hit" {
				s.hit()
			} else {
				s.beat()
			}
		}
	}

	if *demo {
		start()
		if st == nil {
			os.Exit(1)
		}
		log.Print("demo: 20 s at 128 bpm, a hit every 16")
		for i := 0; i < 42; i++ {
			if i%16 == 15 {
				handle("hit")
			} else {
				handle("beat")
			}
			time.Sleep(469 * time.Millisecond)
		}
		stop()
		return
	}

	status()
	ha.listen(ctx, handle)
}
