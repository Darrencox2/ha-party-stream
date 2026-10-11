// party-stream – Hue Entertainment streaming for Darren's Party Mode (2026-10-12).
//
// The house's Party ear (the hall tablet, www/house/party-ear.js) hears the music and fires
// Home Assistant events; this program renders a light show from them on the Hue lights of an
// Entertainment area, streamed straight to the bridge over DTLS at ~40 frames a second – so
// the lights move with the music instead of waiting on the Hue REST API (~250 ms, 10/s).
//
//	events (type "party_stream", data.cmd … ):
//	  start / stop     activate the area and begin streaming / fade out and release it
//	  kick  e=1.6      a kick (e = how far above the recent average, ~1–4)
//	  snare e=…        a snare or clap
//	  hat   e=…        a hi-hat tick
//	  level db=-38     the room's loudness (full band, dBFS) every ~100 ms
//	  hit              an accent (the whole room)
//	  beat             the older "a kick" with no strength (= kick e=1.5)
//	state: binary_sensor.party_stream (on while streaming; attributes paired/area/kicks…)
//
// THE SHOW (mode "show") – layers mixed per frame, per light, using the bridge's positions:
//	bed     a slow colour wash drifting round the room (~30 s), dim, breathing with the
//	        loudness; a quiet passage becomes a slow glow, a build brightens and whitens it
//	kick    a white pop then colour on the ceilings, strength from the kick; the pattern
//	        rotates every pattern_every kicks: diagonals · left/right · a sweep round the
//	        room · all together
//	snare   the followers (bloom, strip) answer in the complementary colour
//	hat     tiny white sparkles on the sparkle lights (the strip)
//	drop    quiet for 1.5 s then a big kick → a white strobe burst, a new palette, all-together
//	        kicks for a while
//	hit     everything, white pop + colour
// mode "chase" is the plain two-step chase with no bed or pops (the first version).
//
// Pairing: on first run it asks the bridge for an application key WITH a client key (the
// Entertainment PSK) – press the bridge's link button within five minutes. Keys are kept in
// <data>/hue.json (the add-on's own config folder, /addon_configs/<slug>/hue.json; never in git).
//
// Runs as a Home Assistant add-on (options from /data/options.json, HA API through the
// Supervisor) or on a Mac for testing: -options opts.json -data ./keys -demo.
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
	"sort"
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
	Mode      string   `json:"mode"` // show | chase
	PairA     []string `json:"pair_a"`
	PairB     []string `json:"pair_b"`
	Followers []string `json:"followers"`
	Sparkle   []string `json:"sparkle"` // lights for the hi-hat sparkles (the strip)
	// kick flash: hold at full, then fade to dark in decay_ms
	HoldMs  int `json:"hold_ms"`
	DecayMs int `json:"decay_ms"`
	// snare flash on the followers
	SnareHoldMs  int `json:"snare_hold_ms"`
	SnareDecayMs int `json:"snare_decay_ms"`
	// the bed: level 0..1 at full loudness, and how long one drift round the colour wheel takes
	BedLevel  float64 `json:"bed_level"`
	BedSpeedS float64 `json:"bed_speed_s"`
	HatLevel  float64 `json:"hat_level"`
	// the kick pattern changes every N kicks; followers flash with every Nth kick in chase mode
	PatternEvery int  `json:"pattern_every"`
	FollowEvery  int  `json:"follow_every"`
	Phrase       int  `json:"phrase"`
	DropStrobe   bool `json:"drop_strobe"`
}

type Keys struct {
	Username  string `json:"username"`
	ClientKey string `json:"clientkey"`
	AppID     string `json:"application_id"`
}

func loadOptions(path string) Options {
	o := Options{Bridge: "192.168.1.14", Area: "Music area", FPS: 40, Mode: "show", HoldMs: 50, DecayMs: 220,
		SnareHoldMs: 40, SnareDecayMs: 180, BedLevel: 0.12, BedSpeedS: 30, HatLevel: 0.35,
		PatternEvery: 16, FollowEvery: 4, Phrase: 8, DropStrobe: true}
	if b, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(b, &o); err != nil {
			log.Printf("options: %v (using defaults)", err)
		}
	} else {
		log.Printf("options: %v (using defaults)", err)
	}
	o.FPS = clampInt(o.FPS, 10, 50)
	if o.Phrase < 1 {
		o.Phrase = 8
	}
	if o.PatternEvery < 1 {
		o.PatternEvery = 16
	}
	if o.BedSpeedS <= 0 {
		o.BedSpeedS = 30
	}
	if o.Mode != "chase" {
		o.Mode = "show"
	}
	return o
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
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
	X, Y  float64  // the bridge's position for it (room coordinates, -1..1)
}

// area resolves the Entertainment area by name and maps its channels to device names.
func (b *Bridge) area(name string) (*Area, error) {
	devs := map[string]string{}
	raw, err := b.list("device")
	if err != nil {
		return nil, err
	}
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
	ents := map[string]string{}
	raw, err = b.list("entertainment")
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
				Position  struct {
					X, Y, Z float64
				} `json:"position"`
				Members []struct {
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
			cc := Channel{ID: ch.ChannelID, X: ch.Position.X, Y: ch.Position.Y}
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

// ── colour ─────────────────────────────────────────────────────────────────

type RGB = [3]float64

var white = RGB{1, 1, 1}

func hsv(h, s, v float64) RGB {
	h = math.Mod(math.Mod(h, 360)+360, 360) / 60
	i := math.Floor(h)
	f := h - i
	p, q, t := v*(1-s), v*(1-s*f), v*(1-s*(1-f))
	switch int(i) {
	case 0:
		return RGB{v, t, p}
	case 1:
		return RGB{q, v, p}
	case 2:
		return RGB{p, v, t}
	case 3:
		return RGB{p, q, v}
	case 4:
		return RGB{t, p, v}
	default:
		return RGB{v, p, q}
	}
}

// ── the stream: DTLS + HueStream v2 frames + the layered renderer ──────────

type chanState struct {
	x, y float64
	// colour flash (kicks, snares, hits): colour, level, hold until, decay per frame
	flash      RGB
	flashLvl   float64
	flashHold  time.Time
	flashDecay float64
	// white pop (the punch at the front of a kick) and hat sparkles
	pop     float64
	popHold time.Time
	spark   float64
}

type sched struct {
	at    time.Time
	ids   []int
	col   RGB
	lvl   float64
	decay float64
	hold  time.Duration
	pop   float64
}

type Streamer struct {
	opts   Options
	bridge *Bridge
	area   *Area
	conn   *dtls.Conn

	mu      sync.Mutex
	state   map[int]*chanState
	ceiling []int // pair_a + pair_b
	pairA   []int
	pairB   []int
	follow  []int
	sparkle []int
	left    []int
	right   []int
	order   []int // sweep order round the room
	seq     uint8
	rng     *rand.Rand

	// counters + music state
	kicks, snares, hats, hits int
	turnA                     bool
	pattern                   int
	patternKicks              int
	allUntilKick              int
	baseHue                   float64
	lvl, lvlMax, lvlAvg       float64 // loudness dBFS: smoothed, recent max, slow average
	lvlAt                     time.Time
	quietSince                time.Time // when the current quiet stretch began
	quietLast                 time.Time // the last frame that was quiet (a drop may follow within 600 ms)
	quietLen                  time.Duration
	loud                      float64 // 0..1 how loud vs the recent max
	build                     float64 // 0..1 how much louder than the last ~8 s
	queue                     []sched
	started                   time.Time
	lastFrame                 time.Time
	running                   bool
	stopCh                    chan struct{}
	decayKick, decaySnare     float64
	decayPop, decaySpark      float64
}

func newStreamer(opts Options, b *Bridge, a *Area) *Streamer {
	s := &Streamer{opts: opts, bridge: b, area: a, state: map[int]*chanState{}, rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
	var cx, cy float64
	for _, ch := range a.Channels {
		s.state[ch.ID] = &chanState{x: ch.X, y: ch.Y}
		cx += ch.X
		cy += ch.Y
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
			if contains(opts.Sparkle, n) {
				s.sparkle = appendUnique(s.sparkle, ch.ID)
			}
		}
	}
	if n := float64(len(a.Channels)); n > 0 {
		cx, cy = cx/n, cy/n
	}
	s.ceiling = append(append([]int{}, s.pairA...), s.pairB...)
	for _, id := range s.ceiling {
		if s.state[id].x < cx {
			s.left = append(s.left, id)
		} else {
			s.right = append(s.right, id)
		}
	}
	// sweep: every channel, clockwise round the room's centre
	for _, ch := range a.Channels {
		s.order = append(s.order, ch.ID)
	}
	sort.Slice(s.order, func(i, j int) bool {
		a, b := s.state[s.order[i]], s.state[s.order[j]]
		return math.Atan2(a.y-cy, a.x-cx) < math.Atan2(b.y-cy, b.x-cx)
	})
	s.baseHue = float64(s.rng.Intn(360))
	s.lvl, s.lvlMax, s.lvlAvg = -40, -30, -40
	dt := 1 / float64(opts.FPS)
	perFrame := func(ms int) float64 { // level 1 → 0.05 in ms
		if ms < 20 {
			ms = 20
		}
		return math.Exp(-dt / (float64(ms) / 1000 / math.Log(20)))
	}
	s.decayKick = perFrame(opts.DecayMs)
	s.decaySnare = perFrame(opts.SnareDecayMs)
	s.decayPop = perFrame(90)
	s.decaySpark = perFrame(110)
	if len(s.sparkle) == 0 {
		s.sparkle = s.follow
	}
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

// frame renders every channel (bed + flash + pop + sparkle) into one HueStream 2.0 message.
func (s *Streamer) frame(now time.Time) []byte {
	buf := make([]byte, 0, 16+36+len(s.area.Channels)*7)
	buf = append(buf, []byte("HueStream")...)
	buf = append(buf, 0x02, 0x00) // version 2.0
	buf = append(buf, s.seq)
	buf = append(buf, 0x00, 0x00) // reserved
	buf = append(buf, 0x00)       // colour space RGB
	buf = append(buf, 0x00)       // reserved
	buf = append(buf, []byte(s.area.ID)...)
	s.seq++
	t := now.Sub(s.started).Seconds()
	bedOn := s.opts.Mode == "show" && s.opts.BedLevel > 0
	quiet := s.loud < 0.2 && !s.quietSince.IsZero() && now.Sub(s.quietSince) > 1500*time.Millisecond
	for _, ch := range s.area.Channels {
		st := s.state[ch.ID]
		var out RGB
		if bedOn {
			// the wash: hue drifts round the wheel with position offsets; brightness breathes with
			// the loudness, swells with a build, and slowly pulses in a quiet passage
			hue := s.baseHue + 40*math.Sin(2*math.Pi*t/s.opts.BedSpeedS+st.x*1.2+st.y*0.7) + st.x*18
			v := s.opts.BedLevel * (0.25 + 0.75*s.loud) * (1 + 1.2*s.build)
			if quiet {
				v = s.opts.BedLevel * (0.35 + 0.25*math.Sin(2*math.Pi*t/6))
			}
			sat := 1 - 0.55*s.build
			out = hsv(hue, sat, clamp(v, 0, 0.6))
		}
		fl := math.Pow(st.flashLvl, 1.6)
		pop := math.Pow(st.pop, 1.3)
		for i := 0; i < 3; i++ {
			out[i] = clamp(out[i]+st.flash[i]*fl+white[i]*pop+white[i]*st.spark*0.9, 0, 1)
		}
		buf = append(buf, byte(ch.ID))
		for i := 0; i < 3; i++ {
			v := uint16(math.Round(out[i] * 65535))
			buf = append(buf, byte(v>>8), byte(v))
		}
	}
	return buf
}

func (s *Streamer) run(ctx context.Context, status func()) {
	dt := time.Second / time.Duration(s.opts.FPS)
	tick := time.NewTicker(dt)
	defer tick.Stop()
	s.started = time.Now()
	s.lastFrame = s.started
	s.lvlAt = s.started
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
			s.step(now)
			f := s.frame(now)
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

// step advances the envelopes, the loudness trackers and the scheduled flashes.
func (s *Streamer) step(now time.Time) {
	dts := now.Sub(s.lastFrame).Seconds()
	s.lastFrame = now
	for _, st := range s.state {
		if now.After(st.flashHold) {
			st.flashLvl *= st.flashDecay
			if st.flashLvl < 0.004 {
				st.flashLvl = 0
			}
		}
		if now.After(st.popHold) {
			st.pop *= s.decayPop
			if st.pop < 0.004 {
				st.pop = 0
			}
		}
		st.spark *= s.decaySpark
		if st.spark < 0.004 {
			st.spark = 0
		}
	}
	// loudness: the recent max sinks slowly, the slow average follows with ~8 s
	s.lvlMax -= 0.4 * dts
	if s.lvl > s.lvlMax {
		s.lvlMax = s.lvl
	}
	s.lvlAvg += (s.lvl - s.lvlAvg) * clamp(dts/8, 0, 1)
	// no level events for a while (the ear's old build, or a dropped connection) → assume loud
	if now.Sub(s.lvlAt) > 3*time.Second {
		s.loud = 0.8
		s.build = 0
	} else {
		s.loud = clamp((s.lvl-(s.lvlMax-18))/18, 0, 1)
		s.build = clamp((s.lvl-s.lvlAvg-2)/6, 0, 1)
	}
	if s.loud < 0.2 {
		if s.quietSince.IsZero() {
			s.quietSince = now
		}
		s.quietLast = now
		s.quietLen = now.Sub(s.quietSince)
	} else {
		s.quietSince = time.Time{}
	}
	// scheduled flashes (sweeps, strobes)
	keep := s.queue[:0]
	for _, q := range s.queue {
		if now.Before(q.at) {
			keep = append(keep, q)
			continue
		}
		s.flash(q.ids, q.col, q.lvl, q.decay, q.hold, q.pop, now)
	}
	s.queue = keep
}

func (s *Streamer) flash(ids []int, col RGB, lvl, decay float64, hold time.Duration, pop float64, now time.Time) {
	until := now.Add(hold)
	for _, id := range ids {
		st := s.state[id]
		if st == nil {
			continue
		}
		if lvl >= st.flashLvl {
			st.flash = col
			st.flashLvl = lvl
			st.flashHold = until
			st.flashDecay = decay
		}
		if pop > st.pop {
			st.pop = pop
			st.popHold = now.Add(25 * time.Millisecond)
		}
	}
}

func (s *Streamer) level(db float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	dt := now.Sub(s.lvlAt).Seconds()
	if dt <= 0 || dt > 2 {
		dt = 0.1
	}
	// attack in ~0.1 s, release in ~0.6 s, whatever the event rate
	tau := 0.6
	if db > s.lvl {
		tau = 0.1
	}
	s.lvl += (db - s.lvl) * (1 - math.Exp(-dt/tau))
	s.lvlAt = now
}

func strength(e float64) float64 { // the ear's ratio (1..4) → 0.35..1
	if e <= 0 {
		e = 1.5
	}
	return clamp(0.35+(e-1)*0.4, 0.35, 1)
}

// kick: the main event. Pattern, colour and the drop/strobe logic live here.
func (s *Streamer) kick(e float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	str := strength(e)
	s.kicks++
	s.patternKicks++
	hold := time.Duration(s.opts.HoldMs) * time.Millisecond
	hueA, hueB := s.baseHue, s.baseHue+150
	colA, colB := hsv(hueA, 0.9, 1), hsv(hueB, 0.9, 1)

	// a drop: a quiet stretch of 1.5 s+ that ended within the last 600 ms, then a proper kick
	drop := s.opts.Mode == "show" && !s.quietLast.IsZero() && now.Sub(s.quietLast) < 600*time.Millisecond && s.quietLen >= 1500*time.Millisecond && str >= 0.55
	if drop && s.opts.DropStrobe {
		all := s.all()
		for i := 0; i < 4; i++ {
			s.queue = append(s.queue, sched{at: now.Add(time.Duration(i) * 95 * time.Millisecond), ids: all, col: white, lvl: 1, decay: s.decayPop, hold: 30 * time.Millisecond, pop: 1})
		}
		s.baseHue += 137
		s.allUntilKick = s.kicks + 8
		s.quietSince, s.quietLast, s.quietLen = time.Time{}, time.Time{}, 0
		s.lvlAvg = s.lvl
		log.Printf("stream: drop at kick %d", s.kicks)
		return
	}
	if s.kicks%32 == 0 {
		s.baseHue += 137 // a fresh palette now and then
	}
	if s.opts.Mode == "chase" {
		s.turnA = !s.turnA
		if s.turnA {
			s.flash(s.pairA, colA, 1, s.decayKick, hold, 0, now)
		} else {
			s.flash(s.pairB, colB, 1, s.decayKick, hold, 0, now)
		}
		if s.opts.FollowEvery > 0 && s.kicks%s.opts.FollowEvery == 0 {
			s.flash(s.follow, colB, 1, s.decayKick, hold, 0, now)
		}
		return
	}
	if s.patternKicks > s.opts.PatternEvery {
		s.patternKicks = 1
		s.pattern = (s.pattern + 1) % 4
	}
	pattern := s.pattern
	if s.kicks < s.allUntilKick {
		pattern = 3 // all together after a drop
	}
	pop := 0.55 * str
	switch pattern {
	case 0: // diagonals
		s.turnA = !s.turnA
		if s.turnA {
			s.flash(s.pairA, colA, str, s.decayKick, hold, pop, now)
		} else {
			s.flash(s.pairB, colB, str, s.decayKick, hold, pop, now)
		}
	case 1: // left / right
		s.turnA = !s.turnA
		if s.turnA {
			s.flash(s.left, colA, str, s.decayKick, hold, pop, now)
		} else {
			s.flash(s.right, colB, str, s.decayKick, hold, pop, now)
		}
	case 2: // a sweep round the room, 45 ms a step
		for i, id := range s.order {
			s.queue = append(s.queue, sched{at: now.Add(time.Duration(i) * 45 * time.Millisecond), ids: []int{id}, col: colA, lvl: str, decay: s.decayKick, hold: hold, pop: pop * 0.6})
		}
	default: // all the ceilings together, colour alternating
		s.turnA = !s.turnA
		c := colA
		if !s.turnA {
			c = colB
		}
		s.flash(s.ceiling, c, str, s.decayKick, hold, pop, now)
	}
}

func (s *Streamer) snare(e float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snares++
	str := strength(e)
	col := hsv(s.baseHue+150+30, 0.95, 1)
	if s.opts.Mode == "chase" {
		return
	}
	s.flash(s.follow, col, str, s.decaySnare, time.Duration(s.opts.SnareHoldMs)*time.Millisecond, 0.25*str, time.Now())
}

func (s *Streamer) hat(e float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hats++
	if s.opts.Mode == "chase" || s.opts.HatLevel <= 0 {
		return
	}
	lvl := s.opts.HatLevel * strength(e)
	// one random sparkle light per tick, not all of them
	if len(s.sparkle) > 0 {
		id := s.sparkle[s.rng.Intn(len(s.sparkle))]
		if st := s.state[id]; st != nil && lvl > st.spark {
			st.spark = lvl
		}
	}
}

// hit: everything, white pop + colour, held a little longer.
func (s *Streamer) hit() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hits++
	c := hsv(s.baseHue, 0.85, 1)
	s.flash(s.all(), c, 1, s.decayKick, 2*time.Duration(s.opts.HoldMs)*time.Millisecond, 0.9, time.Now())
}

func (s *Streamer) all() []int {
	ids := make([]int, 0, len(s.state))
	for _, ch := range s.area.Channels {
		ids = append(ids, ch.ID)
	}
	return ids
}

func (s *Streamer) close() {
	s.running = false
	if s.conn != nil {
		// a few black frames so nothing is left mid-flash, then the bridge restores the lights
		s.mu.Lock()
		for _, st := range s.state {
			st.flashLvl, st.pop, st.spark = 0, 0, 0
		}
		s.opts.BedLevel = 0
		for i := 0; i < 3; i++ {
			_, _ = s.conn.Write(s.frame(time.Now()))
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
	if res.StatusCode >= 300 {
		out, _ := io.ReadAll(res.Body)
		log.Printf("ha: state: HTTP %d %s", res.StatusCode, strings.TrimSpace(string(out)))
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
		Cmd string  `json:"cmd"`
		E   float64 `json:"e"`
		DB  float64 `json:"db"`
	} `json:"data"`
}

type command struct {
	cmd string
	e   float64
	db  float64
}

// listen subscribes to party_stream events (+ show_room clearing as a safety net) and hands
// them to handle; it reconnects until ctx ends.
func (h *HA) listen(ctx context.Context, handle func(command)) {
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

func (h *HA) listenOnce(ctx context.Context, handle func(command)) error {
	c, _, err := websocket.DefaultDialer.DialContext(ctx, h.wsURL, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	if _, _, err := c.ReadMessage(); err != nil { // auth_required
		return err
	}
	if err := c.WriteJSON(map[string]string{"type": "auth", "access_token": h.token}); err != nil {
		return err
	}
	var ok struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	}
	if err := c.ReadJSON(&ok); err != nil || ok.Type != "auth_ok" {
		return fmt.Errorf("auth failed: %s %s", ok.Type, ok.Message)
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
			handle(command{cmd: "stop"})
			continue
		}
		var ev eventData
		if json.Unmarshal(m.Event, &ev) == nil && ev.Data.Cmd != "" {
			handle(command{cmd: ev.Data.Cmd, e: ev.Data.E, db: ev.Data.DB})
		}
	}
}

// ── main ───────────────────────────────────────────────────────────────────

func main() {
	optPath := flag.String("options", "/data/options.json", "options file")
	dataDir := flag.String("data", "/config", "where hue.json lives (the add-on's own config folder, /addon_configs/<slug> on the host)")
	haURL := flag.String("ha-url", "", "HA base URL (default: the Supervisor proxy)")
	haToken := flag.String("ha-token", "", "HA token (default: SUPERVISOR_TOKEN)")
	demo := flag.Bool("demo", false, "no HA: pair, stream a 45 s demo track (intro, groove, breakdown, drop), exit")
	flag.Parse()
	log.SetFlags(log.Ltime)

	opts := loadOptions(*optPath)
	ha := &HA{token: *haToken}
	if ha.token == "" {
		ha.token = os.Getenv("SUPERVISOR_TOKEN")
		if ha.token == "" {
			ha.token = os.Getenv("HASSIO_TOKEN")
		}
		// s6-overlay (the add-on base image) keeps the container environment in files and
		// only hands it to commands run through with-contenv – read it ourselves as well
		if ha.token == "" {
			for _, f := range []string{"/run/s6/container_environment/SUPERVISOR_TOKEN", "/run/s6/container_environment/HASSIO_TOKEN"} {
				if b, err := os.ReadFile(f); err == nil && len(bytes.TrimSpace(b)) > 0 {
					ha.token = string(bytes.TrimSpace(b))
					break
				}
			}
		}
	}
	log.Printf("ha: token %d chars (SUPERVISOR_TOKEN set: %v, HASSIO_TOKEN set: %v)", len(ha.token), os.Getenv("SUPERVISOR_TOKEN") != "", os.Getenv("HASSIO_TOKEN") != "")
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
	log.Printf("area %q (%s): %d channels, status %s, mode %s", area.Name, area.ID, len(area.Channels), area.Status, opts.Mode)
	for _, ch := range area.Channels {
		log.Printf("  channel %d: %s (%.2f, %.2f)", ch.ID, strings.Join(ch.Names, " + "), ch.X, ch.Y)
	}

	var (
		mu sync.Mutex
		st *Streamer
	)
	status := func() {
		mu.Lock()
		defer mu.Unlock()
		attrs := map[string]any{"paired": true, "area": area.Name, "channels": len(area.Channels), "fps": opts.FPS, "mode": opts.Mode}
		on := st != nil && st.running
		if on {
			st.mu.Lock()
			attrs["kicks"] = st.kicks
			attrs["snares"] = st.snares
			attrs["hats"] = st.hats
			attrs["hits"] = st.hits
			attrs["level_db"] = math.Round(st.lvl)
			attrs["loud"] = math.Round(st.loud*100) / 100
			attrs["pattern"] = st.pattern
			attrs["hue"] = math.Round(math.Mod(math.Mod(st.baseHue, 360)+360, 360)) // the tablet's screen follows the palette
			attrs["since"] = st.started.Format(time.RFC3339)
			st.mu.Unlock()
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
		log.Printf("stream: pair_a %v pair_b %v left %v right %v followers %v sparkle %v sweep %v", s.pairA, s.pairB, s.left, s.right, s.follow, s.sparkle, s.order)
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
			log.Printf("stream: stopped after %d kicks, %d snares, %d hats, %d hits", s.kicks, s.snares, s.hats, s.hits)
		}
		status()
	}
	handle := func(c command) {
		switch c.cmd {
		case "start":
			start()
		case "stop":
			stop()
		default:
			mu.Lock()
			s := st
			mu.Unlock()
			if s == nil || !s.running {
				return
			}
			switch c.cmd {
			case "kick":
				s.kick(c.e)
			case "beat":
				s.kick(1.5)
			case "snare":
				s.snare(c.e)
			case "hat":
				s.hat(c.e)
			case "level":
				s.level(c.db)
			case "hit":
				s.hit()
			}
		}
	}

	if *demo {
		start()
		if st == nil {
			os.Exit(1)
		}
		runDemo(handle)
		stop()
		return
	}

	status()
	ha.listen(ctx, handle)
}

// runDemo plays a pretend track: an intro of hats over a rising bed, a groove with kicks and
// snares, a breakdown, a drop with the strobe, then the full groove with accents. ~45 s.
func runDemo(handle func(command)) {
	beat := 469 * time.Millisecond // 128 bpm
	log.Print("demo: intro (hats, rising level)")
	t0 := time.Now()
	for i := 0; time.Since(t0) < 8*time.Second; i++ {
		handle(command{cmd: "hat", e: 2})
		if i%2 == 0 {
			handle(command{cmd: "level", db: -46 + 8*time.Since(t0).Seconds()/8})
		}
		time.Sleep(beat / 2)
	}
	log.Print("demo: groove (kicks, snares on 2 and 4, hats)")
	for i := 0; i < 32; i++ {
		handle(command{cmd: "kick", e: 1.8})
		if i%2 == 1 {
			handle(command{cmd: "snare", e: 2.2})
		}
		handle(command{cmd: "level", db: -30})
		time.Sleep(beat / 2)
		handle(command{cmd: "hat", e: 1.8})
		time.Sleep(beat / 2)
	}
	log.Print("demo: breakdown (quiet)")
	t0 = time.Now()
	for time.Since(t0) < 5*time.Second {
		handle(command{cmd: "level", db: -50})
		handle(command{cmd: "hat", e: 1.5})
		time.Sleep(beat)
	}
	log.Print("demo: the drop")
	handle(command{cmd: "level", db: -24})
	handle(command{cmd: "kick", e: 3.5})
	time.Sleep(beat)
	for i := 0; i < 40; i++ {
		handle(command{cmd: "kick", e: 2.2})
		if i%2 == 1 {
			handle(command{cmd: "snare", e: 2.5})
		}
		if i == 15 || i == 31 {
			handle(command{cmd: "hit"})
		}
		handle(command{cmd: "level", db: -26})
		time.Sleep(beat / 2)
		handle(command{cmd: "hat", e: 2})
		time.Sleep(beat / 2)
	}
	log.Print("demo: done")
}
