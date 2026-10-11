# Party stream – Hue Entertainment for Party Mode

A Home Assistant add-on for Darren's house. The hall tablet's **Party ear**
(`www/house/party-ear.js`) hears the music and fires `party_stream` events; this add-on
streams a flash per beat to the Hue lights of an Entertainment area over DTLS at ~40
frames a second, so the lights pulse with the kick and go dark in between – no Hue REST
lag, no 10-commands-a-second limit.

- `start` / `stop` events (from `packages/party_mode.yaml`) activate and release the area.
- `beat` → the two-step chase: `pair_a` and `pair_b` alternate, one colour each for a
  phrase of `phrase` beats; `followers` flash every `follow_every`-th beat.
- `hit` → every channel, one colour, held twice as long.
- `binary_sensor.party_stream` is `on` while streaming (attributes: paired, area, beats, hits).

First run: it asks the bridge for an application key **with a client key** – press the
bridge's link button within five minutes (a persistent notification says so). The keys
live in the add-on's own config folder (`/addon_configs/<slug>/hue.json` on the Green), never in git.

The binary is built on the Mac (no Go on the Green):

    cd addons/party_stream
    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o bin/party-stream-aarch64 .

Test on the Mac against the bridge without HA: `go run . -options opts.json -data ./keys -demo`.
