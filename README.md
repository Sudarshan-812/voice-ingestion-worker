# Voice Ingestion Worker

## Overview

Ingests live audio from several materially different sources, normalizes
all of it to a single outbound Opus stream with RFC 2198 redundancy, and
fans that stream out to any number of independent consumers (transcribers,
recorders, analytics) at once. See `DECISIONS.md` for the reasoning behind
every non-obvious choice below, and `loss_experiment.md` for the packet
loss demonstration.

## Architecture

```
ingest_websocket ---\
ingest_http_stream ---+--> AudioPipeline (per source) --> IngestionWorker.broadcast
ingest_file_replay --/                                          |
                                                     fan-out to N bounded queues
                                                                  |
                                                    consumer 1 ... consumer N (/stream)
```

- **`src/inputs.py`** -- three ingestion handlers, one per source. Each owns
  its own `AudioPipeline` instance (no shared state across streams) and
  pushes finished packets into the single `IngestionWorker`.
- **`src/encoder.py`** -- `AudioPipeline`: PyAV resampling to 48kHz mono
  s16, 20ms framing, Opus encoding.
- **`src/rfc2198.py`** -- byte-level RFC 2198 packer (depth-1: current
  frame + previous frame).
- **`src/engine.py`** -- `IngestionWorker`: owns the set of attached
  consumer queues and the bounded-queue/drop backpressure policy.
- **`src/main.py`** -- FastAPI wiring for the four HTTP/WS endpoints below.

## Ingestion Sources

Three sources, chosen because each stresses a different part of the
pipeline rather than just arriving through a different pipe:

1. **`WS /ingest/live`** -- realtime packetized audio pushed frame-by-frame
   over a WebSocket (e.g. a telephony bridge or browser client). This is
   the "live, arriving over the wire" source required by the brief: the
   worker has no control over pacing and must keep up with whatever
   cadence the caller sends at.
2. **`POST /ingest/stream`** -- audio uploaded as an HTTP chunked-transfer
   body. Same live-arrival constraint as the WS source, but through a
   request-response shaped transport instead of a persistent bidirectional
   socket -- exercises `request.stream()` backpressure instead of
   WebSocket framing.
3. **`POST /ingest/replay`** -- replays `data/sample.wav` from disk,
   throttled to real-time pacing (sleeps between chunks for exactly the
   wall-clock duration of the audio just sent). This is the "archive
   someone wants replayed through the live path" source, and the one
   genuinely different problem here is pacing: reading a file naively
   would blast it through the pipeline as fast as disk I/O allows, which
   is not what a live consumer downstream expects.

All three converge on the same `AudioPipeline.process_chunk()` ->
`IngestionWorker.broadcast()` path once bytes are in hand.

**Sample rate / channel handling:** `/ingest/live` and `/ingest/stream`
accept optional `?sample_rate=&channels=` query params describing the raw
PCM the caller is about to send (default: 48000/mono, i.e. already at the
target format). `/ingest/replay` reads the WAV header directly. See
DECISIONS.md, "Sample-Rate Negotiation on Live Inputs".

All sources are expected to send raw interleaved 16-bit PCM. Encoded/
containerized input (e.g. a browser sending WebM/Opus directly) is out of
scope for this submission -- see "Rejected Alternatives" in DECISIONS.md.

## Outbound Stream / Consumer Contract

- `WS /stream` -- consumers connect and receive a continuous sequence of
  binary WebSocket frames, each one RFC 2198-wrapped Opus packet (20ms of
  audio, 48kHz mono, plus the previous frame as redundancy).
- No handshake, no out-of-band metadata: the payload type (111, dynamic)
  and framing are fixed and documented in `src/rfc2198.py`.
- **Late joiners** hear nothing before the moment they attach -- there is
  no priming buffer. The first packet they receive after connecting is a
  normal in-sequence packet; its redundant block (if any) points at a
  frame they never saw, which is simply ignored on decode.
- Any number of consumers may attach and detach at any time, independently
  of each other and of the ingestion side.

## Running Locally

Requires Python 3.11+, `ffmpeg`'s shared libraries, and `libopus`
installed on the host (the Docker image installs both; see below for a
container-based run instead).

```bash
pip install -r requirements.txt
uvicorn src.main:app --reload
```

Then feed it audio, e.g. `curl -X POST http://localhost:8000/ingest/replay`
to replay the bundled sample file, and see "Testing Consumers" below to
attach.

## Building the Container

```bash
make            # equivalent to `make image`
make run        # docker run -p 8000:8000 voice-ingestion-worker
```

`make image` is the one target required by the brief: on a clean checkout
it produces a runnable image with nothing preinstalled and no manual steps.

## Configuration

Everything is currently a module-level constant rather than an environment
variable (`TARGET_SAMPLE_RATE`, `TARGET_CHANNELS`, `OPUS_FRAME_MS` in
`src/encoder.py`; `CONSUMER_QUEUE_MAXSIZE` in `src/engine.py`) -- there was
no deployment surface in scope for this exercise that needed them
runtime-configurable. If/when this needs to run against more than one
outbound format or loss profile, these are the knobs to promote to env
vars first.

## Operational Notes

- `GET /` returns `{"status": "ok", "consumers": N}` -- a cheap liveness
  probe that also surfaces current fan-out width, useful for confirming
  consumer attach/detach is being tracked correctly under load.
- A stalled or slow consumer never blocks ingestion or other consumers:
  `IngestionWorker.broadcast` uses `put_nowait` against a bounded
  (maxsize=100) per-consumer queue and silently drops the packet for that
  consumer only if it's full. See "Why Bounded Queues + Drop Policy" in
  DECISIONS.md.
- The worker holds no persistent state across restarts -- there's nothing
  to migrate or replay on deploy; a restarted worker just starts with zero
  consumers and waits for ingestion sources to reconnect.

## Testing Consumers

`scripts/mock_consumer.py` is a mock transcriber: it opens several concurrent
WebSocket connections to `/stream`, each with a staggered start time and
connected duration, so it exercises attach/detach churn against
`IngestionWorker` rather than a single steady connection. Each consumer also
unwraps the RFC 2198 payload back to the primary Opus frame and feeds it to
`opuslib.Decoder`, proving the encode -> wrap -> broadcast -> unwrap -> decode
round trip is byte-correct, not just that packets arrive.

To run it:

1. Start the worker: `uvicorn src.main:app --reload`
2. Kick off an ingestion source so there's something on the stream, e.g. the
   replay endpoint: `curl -X POST http://localhost:8000/ingest/replay`
3. In a second terminal, run the mock consumers: `python scripts/mock_consumer.py`

Watch the logs for each consumer's connect/decode/disconnect lifecycle, and
confirm `GET /` shows the expected `consumers` count rising and falling as
individual mock consumers attach and detach.

## Measurements

`scripts/measure.py` captures three things from a live worker: end-to-end
latency (ingest -> normalize -> encode -> RFC 2198 wrap -> fan-out ->
consumer receive) as a distribution rather than an average, CPU usage
under N concurrent replay streams, and packets-received-per-consumer as
consumer count rises.

```bash
uvicorn src.main:app &          # note the PID
python scripts/measure.py --pid <worker_pid> --consumers 1,5,10,25,50
```

*Results pending a local environment with a working `libopus` -- see
`DECISIONS.md` if this section still says "pending" by submission time.*

## Packet Loss Experiment

See `loss_experiment.md` for the methodology and results of inducing loss
against the outbound stream and showing RFC 2198 recovery in action.
Run it yourself with `python scripts/loss_simulator.py` (worker running,
replay endpoint triggered, per steps 1-2 above).
