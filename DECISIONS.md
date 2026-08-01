# Architecture Decisions

Format per entry: decision, the alternative rejected, the observation that
would reverse it.

## Why Opus
- **Decision:** Every input converges on Opus @ 48kHz mono, the single
  outbound codec regardless of arrival format.
- **Rejected:** G.711 (native to telephony, zero transcoding for that one
  source) -- ~10x the bandwidth, no loss-concealment story to build
  redundancy on. Also rejected passing each input through in its native
  codec and pushing normalization onto consumers.
- **Reversal trigger:** A consumer that structurally can't decode Opus
  needs its own transcoding adapter, not a second outbound codec.

## Why RFC 2198 Redundancy
- **Decision:** Depth-1 redundancy -- each packet carries its own Opus
  frame plus a copy of the immediately preceding one (960-sample / 20ms
  offset), so one lost packet is fully recoverable from the next.
- **Rejected:** Deeper redundancy (3+ frames back) -- multiplies outbound
  bandwidth. `aiortc` for generic RTP framing -- pulls in ICE/DTLS/SRTP
  we don't need for a server-side worker that just packs bytes.
- **Reversal trigger:** Burst loss of 2+ consecutive packets becoming
  common in practice (see `loss_experiment.md`) -- depth-1 can't recover
  that, would need depth 2-3.

## Why Bounded Queues + Drop Policy (vs. Backpressure)
- **Decision:** One bounded `asyncio.Queue` per consumer (maxsize=100,
  ~2s audio); `broadcast` uses `put_nowait` and drops for that consumer
  only if full. Ingestion never waits on a consumer. Isolation is
  structural -- one queue filling never touches another queue or the
  ingestion path (proven under attach/detach churn in `mock_consumer.py`).
- **Rejected:** Blocking broadcast until every queue has room -- lets one
  slow consumer stall every other consumer and the source, violating the
  no-stall constraint directly.
- **Reversal trigger:** A consumer needing guaranteed delivery (e.g.
  compliance recording) needs its own disk-backed buffer, not a change to
  the shared policy. Also: the wire format has no sequence number, so a
  lagging consumer can't tell "dropped" from "nothing sent" -- add one if
  a consumer needs to detect its own lag rather than just decode around it.

## Why PyAV for Normalization
- **Decision:** PyAV's `AudioResampler` handles all rate/channel
  conversion ahead of Opus encoding.
- **Rejected:** Hand-rolled linear-interpolation resampling (no
  anti-aliasing, audibly worse). `scipy` for the one thing ffmpeg's
  resampler (already wrapped by PyAV) does correctly.
- **Reversal trigger:** If PyAV's per-frame overhead becomes the CPU
  bottleneck under many concurrent streams, profile against a lighter
  dedicated resampler (e.g. `libsamplerate` bindings).

## Sample-Rate Negotiation on Live Inputs
- **Decision:** `/ingest/live` and `/ingest/stream` take optional
  `?sample_rate=&channels=` params so each caller declares its actual PCM
  format; `/ingest/replay` reads it from the WAV header. Defaults (48kHz
  mono) match the outbound target.
- **Rejected:** Assuming one fixed source format for all live input --
  the original implementation, silently wrong for e.g. 8kHz telephony.
  Sniffing format from raw PCM bytes -- sample rate isn't recoverable
  from bytes alone.
- **Reversal trigger:** A source changing format mid-stream (SIP
  renegotiation) would need an in-band format header, not a per-connection
  param.

## What a Late-Joining Consumer Hears
- **Decision:** Nothing before it attaches -- no priming buffer, no
  replay-from-start.
- **Rejected:** Buffering the last N seconds and replaying on attach --
  adds per-consumer state, and transcription/turn-detection consumers
  don't want stale audio replayed as live.
- **Reversal trigger:** A consumer class explicitly wanting catch-up
  semantics should get an opt-in priming buffer for that type, not a
  global one.

## Rejected Alternatives (broader)
- **Redis Streams/Kafka** between ingestion and consumers instead of
  in-process fan-out -- adds an operational dependency and network hop for
  what's currently a single-process problem. Reversal trigger: needing to
  scale consumer count beyond one process/host.

## One Thing I Got Wrong
**What I expected:** In `AudioPipeline.process_chunk`, that reading a
resampled `av.AudioFrame`'s raw samples straight out of `frame.planes[0]`
(`np.frombuffer(frame.planes[0], dtype=np.int16)`) would give exactly
`frame.samples` values.

**What actually happened:** PyAV pads plane buffers to an internal
line-size boundary. Resampling 320 samples (20ms @ 16kHz) to 48kHz
produced a frame reporting `frame.samples == 912`, but `frame.planes[0]`
held 992 int16 values -- 80 extra samples of padding past the real audio.
Reading the plane directly would have spliced that padding into every
resampled chunk, quietly corrupting PCM before it reached the encoder.

**What I changed:** Switched to `frame.to_ndarray()`, which PyAV trims to
exactly `frame.samples`, and built the buffering logic in `process_chunk`
on top of that instead of touching `.planes` directly. Caught it by
inspecting shapes in a throwaway script before it reached the encoder --
PyAV's "raw buffer" accessors are lower-level than they look and aren't
sample-count-safe by default.
