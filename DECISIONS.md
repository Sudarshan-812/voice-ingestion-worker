# Architecture Decisions

## Why Opus

## Why RFC 2198 Redundancy

**Decision:** Implemented a depth-1 RFC 2198 redundancy (current frame + exactly
one previous 20ms frame) using a hardcoded 960-sample timestamp offset
(20ms @ 48kHz). Each Opus packet carries its own payload plus a copy of the
immediately preceding packet, so a single lost packet can be fully
reconstructed from the one that follows it.

**Rejected Alternative:** Deep redundancy (wrapping 3+ previous frames), or
adopting a heavyweight RTP library (e.g. `aiortc`) to manage the payload
framing generically. Both add real cost -- deep redundancy multiplies
outbound bandwidth per frame, and a full RTP stack pulls in ICE/DTLS/SRTP
machinery we don't need for a server-side ingestion worker that just needs
to pack bytes correctly.

**Reversal Trigger:** If the network exhibits heavy burst packet loss
exceeding 20ms (i.e. two or more consecutive frames lost), depth-1
redundancy stops being sufficient to reconstruct the stream and we'd need
to increase redundancy depth to 2 or 3 previous frames (trading more
bandwidth for more loss tolerance).

## Why Bounded Queues + Drop Policy (vs. Backpressure)

## Why PyAV for Normalization

## Consumer Isolation Guarantees

## Rejected Alternatives

## One Thing I Got Wrong

**What I expected:** In `AudioPipeline.process_chunk`, I assumed I could read
a resampled `av.AudioFrame`'s raw samples straight out of
`frame.planes[0]` and treat that buffer as exactly `frame.samples` int16
values -- i.e. `np.frombuffer(frame.planes[0], dtype=np.int16)` would give
me precisely the audio for that frame.

**What actually happened:** PyAV pads plane buffers to an internal
line-size boundary, so the raw buffer is larger than the actual sample
count. Resampling 320 samples (20ms @ 16kHz) up to 48kHz produced a frame
that reported `frame.samples == 912`, but `frame.planes[0]` held 992 int16
values -- 80 extra samples of padding/stale buffer past the real audio.
Reading the plane directly would have spliced that padding into every
resampled chunk, corrupting the PCM by the time it reached the Opus
encoder, in a way that wouldn't necessarily crash anything -- it would just
quietly produce slightly wrong audio.

**What I changed:** Switched to `frame.to_ndarray()`, which PyAV trims to
exactly `frame.samples` before returning it, and built the buffering logic
in `process_chunk` (accumulate resampled PCM, slice off exactly
`OPUS_FRAME_SAMPLES` at a time) on top of that instead of touching
`.planes` directly. Caught this by inspecting shapes in a throwaway test
script before it ever reached the encoder -- worth remembering that PyAV's
"raw buffer" accessors are lower-level than they look and aren't
sample-count-safe by default.
