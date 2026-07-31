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
