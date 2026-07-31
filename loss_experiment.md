# Packet Loss Experiment

## Goal

Prove that wrapping outbound Opus frames in depth-1 RFC 2198 redundancy
(see "Why RFC 2198 Redundancy" in DECISIONS.md) lets a consumer recover from
isolated packet loss with no audible gap, and characterize exactly where
that protection stops working.

## Methodology

### The Setup

Loss is induced at the **application layer, on the consumer side**, in
`scripts/loss_simulator.py`: for every packet the mock consumer receives
from `/stream`, it rolls a random number and, `DROP_RATE` (20%) of the
time, discards the packet as if it never arrived over the network, instead
of handing it to the decoder. This is deliberate rather than a shortcut --
simulating loss with OS-level tools (`tc`/`netem`) requires root/admin
privileges and behaves differently across Linux, macOS, and Windows, which
would make the experiment non-reproducible across contributors' machines
and CI. Dropping at the consumer, after the packet has already traversed
the real WebSocket transport, isolates exactly the variable we care about
(loss *rate* and *pattern*) from the mechanism that produces it -- the
RFC 2198 recovery logic can't tell the difference between a packet dropped
by a flaky Wi-Fi link and one dropped by `random.random() < 0.20`.

### Test Matrix

| Scenario | Redundancy | Loss pattern |
|---|---|---|
| Baseline | None (raw Opus, hypothetical) | 20% isolated drops |
| Depth-1 (actual) | 1 previous 20ms frame | 20% isolated drops |
| Depth-1 (actual) | 1 previous 20ms frame | Burst: 2 consecutive drops |

## Results

Run via `python scripts/loss_simulator.py` against a live worker replaying
`data/sample.wav` (see README "Testing Consumers"). Each run logs one line
per event and a final summary:

```
NETWORK DROP: Packet 14 lost
RECOVERY: Reconstructed Packet 14 from Packet 15 redundant header.
...
NETWORK DROP: Packet 31 lost
PERMANENT LOSS: Packet 31 unrecoverable (burst loss, Packet 32 also dropped)
NETWORK DROP: Packet 32 lost
RECOVERY: Reconstructed Packet 32 from Packet 33 redundant header.
...
loss simulator: done -- received=39 dropped=11 recovered=10 permanently_lost=1
```

At a 20% independent drop rate, the overwhelming majority of drops are
isolated (no two consecutive packets lost) and are fully recovered; the
rare consecutive-drop case is exactly where recovery fails, as predicted.

## Analysis

### Without Redundancy

If the worker emitted raw Opus with no RFC 2198 wrapping, every dropped
packet would be an unrecoverable 20ms gap. Downstream, the decoder either:

- Inserts silence (or repeats/extrapolates via PLC -- packet loss
  concealment) for the missing 20ms, producing an audible stutter every
  time a packet is dropped, or
- If PLC isn't invoked correctly, has a discontinuity in the decoder's
  internal state (Opus's predictive coding assumes a continuous prior
  frame), which can produce short-lived artifacts or "decoder reset
  anomalies" until the next few frames re-stabilize the internal state.

At a 20% drop rate this is not a minor degradation -- roughly 1 in 5 frames
is affected, producing consistently robotic, stuttering audio.

### With RFC 2198 Redundancy (Depth 1)

Because every packet after the first carries a full copy of the *previous*
20ms frame in its redundant block, a single isolated drop is invisible to
the listener: when Packet N is lost, Packet N+1 still arrives with
Packet N's audio attached, and the consumer's `LossRecoverySession`
extracts it (`RECOVERY: Reconstructed Packet N from Packet N+1 redundant
header.`) and decodes it in the correct sequence position, immediately
before Packet N+1's own primary payload. The audio is reconstructed with a
`FRAME_SAMPLES`-accurate one-frame delay and no gap.

### Limitations

Redundancy depth is exactly 1, so this only protects against **isolated**
single-packet loss. If two consecutive packets are dropped (Packet N and
Packet N+1), the redundant copy that could have saved Packet N was carried
inside Packet N+1 -- which was also lost. Packet N+1 itself is still
recoverable (from Packet N+2's redundant block), but Packet N is gone for
good; `loss_simulator.py` logs this as `PERMANENT LOSS: Packet N
unrecoverable (burst loss, Packet N+1 also dropped)`. This matches the
reversal trigger noted in "Why RFC 2198 Redundancy" in DECISIONS.md: if real-world
network conditions show burst loss beyond 20ms becoming common, redundancy
depth needs to increase to 2 or 3 previous frames to keep closing this gap
-- at the cost of proportionally more outbound bandwidth per packet.

## Conclusions / Follow-ups

- Depth-1 RFC 2198 redundancy is sufficient to make isolated packet loss
  (the dominant failure mode on typical best-effort networks at moderate
  loss rates) fully inaudible.
- Burst loss of 2+ consecutive packets is the known, accepted gap in this
  design -- not a bug, but a deliberate depth-1 tradeoff (see DECISIONS.md).
- Follow-up: run `loss_simulator.py` against real network conditions (not
  just the simulated drop) to check whether burst loss is actually common
  enough on the target deployment network to justify increasing redundancy
  depth.
