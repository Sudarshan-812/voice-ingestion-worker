"""Packet loss / RFC 2198 recovery experiment.

Based on mock_consumer.py, but instead of just decoding every packet, this
script simulates network loss at the application layer: for each packet it
receives from /stream, it rolls the dice and, DROP_RATE of the time, discards
the packet as if it never arrived. When the next packet shows up, it checks
whether the previous one was "lost" and -- because every packet carries an
RFC 2198 redundant copy of the frame before it -- pulls the missing audio
back out of the current packet's redundant block instead of losing it.

See loss_experiment.md for the write-up and results this script produces.
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass, field

import opuslib
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

SERVER_URL = "ws://localhost:8000/stream"
SAMPLE_RATE = 48_000
CHANNELS = 1
FRAME_SAMPLES = 960  # 20ms @ 48kHz, matches src.encoder.OPUS_FRAME_SAMPLES
DROP_RATE = 0.20  # simulated network loss: 20% of incoming packets
RUN_DURATION_SECONDS = 15.0


def parse_rfc2198(packet: bytes) -> tuple[bytes | None, bytes]:
    """Splits one RFC 2198 payload into (redundant_payload_or_None, primary_payload).

    Mirrors src.rfc2198.wrap_rfc2198 exactly (see scripts/mock_consumer.py's
    unwrap_rfc2198 for the F=0/F=1 wire-format breakdown).
    """
    has_redundancy = bool(packet[0] & 0x80)
    if not has_redundancy:
        return None, packet[1:]

    redundant_length = ((packet[2] & 0x03) << 8) | packet[3]
    redundant_start = 5
    redundant_end = redundant_start + redundant_length
    return packet[redundant_start:redundant_end], packet[redundant_end:]


@dataclass
class LossStats:
    received: int = 0
    dropped: int = 0
    recovered: int = 0
    permanently_lost: int = 0
    events: list[str] = field(default_factory=list)


class LossRecoverySession:
    """Tracks simulated packet loss + depth-1 RFC 2198 recovery across a
    single in-order packet stream.

    Only ever tracks one outstanding loss at a time, because the encoder's
    redundancy depth is 1 -- a packet can only recover the *immediately*
    preceding frame. If a second drop happens before recovery, the first
    one is unrecoverable (burst loss); see loss_experiment.md "Limitations".
    """

    def __init__(self, decoder: "opuslib.Decoder", stats: LossStats) -> None:
        self._decoder = decoder
        self._stats = stats
        self._pending_lost_seq: int | None = None

    def _log_permanent_loss(self, seq: int, reason: str) -> None:
        message = f"PERMANENT LOSS: Packet {seq} unrecoverable ({reason})"
        logger.warning(message)
        self._stats.events.append(message)
        self._stats.permanently_lost += 1

    def observe_drop(self, seq: int) -> None:
        """Simulates packet `seq` never arriving."""
        if self._pending_lost_seq is not None:
            # Two drops in a row: depth-1 redundancy can't reach back far
            # enough to save the earlier one.
            self._log_permanent_loss(
                self._pending_lost_seq, f"burst loss, Packet {seq} also dropped"
            )

        message = f"NETWORK DROP: Packet {seq} lost"
        logger.info(message)
        self._stats.events.append(message)
        self._stats.dropped += 1
        self._pending_lost_seq = seq

    def observe_received(self, seq: int, packet: bytes) -> None:
        """Processes packet `seq`, recovering the previous packet from its
        redundant block if the previous packet was marked lost.
        """
        redundant_payload, primary_payload = parse_rfc2198(packet)

        if self._pending_lost_seq is not None:
            lost_seq = self._pending_lost_seq
            if lost_seq == seq - 1 and redundant_payload is not None:
                self._decoder.decode(redundant_payload, FRAME_SAMPLES)
                message = f"RECOVERY: Reconstructed Packet {lost_seq} from Packet {seq} redundant header."
                logger.info(message)
                self._stats.events.append(message)
                self._stats.recovered += 1
            elif redundant_payload is None:
                # F=0: this packet carries no redundancy at all, so whatever
                # was lost before it is gone for good.
                self._log_permanent_loss(lost_seq, f"Packet {seq} carried no redundancy (F=0)")
            else:
                self._log_permanent_loss(lost_seq, f"gap before Packet {seq}")
            self._pending_lost_seq = None

        self._decoder.decode(primary_payload, FRAME_SAMPLES)
        self._stats.received += 1

    def finalize(self) -> None:
        """Call once the stream ends -- flushes any never-recovered drop."""
        if self._pending_lost_seq is not None:
            self._log_permanent_loss(self._pending_lost_seq, "stream ended before the next packet arrived")
            self._pending_lost_seq = None


async def run_experiment(duration: float = RUN_DURATION_SECONDS, drop_rate: float = DROP_RATE) -> LossStats:
    """Connects to /stream, simulates `drop_rate` packet loss for `duration`
    seconds, and returns aggregate stats.
    """
    decoder = opuslib.Decoder(SAMPLE_RATE, CHANNELS)
    stats = LossStats()
    session = LossRecoverySession(decoder, stats)
    seq = 0

    logger.info("loss simulator: connecting (drop_rate=%.0f%%, duration=%.0fs)", drop_rate * 100, duration)
    try:
        async with websockets.connect(SERVER_URL) as ws:
            logger.info("loss simulator: connected")
            loop = asyncio.get_running_loop()
            deadline = loop.time() + duration

            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    packet = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break

                seq += 1
                if random.random() < drop_rate:
                    session.observe_drop(seq)
                else:
                    session.observe_received(seq, packet)
    except (websockets.exceptions.ConnectionClosed, OSError) as exc:
        logger.warning("loss simulator: connection error: %s", exc)
    finally:
        session.finalize()
        logger.info(
            "loss simulator: done -- received=%d dropped=%d recovered=%d permanently_lost=%d",
            stats.received,
            stats.dropped,
            stats.recovered,
            stats.permanently_lost,
        )

    return stats


async def main() -> None:
    await run_experiment()


if __name__ == "__main__":
    asyncio.run(main())
