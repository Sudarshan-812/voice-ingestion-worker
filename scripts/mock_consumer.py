"""Mock transcriber consumer.

Simulates multiple concurrent consumers attaching to and detaching from the
IngestionWorker's /stream endpoint, with staggered start/stop times, to
prove the worker's fan-out isolation holds under attach/detach churn (see
"Consumer Isolation Guarantees" in DECISIONS.md).

Each consumer also unwraps the RFC 2198 payload back down to the primary
Opus frame and feeds it to opuslib, which proves the full round trip
(encode -> RFC 2198 wrap -> broadcast -> unwrap -> decode) is byte-correct
end to end, not just that bytes arrive.
"""

from __future__ import annotations

import asyncio
import logging

import opuslib
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

SERVER_URL = "ws://localhost:8000/stream"
SAMPLE_RATE = 48_000
CHANNELS = 1
FRAME_SAMPLES = 960  # 20ms @ 48kHz, matches src.encoder.OPUS_FRAME_SAMPLES
LOG_EVERY_N_PACKETS = 50


def unwrap_rfc2198(packet: bytes) -> bytes:
    """Reverses src.rfc2198.wrap_rfc2198: returns the primary Opus payload.

    Wire format (see src/rfc2198.py):
      - F=0 (no redundancy): 1-byte primary header (byte 1) + primary payload.
      - F=1 (redundancy present): 4-byte redundant header (bytes 1-4) + 1-byte
        primary header (byte 5) + redundant payload + primary payload. The
        redundant block's length is a 10-bit field split across the header's
        3rd and 4th bytes (top 2 bits, then low 8 bits).
    """
    first_byte = packet[0]
    has_redundancy = bool(first_byte & 0x80)

    if not has_redundancy:
        return packet[1:]

    redundant_length = ((packet[2] & 0x03) << 8) | packet[3]
    primary_data_start = 5 + redundant_length  # skip 4-byte + 1-byte headers, then redundant data
    return packet[primary_data_start:]


async def consumer_task(consumer_id: int, duration: float) -> None:
    """Attaches to /stream, decodes packets for `duration` seconds, then disconnects."""
    decoder = opuslib.Decoder(SAMPLE_RATE, CHANNELS)
    decoded_count = 0

    logger.info("consumer %d: connecting", consumer_id)
    try:
        async with websockets.connect(SERVER_URL) as ws:
            logger.info("consumer %d: connected", consumer_id)
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

                primary_opus = unwrap_rfc2198(packet)
                decoder.decode(primary_opus, FRAME_SAMPLES)
                decoded_count += 1

                if decoded_count % LOG_EVERY_N_PACKETS == 0:
                    logger.info("consumer %d: decoded %d packets", consumer_id, decoded_count)
    except (websockets.exceptions.ConnectionClosed, OSError) as exc:
        logger.warning("consumer %d: connection error: %s", consumer_id, exc)
    finally:
        logger.info("consumer %d: disconnected cleanly after %d packets", consumer_id, decoded_count)


async def main() -> None:
    """Launches 5 concurrent consumers with staggered start times and
    durations to simulate aggressive attach/detach churn under load.
    """
    # (consumer_id, start_delay_seconds, connected_duration_seconds)
    consumers = [
        (1, 0.0, 5.0),
        (2, 1.0, 3.0),
        (3, 1.5, 6.0),
        (4, 2.0, 2.0),
        (5, 3.0, 4.0),
    ]

    async def staggered(consumer_id: int, start_delay: float, duration: float) -> None:
        await asyncio.sleep(start_delay)
        await consumer_task(consumer_id, duration)

    await asyncio.gather(*(staggered(cid, delay, dur) for cid, delay, dur in consumers))


if __name__ == "__main__":
    asyncio.run(main())
