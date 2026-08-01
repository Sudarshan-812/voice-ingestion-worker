"""End-to-end measurements: ingest-to-consumer latency distribution, CPU
per concurrent stream, and behavior as consumer count rises.

Requires the worker running (`uvicorn src.main:app`) and `psutil` installed.
Talks to the worker purely over HTTP/WS -- CPU sampling reads the worker
process by PID, so run this from the same machine (or pass --pid if the
worker is running under a different process than the one you'd guess).

Usage:
    uvicorn src.main:app &
    python scripts/measure.py --pid <worker_pid> --consumers 1,5,10,25,50

See MEASUREMENTS.md for the write-up this script's output feeds into.
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import struct
import time
import wave
from dataclasses import dataclass, field
from pathlib import Path

import psutil
import websockets

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SAMPLE_WAV = DATA_DIR / "sample.wav"
STREAM_URL = "ws://localhost:8000/stream"
LIVE_URL = "ws://localhost:8000/ingest/live"

# 20ms @ 48kHz s16 mono raw PCM -- matches src.encoder.OPUS_FRAME_SAMPLES.
CHUNK_SAMPLES = 960
CHUNK_BYTES = CHUNK_SAMPLES * 2


@dataclass
class LatencySample:
    send_ts: float
    recv_ts: float

    @property
    def latency_ms(self) -> float:
        return (self.recv_ts - self.send_ts) * 1000


@dataclass
class RunResult:
    latencies_ms: list[float] = field(default_factory=list)
    cpu_percent_samples: list[float] = field(default_factory=list)
    packets_received: int = 0
    packets_dropped_estimate: int = 0


def _tone_chunk(seq: int) -> bytes:
    """Deterministic-but-distinguishable silent PCM chunk, tagged with a
    sequence number in the first two samples so a consumer could in
    principle correlate send/receive order (not required for the latency
    measurement below, which correlates via a single dedicated consumer
    reading its own injected stream, but kept for clarity/debuggability).
    """
    samples = [0] * CHUNK_SAMPLES
    samples[0] = seq & 0x7FFF
    return struct.pack(f"<{CHUNK_SAMPLES}h", *samples)


async def _latency_probe(num_frames: int) -> list[float]:
    """Opens one live-ingest connection and one /stream consumer, sends
    timestamped frames in, and measures wall-clock time until each
    corresponding outbound packet is observed on /stream.

    This measures added pipeline latency (resample -> encode -> RFC 2198
    wrap -> queue -> WS send -> WS receive), not network RTT -- both
    sockets are local. A dedicated ingest connection is used (rather than
    the shared replay/http paths) so each send has a precise timestamp.
    """
    latencies: list[float] = []

    async with websockets.connect(STREAM_URL) as consumer_ws:
        async with websockets.connect(LIVE_URL) as ingest_ws:
            send_times: list[float] = []

            async def sender() -> None:
                for i in range(num_frames):
                    send_times.append(time.perf_counter())
                    await ingest_ws.send(_tone_chunk(i))
                    await asyncio.sleep(0.02)  # pace like real 20ms audio

            async def receiver() -> None:
                received = 0
                while received < num_frames:
                    await consumer_ws.recv()
                    recv_time = time.perf_counter()
                    if received < len(send_times):
                        latencies.append((recv_time - send_times[received]) * 1000)
                    received += 1

            await asyncio.gather(sender(), receiver())

    return latencies


async def _cpu_sampler(pid: int, stop: asyncio.Event, interval: float = 0.5) -> list[float]:
    proc = psutil.Process(pid)
    proc.cpu_percent()  # prime; first call always returns 0.0
    samples: list[float] = []
    while not stop.is_set():
        await asyncio.sleep(interval)
        samples.append(proc.cpu_percent())
    return samples


def _post_replay_sync() -> None:
    import urllib.request

    req = urllib.request.Request("http://localhost:8000/ingest/replay", method="POST")
    with urllib.request.urlopen(req) as resp:
        if resp.status != 200:
            raise RuntimeError(f"replay trigger failed: {resp.status}")


async def _replay_source(stop: asyncio.Event | None = None) -> None:
    """Triggers throttled file-replay ingestion via HTTP POST.

    Each request blocks until the whole file has been paced through the
    pipeline (see src/inputs.py's ingest_file_replay), so it's run in a
    thread to avoid blocking the event loop. If `stop` is given, keeps
    re-triggering replay back-to-back until `stop` is set, so a load test
    has continuous audio for its full duration rather than just one replay
    of data/sample.wav.
    """
    if stop is None:
        await asyncio.to_thread(_post_replay_sync)
        return
    while not stop.is_set():
        await asyncio.to_thread(_post_replay_sync)


async def _consumer_drain(duration: float) -> int:
    """Attaches to /stream and counts packets received in `duration`s."""
    count = 0
    async with websockets.connect(STREAM_URL) as ws:
        deadline = time.perf_counter() + duration
        while time.perf_counter() < deadline:
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                break
            try:
                await asyncio.wait_for(ws.recv(), timeout=remaining)
                count += 1
            except asyncio.TimeoutError:
                break
    return count


async def run_latency_measurement(num_frames: int = 200) -> None:
    print(f"\n=== Latency distribution ({num_frames} frames) ===")
    latencies = await _latency_probe(num_frames)
    if not latencies:
        print("no samples captured")
        return
    latencies.sort()
    p50 = statistics.median(latencies)
    p90 = latencies[int(len(latencies) * 0.90)]
    p99 = latencies[min(int(len(latencies) * 0.99), len(latencies) - 1)]
    print(f"n={len(latencies)}  min={min(latencies):.2f}ms  p50={p50:.2f}ms  "
          f"p90={p90:.2f}ms  p99={p99:.2f}ms  max={max(latencies):.2f}ms")


async def run_cpu_measurement(pid: int, num_concurrent_replays: int, duration: float = 10.0) -> None:
    print(f"\n=== CPU under {num_concurrent_replays} concurrent replay stream(s) ===")
    cpu_stop = asyncio.Event()
    replay_stop = asyncio.Event()
    cpu_task = asyncio.create_task(_cpu_sampler(pid, cpu_stop))

    replay_tasks = [asyncio.create_task(_replay_source(replay_stop)) for _ in range(num_concurrent_replays)]
    await asyncio.sleep(duration)
    cpu_stop.set()
    replay_stop.set()
    samples = await cpu_task
    for t in replay_tasks:
        t.cancel()

    if samples:
        print(f"cpu%% samples over {duration}s: min={min(samples):.1f} "
              f"avg={statistics.mean(samples):.1f} max={max(samples):.1f}")
        print(f"approx cpu%% per stream: {statistics.mean(samples) / max(num_concurrent_replays, 1):.1f}")


async def run_consumer_scaling(consumer_counts: list[int], duration: float = 5.0) -> None:
    print(f"\n=== Consumer scaling ({consumer_counts}) ===")
    for n in consumer_counts:
        replay_stop = asyncio.Event()
        replay_task = asyncio.create_task(_replay_source(replay_stop))
        results = await asyncio.gather(*(_consumer_drain(duration) for _ in range(n)))
        replay_stop.set()
        replay_task.cancel()
        if results:
            print(f"consumers={n:>3}  packets/consumer min={min(results)} "
                  f"avg={statistics.mean(results):.1f} max={max(results)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", type=int, required=True, help="PID of the running uvicorn worker process")
    parser.add_argument("--frames", type=int, default=200, help="frames for latency probe")
    parser.add_argument("--consumers", type=str, default="1,5,10,25,50", help="comma-separated consumer counts to test")
    parser.add_argument("--cpu-streams", type=int, default=4, help="concurrent replay streams for CPU test")
    args = parser.parse_args()

    consumer_counts = [int(x) for x in args.consumers.split(",")]

    asyncio.run(run_latency_measurement(args.frames))
    asyncio.run(run_cpu_measurement(args.pid, args.cpu_streams))
    asyncio.run(run_consumer_scaling(consumer_counts))


if __name__ == "__main__":
    main()
