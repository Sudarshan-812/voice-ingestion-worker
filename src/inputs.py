"""The three ingestion handlers.

Each handler pulls raw audio from a materially different source, feeds it
through an AudioPipeline (normalize -> encode -> RFC 2198 wrap), and
broadcasts the resulting packets on the shared IngestionWorker.

Sources:
    1. ingest_websocket   -- realtime packetized audio pushed over a WS.
    2. ingest_http_stream -- HTTP chunked-transfer audio upload.
    3. ingest_file_replay -- replays a WAV file (e.g. data/sample.wav) for
       testing/demo purposes, throttled to real-time pacing.
"""

from __future__ import annotations

import asyncio
import logging
import wave
from pathlib import Path

from fastapi import Request, WebSocket, WebSocketDisconnect

from src.encoder import AudioPipeline
from src.engine import IngestionWorker

logger = logging.getLogger(__name__)

REPLAY_CHUNK_BYTES = 4096


async def ingest_websocket(websocket: WebSocket, worker: IngestionWorker, pipeline: AudioPipeline) -> None:
    """Realtime packetized audio pushed over a WebSocket connection."""
    await websocket.accept()
    try:
        while True:
            chunk = await websocket.receive_bytes()
            for packet in pipeline.process_chunk(chunk):
                worker.broadcast(packet)
    except WebSocketDisconnect:
        logger.info("live ingest connection closed")


async def ingest_http_stream(request: Request, worker: IngestionWorker, pipeline: AudioPipeline) -> None:
    """Audio uploaded via an HTTP chunked-transfer request body."""
    async for chunk in request.stream():
        for packet in pipeline.process_chunk(chunk):
            worker.broadcast(packet)


async def ingest_file_replay(file_path: str, worker: IngestionWorker, pipeline: AudioPipeline) -> None:
    """Replays a WAV file into the worker at real-time pacing.

    Reads ~REPLAY_CHUNK_BYTES-sized chunks and, after broadcasting each one,
    sleeps for exactly the amount of audio the chunk represents
    (frames / sample_rate). This paces the feed to wall-clock real-time
    speed regardless of how fast the file could be read off disk.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"replay source not found: {path}")

    with wave.open(str(path), "rb") as wav:
        if wav.getsampwidth() != 2:
            raise ValueError(f"expected 16-bit PCM, got {wav.getsampwidth() * 8}-bit")

        sample_rate = wav.getframerate()
        bytes_per_frame = wav.getnchannels() * wav.getsampwidth()
        frames_per_chunk = max(1, REPLAY_CHUNK_BYTES // bytes_per_frame)

        while True:
            raw_chunk = wav.readframes(frames_per_chunk)
            if not raw_chunk:
                break

            for packet in pipeline.process_chunk(raw_chunk):
                worker.broadcast(packet)

            num_frames = len(raw_chunk) // bytes_per_frame
            await asyncio.sleep(num_frames / sample_rate)
