"""The three ingestion handlers.

Each handler is responsible for pulling raw audio from a materially
different source and handing normalized PCM frames off to the encoder
pipeline. All three converge on the same IngestionWorker.broadcast call.

Sources:
    1. LiveWebSocketInput  -- realtime packetized audio pushed over a WS.
    2. HttpChunkedStreamInput -- HTTP chunked-transfer audio upload.
    3. FileReplayInput -- replays a local file (e.g. data/sample.wav) for
       testing/demo purposes at real-time pacing.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import AsyncIterator

from src.encoder import AudioNormalizer, OpusEncoder
from src.engine import IngestionWorker

logger = logging.getLogger(__name__)


class LiveWebSocketInput:
    """Ingests realtime packetized audio from a WebSocket connection."""

    def __init__(self, worker: IngestionWorker) -> None:
        self._worker = worker
        self._normalizer = AudioNormalizer()
        self._encoder = OpusEncoder()

    async def run(self, packets: AsyncIterator[bytes]) -> None:
        async for raw_packet in packets:
            # TODO: decode raw_packet -> PCM, normalize, encode to Opus,
            # wrap in RFC 2198, then self._worker.broadcast(rtp_packet)
            logger.debug("live packet received: %d bytes", len(raw_packet))


class HttpChunkedStreamInput:
    """Ingests audio from an HTTP chunked-transfer request body."""

    def __init__(self, worker: IngestionWorker) -> None:
        self._worker = worker
        self._normalizer = AudioNormalizer()
        self._encoder = OpusEncoder()

    async def run(self, chunks: AsyncIterator[bytes]) -> None:
        async for chunk in chunks:
            # TODO: decode chunk -> PCM, normalize, encode to Opus,
            # wrap in RFC 2198, then self._worker.broadcast(rtp_packet)
            logger.debug("http chunk received: %d bytes", len(chunk))


class FileReplayInput:
    """Replays a local audio file at real-time pacing for testing/demo."""

    def __init__(self, worker: IngestionWorker, file_path: Path) -> None:
        self._worker = worker
        self._file_path = file_path
        self._normalizer = AudioNormalizer()
        self._encoder = OpusEncoder()

    async def run(self) -> None:
        if not self._file_path.exists():
            raise FileNotFoundError(f"replay source not found: {self._file_path}")
        # TODO: open self._file_path with PyAV, decode frames, normalize,
        # encode to Opus, wrap in RFC 2198, paced via asyncio.sleep to
        # real-time, then self._worker.broadcast(rtp_packet)
        logger.debug("replay started for %s", self._file_path)
        await asyncio.sleep(0)
