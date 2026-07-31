"""FastAPI entrypoint for the Voice Ingestion Worker.

Wires the IngestionWorker singleton to the three ingestion endpoints and
the single outbound consumer endpoint.
"""

from __future__ import annotations

import logging
import wave
from pathlib import Path

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from src.encoder import AudioPipeline
from src.engine import IngestionWorker
from src.inputs import ingest_file_replay, ingest_http_stream, ingest_websocket

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DEFAULT_REPLAY_FILE = DATA_DIR / "sample.wav"

app = FastAPI(title="Voice Ingestion Worker")
worker = IngestionWorker()


def _replay_pipeline(file_path: Path) -> AudioPipeline:
    """Builds an AudioPipeline matched to a WAV file's actual sample rate
    and channel count, so the replay source is normalized correctly
    regardless of the file's native format.
    """
    with wave.open(str(file_path), "rb") as wav:
        return AudioPipeline(source_rate=wav.getframerate(), source_channels=wav.getnchannels())


@app.get("/")
async def health_check() -> JSONResponse:
    return JSONResponse({"status": "ok", "consumers": worker.consumer_count})


@app.websocket("/ingest/live")
async def ingest_live(websocket: WebSocket) -> None:
    """Realtime packetized audio input."""
    pipeline = AudioPipeline()
    await ingest_websocket(websocket, worker, pipeline)


@app.post("/ingest/stream")
async def ingest_stream(request: Request) -> JSONResponse:
    """HTTP chunked streaming audio input."""
    pipeline = AudioPipeline()
    await ingest_http_stream(request, worker, pipeline)
    return JSONResponse({"status": "accepted"})


@app.post("/ingest/replay")
async def ingest_replay() -> JSONResponse:
    """Triggers the file replay engine (hardcoded to data/sample.wav)."""
    pipeline = _replay_pipeline(DEFAULT_REPLAY_FILE)
    await ingest_file_replay(str(DEFAULT_REPLAY_FILE), worker, pipeline)
    return JSONResponse({"status": "replay finished", "file": str(DEFAULT_REPLAY_FILE)})


@app.websocket("/stream")
async def stream(websocket: WebSocket) -> None:
    """Outbound endpoint: consumers attach here to receive the fan-out stream."""
    await websocket.accept()
    try:
        async for packet in worker.attach_consumer():
            await websocket.send_bytes(packet)
    except WebSocketDisconnect:
        logger.info("consumer disconnected")
