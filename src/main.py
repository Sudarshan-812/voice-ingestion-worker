"""FastAPI entrypoint for the Voice Ingestion Worker.

Wires the IngestionWorker singleton to the three ingestion endpoints and
the single outbound consumer endpoint.
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from src.engine import IngestionWorker
from src.inputs import FileReplayInput, HttpChunkedStreamInput, LiveWebSocketInput

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DEFAULT_REPLAY_FILE = DATA_DIR / "sample.wav"

app = FastAPI(title="Voice Ingestion Worker")
worker = IngestionWorker()


@app.get("/")
async def health_check() -> JSONResponse:
    return JSONResponse({"status": "ok", "consumers": worker.consumer_count})


@app.websocket("/ingest/live")
async def ingest_live(websocket: WebSocket) -> None:
    """Realtime packetized audio input."""
    await websocket.accept()
    handler = LiveWebSocketInput(worker)

    async def packets():
        try:
            while True:
                yield await websocket.receive_bytes()
        except WebSocketDisconnect:
            return

    try:
        await handler.run(packets())
    finally:
        logger.info("live ingest connection closed")


@app.post("/ingest/stream")
async def ingest_stream(request: Request) -> JSONResponse:
    """HTTP chunked streaming audio input."""
    handler = HttpChunkedStreamInput(worker)
    await handler.run(request.stream())
    return JSONResponse({"status": "accepted"})


@app.post("/ingest/replay")
async def ingest_replay(file_path: str | None = None) -> JSONResponse:
    """Triggers the file replay engine (defaults to data/sample.wav)."""
    target = Path(file_path) if file_path else DEFAULT_REPLAY_FILE
    handler = FileReplayInput(worker, target)
    await handler.run()
    return JSONResponse({"status": "replay started", "file": str(target)})


@app.websocket("/stream")
async def stream(websocket: WebSocket) -> None:
    """Outbound endpoint: consumers attach here to receive the fan-out stream."""
    await websocket.accept()
    try:
        async for packet in worker.attach_consumer():
            await websocket.send_bytes(packet)
    except WebSocketDisconnect:
        logger.info("consumer disconnected")
