# Voice Ingestion Worker

## Overview

## Architecture

## Ingestion Sources

## Outbound Stream / Consumer Contract

## Running Locally

## Building the Container

## Configuration

## Operational Notes

## Testing Consumers

`scripts/mock_consumer.py` is a mock transcriber: it opens several concurrent
WebSocket connections to `/stream`, each with a staggered start time and
connected duration, so it exercises attach/detach churn against
`IngestionWorker` rather than a single steady connection. Each consumer also
unwraps the RFC 2198 payload back to the primary Opus frame and feeds it to
`opuslib.Decoder`, proving the encode -> wrap -> broadcast -> unwrap -> decode
round trip is byte-correct, not just that packets arrive.

To run it:

1. Start the worker: `uvicorn src.main:app --reload`
2. Kick off an ingestion source so there's something on the stream, e.g. the
   replay endpoint: `curl -X POST http://localhost:8000/ingest/replay`
3. In a second terminal, run the mock consumers: `python scripts/mock_consumer.py`

Watch the logs for each consumer's connect/decode/disconnect lifecycle, and
confirm `GET /` shows the expected `consumers` count rising and falling as
individual mock consumers attach and detach.
