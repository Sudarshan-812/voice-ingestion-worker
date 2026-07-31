"""Core asyncio fan-out worker.

A single IngestionWorker instance owns the outbound stream. Producers
(the three ingestion handlers in inputs.py) push encoded RFC 2198 packets
into the worker via `broadcast`. Consumers (WebSocket clients on /stream)
attach a bounded queue via `attach_consumer` and read from it.

Backpressure policy: queues are bounded (maxsize=100) and `broadcast` uses
put_nowait(). A slow or stalled consumer simply drops frames instead of
blocking the worker or any other consumer.
"""

from __future__ import annotations

import asyncio
import logging
from typing import AsyncIterator

logger = logging.getLogger(__name__)

CONSUMER_QUEUE_MAXSIZE = 100


class IngestionWorker:
    """Fans out a single outbound packet stream to N independent consumers."""

    def __init__(self, queue_maxsize: int = CONSUMER_QUEUE_MAXSIZE) -> None:
        self._queue_maxsize = queue_maxsize
        self._consumers: set[asyncio.Queue[bytes]] = set()

    async def attach_consumer(self) -> AsyncIterator[bytes]:
        """Register a new bounded queue and yield packets from it.

        The queue is automatically detached when the consumer stops
        iterating (e.g. the client disconnects and the generator is closed).
        """
        queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=self._queue_maxsize)
        self._consumers.add(queue)
        try:
            while True:
                packet = await queue.get()
                yield packet
        finally:
            self._consumers.discard(queue)

    def broadcast(self, packet: bytes) -> None:
        """Push a packet to every attached consumer without blocking.

        If a consumer's queue is full, the packet is dropped for that
        consumer only -- this method never raises and never awaits.
        """
        for queue in self._consumers:
            try:
                queue.put_nowait(packet)
            except asyncio.QueueFull:
                # Drop policy: a slow consumer must never stall the worker
                # or any other consumer.
                pass

    @property
    def consumer_count(self) -> int:
        return len(self._consumers)
