"""In-process asyncio pub/sub event bus, with an optional Redis fan-out
for cross-process consumers (e.g. a separate dashboard or logger process).

The bus is intentionally simple: a single asyncio.Queue drives an ordered
dispatch loop so that event handling within a process is deterministic and
race-free, which matters for both live trading and backtest replay (the
backtester reuses this exact class to get identical event-routing semantics
between sim and live).
"""
from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from config.logging_config import get_logger
from core.events import Event, EventType

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = get_logger(__name__)

Handler = Callable[[Event], Awaitable[None]]


class EventBus:
    def __init__(self, *, redis_url: str | None = None, queue_maxsize: int = 100_000) -> None:
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=queue_maxsize)
        self._subscribers: dict[EventType, list[Handler]] = defaultdict(list)
        self._global_subscribers: list[Handler] = []
        self._redis_url = redis_url
        self._redis: "aioredis.Redis | None" = None
        self._running = False
        self._dispatch_task: asyncio.Task[None] | None = None
        self._published_count = 0
        self._dropped_count = 0

    async def start(self) -> None:
        if self._redis_url:
            import redis.asyncio as aioredis

            self._redis = aioredis.from_url(self._redis_url)
        self._running = True
        self._dispatch_task = asyncio.create_task(self._dispatch_loop(), name="event-bus-dispatch")
        logger.info("event_bus.started", redis_enabled=bool(self._redis))

    async def is_redis_connected(self) -> bool:
        """Live PING check, not just whether a client was constructed at
        startup -- lets callers (e.g. the dashboard status bar) reflect
        Redis going down mid-session rather than trusting a stale flag."""
        if self._redis is None:
            return False
        try:
            return bool(await self._redis.ping())
        except Exception:
            return False

    async def stop(self) -> None:
        self._running = False
        if self._dispatch_task:
            await self._queue.join()
            self._dispatch_task.cancel()
            try:
                await self._dispatch_task
            except asyncio.CancelledError:
                pass
        if self._redis:
            await self._redis.aclose()
        logger.info(
            "event_bus.stopped",
            published=self._published_count,
            dropped=self._dropped_count,
        )

    def subscribe(self, event_type: EventType, handler: Handler) -> None:
        self._subscribers[event_type].append(handler)

    def subscribe_all(self, handler: Handler) -> None:
        self._global_subscribers.append(handler)

    async def publish(self, event: Event) -> None:
        try:
            self._queue.put_nowait(event)
            self._published_count += 1
        except asyncio.QueueFull:
            self._dropped_count += 1
            logger.error("event_bus.queue_full_dropped_event", event_type=event.event_type.name)

    async def publish_and_wait(self, event: Event) -> None:
        """Publish and block until the queue has room (backpressure-aware)."""
        await self._queue.put(event)
        self._published_count += 1

    async def _dispatch_loop(self) -> None:
        while self._running:
            event = await self._queue.get()
            try:
                await self._dispatch(event)
            except Exception:
                logger.exception("event_bus.handler_error", event_type=event.event_type.name)
            finally:
                self._queue.task_done()

    async def _dispatch(self, event: Event) -> None:
        handlers = self._subscribers.get(event.event_type, ())
        for handler in (*handlers, *self._global_subscribers):
            await handler(event)
        if self._redis:
            await self._publish_redis(event)

    async def _publish_redis(self, event: Event) -> None:
        import dataclasses

        import orjson

        channel = f"events:{event.event_type.name.lower()}"
        payload = orjson.dumps(dataclasses.asdict(event), default=str)
        await self._redis.publish(channel, payload)

    async def join(self) -> None:
        """Block until every event published so far (including ones
        published by handlers while draining) has been fully dispatched.
        Used by the backtest engine to deterministically settle all
        causal effects of one bar (signal -> risk check -> order submission)
        before advancing to the next bar."""
        await self._queue.join()

    @property
    def queue_depth(self) -> int:
        return self._queue.qsize()
