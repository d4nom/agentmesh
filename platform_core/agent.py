from __future__ import annotations

import asyncio
import contextlib
import signal
from pathlib import Path

from opentelemetry import context as context_api

from platform_core import bus
from platform_core.bus import connect, ensure_streams, pull_subscribe
from platform_core.config import AgentConfig
from platform_core.envelope import Envelope, new_id
from platform_core.observability import (
    extract_context,
    get_logger,
    get_tracer,
    inject_traceparent,
)
from platform_core.stores import is_already_processed, make_qdrant, make_redis, mark_processed

DEFAULT_MAX_DELIVER = 5
DEFAULT_ACK_WAIT = 30
HEARTBEAT_INTERVAL_SECONDS = 10
HEALTHY_MARKER = Path("/tmp/healthy")


class BaseAgent:
    """Common lifecycle: connect, subscribe, ack/nak, tracing, logging,
    heartbeat, idempotency, graceful shutdown. Agents override `handle`."""

    def __init__(self, config: AgentConfig) -> None:
        self.config = config
        self._role = config.subscribes.split(".", 1)[-1]
        self._log = get_logger(agent=config.name)
        self._stop_event = asyncio.Event()
        self._nc = None
        self._js = None
        self._redis = None
        self._qdrant = None
        self._sub = None

    async def run(self) -> None:
        self._nc, self._js = await connect(self.config.nats_url)
        self._redis = make_redis(self.config.redis_url)
        if "qdrant" in self.config.stores:
            self._qdrant = make_qdrant(self.config.qdrant_url)

        await ensure_streams(self._js)
        self._sub = await pull_subscribe(
            self._js,
            self.config.subscribes,
            durable=self.config.name,
            max_deliver=DEFAULT_MAX_DELIVER,
            ack_wait=DEFAULT_ACK_WAIT,
        )

        await self._publish_event("events.agent.started", {"agent": self.config.name})
        self._log.info("agent_started", subscribes=self.config.subscribes)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._stop_event.set)

        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        consume_task = asyncio.create_task(self._consume_loop())

        await self._stop_event.wait()
        self._log.info("shutdown_signal_received")

        await consume_task

        heartbeat_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat_task

        await self._publish_event("events.agent.stopped", {"agent": self.config.name})
        await self._nc.drain()
        self._log.info("agent_stopped")

    async def handle(self, env: Envelope) -> None:
        raise NotImplementedError

    async def publish(self, subject: str, type_: str, payload: dict, correlation_id: str) -> None:
        envelope = Envelope(
            sender=self.config.name,
            subject=subject,
            type=type_,
            correlation_id=correlation_id,
            traceparent=inject_traceparent(),
            payload=payload,
        )
        await bus.publish(self._js, subject, envelope)

    async def _publish_event(self, subject: str, payload: dict) -> None:
        await self.publish(subject, "event", payload, correlation_id=new_id())

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.to_thread(HEALTHY_MARKER.touch)
            try:
                await self._publish_event("events.heartbeat", {"agent": self.config.name})
            except Exception:
                self._log.warning("heartbeat_publish_failed", exc_info=True)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop_event.wait(), timeout=HEARTBEAT_INTERVAL_SECONDS)
            if self._stop_event.is_set():
                return

    async def _consume_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                msgs = await self._sub.fetch(1, timeout=1)
            except TimeoutError:
                continue
            except Exception:
                self._log.warning("fetch_failed", exc_info=True)
                continue
            for msg in msgs:
                await self._process(msg)

    async def _process(self, msg) -> None:
        envelope = Envelope.model_validate_json(msg.data)
        ctx = extract_context(envelope.traceparent)
        token = context_api.attach(ctx)
        tracer = get_tracer(f"agentmesh.{self.config.name}")
        try:
            with tracer.start_as_current_span(f"agent.{self.config.name}.handle") as span:
                span.set_attribute("message_id", envelope.message_id)
                span.set_attribute("correlation_id", envelope.correlation_id)
                log = get_logger(
                    agent=self.config.name,
                    message_id=envelope.message_id,
                    correlation_id=envelope.correlation_id,
                )

                if await is_already_processed(self._redis, envelope.message_id):
                    log.info("duplicate_message_skipped")
                    await msg.ack()
                    return

                try:
                    await asyncio.wait_for(self.handle(envelope), timeout=envelope.ttl_ms / 1000)
                except Exception as exc:
                    log.error("handle_failed", error=str(exc), exc_info=True)
                    await self._handle_failure(msg, envelope, exc, log)
                    return

                await mark_processed(self._redis, envelope.message_id)
                await msg.ack()
                log.info("handle_succeeded")
        finally:
            context_api.detach(token)

    async def _handle_failure(self, msg, envelope: Envelope, exc: Exception, log) -> None:
        num_delivered = msg.metadata.num_delivered
        if num_delivered >= DEFAULT_MAX_DELIVER:
            dlq_envelope = envelope.model_copy(
                update={
                    "message_id": new_id(),
                    "sender": self.config.name,
                    "subject": f"dlq.{self._role}",
                    "type": "error",
                    "traceparent": inject_traceparent(),
                    "payload": {**envelope.payload, "error": str(exc)},
                }
            )
            await bus.publish(self._js, f"dlq.{self._role}", dlq_envelope)
            await msg.term()
            log.error("dead_lettered", num_delivered=num_delivered)
        else:
            delay = min(2**num_delivered, 30)
            await msg.nak(delay=delay)
            log.warning("nak_redelivery_scheduled", num_delivered=num_delivered, delay=delay)
