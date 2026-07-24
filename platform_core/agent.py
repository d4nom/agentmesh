from __future__ import annotations

import asyncio
import contextlib
import json
import signal
from datetime import UTC, datetime
from pathlib import Path

from opentelemetry import context as context_api
from opentelemetry.trace import Status, StatusCode
from structlog.contextvars import bound_contextvars

from platform_core import bus
from platform_core.bus import (
    connect,
    durable_consumer_name,
    ensure_streams,
    pull_subscribe,
)
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
JETSTREAM_MAX_DELIVER = -1
DEFAULT_ACK_WAIT = 30
ACK_PROGRESS_INTERVAL_SECONDS = DEFAULT_ACK_WAIT / 3
MAX_CONSECUTIVE_FETCH_FAILURES = 5
MAX_CONSECUTIVE_HEARTBEAT_FAILURES = 5
MAX_FETCH_RETRY_DELAY_SECONDS = 10
HEARTBEAT_INTERVAL_SECONDS = 10
HEALTHY_MARKER = Path("/tmp/healthy")
MAX_DLQ_RAW_MESSAGE_CHARS = 4096
MAX_DLQ_ERROR_CHARS = 8192
MAX_DLQ_ORIGINAL_PAYLOAD_BYTES = 32 * 1024
MAX_DLQ_PAYLOAD_EXCERPT_CHARS = 32 * 1024
DLQ_HANDOFF_RETRY_DELAY_SECONDS = 30


def _bounded_error(exc: Exception) -> str:
    return str(exc)[:MAX_DLQ_ERROR_CHARS]


def _bounded_dead_letter_payload(envelope: Envelope, exc: Exception) -> dict:
    """Preserve small payloads and summarize large ones below NATS limits."""
    serialized = json.dumps(
        envelope.payload,
        ensure_ascii=True,
        separators=(",", ":"),
    )
    error = _bounded_error(exc)
    payload_size_bytes = len(serialized.encode())
    if payload_size_bytes <= MAX_DLQ_ORIGINAL_PAYLOAD_BYTES:
        return {
            **envelope.payload,
            "error": error,
            "error_stage": "handler",
        }
    return {
        "error": error,
        "error_stage": "handler",
        "original_message_id": envelope.message_id,
        "original_sender": envelope.sender,
        "original_subject": envelope.subject,
        "original_type": envelope.type,
        "payload_size_bytes": payload_size_bytes,
        "payload_truncated": True,
        "payload_excerpt": serialized[:MAX_DLQ_PAYLOAD_EXCERPT_CHARS],
    }


class BaseAgent:
    """Common lifecycle: connect, subscribe, ack/nak, tracing, logging,
    heartbeat, idempotency, graceful shutdown. Agents override `handle`."""

    def __init__(self, config: AgentConfig) -> None:
        self.config = config
        self._role = config.subscribes.split(".", 1)[-1]
        self._consumer_id = durable_consumer_name(config.name, config.subscribes)
        self._log = get_logger(agent=config.name)
        self._stop_event = asyncio.Event()
        self._nc = None
        self._js = None
        self._redis = None
        self._qdrant = None
        self._sub = None

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        registered_signals: list[signal.Signals] = []
        startup_tasks: list[asyncio.Task] = []
        started_event_published = False
        try:
            self._nc, self._js = await connect(self.config.nats_url)
            self._redis = make_redis(self.config.redis_url)
            if "qdrant" in self.config.stores:
                self._qdrant = make_qdrant(self.config.qdrant_url)

            await ensure_streams(self._js)
            self._sub = await pull_subscribe(
                self._js,
                self.config.subscribes,
                durable=self._consumer_id,
                max_deliver=JETSTREAM_MAX_DELIVER,
                ack_wait=DEFAULT_ACK_WAIT,
            )

            await self._publish_event("events.agent.started", {"agent": self.config.name})
            started_event_published = True
            self._log.info("agent_started", subscribes=self.config.subscribes)

            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, self._stop_event.set)
                registered_signals.append(sig)

            heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            startup_tasks.append(heartbeat_task)
            consume_task = asyncio.create_task(self._consume_loop())
            startup_tasks.append(consume_task)
            stop_task = asyncio.create_task(self._stop_event.wait())
            startup_tasks.append(stop_task)
        except BaseException:
            self._stop_event.set()
            for task in startup_tasks:
                task.cancel()
            for task in startup_tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            for sig in registered_signals:
                loop.remove_signal_handler(sig)
            await self._release_resources(
                publish_stopped=started_event_published,
            )
            raise

        try:
            done, _ = await asyncio.wait(
                {stop_task, consume_task, heartbeat_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_task not in done:
                # Both background tasks must live for the lifetime of the service.
                # Propagating either failure exits PID 1 so the container restart
                # policy can recover it instead of leaving a partial zombie.
                for background_task in (consume_task, heartbeat_task):
                    if background_task in done:
                        error = background_task.exception()
                        if error is not None:
                            raise error
                raise RuntimeError("agent background task stopped unexpectedly")
            else:
                self._log.info("shutdown_signal_received")
                await consume_task
        except Exception:
            self._log.error("agent_background_task_failed", exc_info=True)
            raise
        finally:
            self._stop_event.set()
            stop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stop_task

            if not consume_task.done():
                consume_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await consume_task

            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await heartbeat_task

            for sig in registered_signals:
                loop.remove_signal_handler(sig)

            await self._release_resources(publish_stopped=True)

    async def handle(self, env: Envelope) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        """Release resources owned by a concrete agent implementation."""
        return None

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
        consecutive_failures = 0
        while True:
            try:
                await self._publish_event("events.heartbeat", {"agent": self.config.name})
            except Exception as exc:
                consecutive_failures += 1
                self._log.warning(
                    "heartbeat_publish_failed",
                    consecutive_failures=consecutive_failures,
                    max_consecutive_failures=MAX_CONSECUTIVE_HEARTBEAT_FAILURES,
                    exc_info=True,
                )
                if consecutive_failures >= MAX_CONSECUTIVE_HEARTBEAT_FAILURES:
                    raise RuntimeError(
                        f"heartbeat publish failed {consecutive_failures} consecutive times"
                    ) from exc
            else:
                consecutive_failures = 0
                await asyncio.to_thread(HEALTHY_MARKER.touch)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop_event.wait(), timeout=HEARTBEAT_INTERVAL_SECONDS)
            if self._stop_event.is_set():
                return

    async def _consume_loop(self) -> None:
        consecutive_failures = 0
        while not self._stop_event.is_set():
            try:
                msgs = await self._sub.fetch(1, timeout=1)
            except TimeoutError:
                consecutive_failures = 0
                continue
            except Exception as exc:
                consecutive_failures += 1
                self._log.warning(
                    "fetch_failed",
                    consecutive_failures=consecutive_failures,
                    max_consecutive_failures=MAX_CONSECUTIVE_FETCH_FAILURES,
                    exc_info=True,
                )
                if consecutive_failures >= MAX_CONSECUTIVE_FETCH_FAILURES:
                    raise RuntimeError(
                        f"consumer fetch failed {consecutive_failures} consecutive times"
                    ) from exc
                await self._wait_before_fetch_retry(consecutive_failures)
                continue
            consecutive_failures = 0
            for msg in msgs:
                await self._process(msg)

    async def _process(self, msg) -> None:
        try:
            envelope = Envelope.model_validate_json(msg.data)
        except Exception as exc:
            await self._run_with_ack_progress(msg, self._handle_protocol_failure(msg, exc))
            return
        if envelope.subject != msg.subject:
            error = ValueError(
                f"envelope subject '{envelope.subject}' does not match "
                f"delivery subject '{msg.subject}'"
            )
            await self._run_with_ack_progress(msg, self._handle_protocol_failure(msg, error))
            return
        expected_type = bus.expected_envelope_type(msg.subject)
        if expected_type is not None and envelope.type != expected_type:
            error = ValueError(
                f"envelope type '{envelope.type}' does not match "
                f"delivery subject '{msg.subject}' (expected '{expected_type}')"
            )
            await self._run_with_ack_progress(msg, self._handle_protocol_failure(msg, error))
            return

        ctx = extract_context(envelope.traceparent)
        token = context_api.attach(ctx)
        tracer = get_tracer(f"agentmesh.{self.config.name}")
        try:
            with bound_contextvars(
                agent=self.config.name,
                message_id=envelope.message_id,
                correlation_id=envelope.correlation_id,
            ):
                with tracer.start_as_current_span(f"agent.{self.config.name}.handle") as span:
                    span.set_attribute("message_id", envelope.message_id)
                    span.set_attribute("correlation_id", envelope.correlation_id)
                    log = get_logger()

                    await self._run_with_ack_progress(
                        msg,
                        self._process_valid_envelope(msg, envelope, span, log),
                    )
        finally:
            context_api.detach(token)

    async def _process_valid_envelope(self, msg, envelope: Envelope, span, log) -> None:
        try:
            already_processed = await is_already_processed(
                self._redis,
                self._consumer_id,
                envelope.message_id,
            )
            if not already_processed:
                async with asyncio.timeout(envelope.ttl_ms / 1000):
                    await self.handle(envelope)
                await mark_processed(
                    self._redis,
                    self._consumer_id,
                    envelope.message_id,
                )
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, _bounded_error(exc)))
            log.error(
                "message_processing_failed",
                error=_bounded_error(exc),
                exc_info=True,
            )
            await self._handle_failure(msg, envelope, exc, log)
            return

        if already_processed:
            log.info("duplicate_message_skipped")

        if not await self._ack_processed_message(msg, span, log):
            return

        if not already_processed:
            log.info(
                "handle_succeeded",
                num_delivered=msg.metadata.num_delivered,
            )

    async def _ack_processed_message(self, msg, span, log) -> bool:
        """Ack completed work without dead-lettering on an ack transport error.

        A Redis marker already makes a later delivery safe. If this ack is
        lost, leave the message unsettled: redelivery repeats only the marker
        check and ack instead of treating successful work as poison.
        """
        try:
            await msg.ack()
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            log.warning(
                "message_ack_failed_redelivery_expected",
                error=str(exc),
                exc_info=True,
            )
            return False
        return True

    async def _handle_protocol_failure(self, msg, exc: Exception) -> None:
        """Retry malformed protocol messages, then preserve them in the DLQ.

        There is no trustworthy correlation_id before Envelope validation, so
        protocol-level dead letters receive a fresh one and carry a bounded raw
        representation for diagnosis.
        """
        num_delivered = msg.metadata.num_delivered
        log = get_logger(agent=self.config.name)
        log.error(
            "envelope_validation_failed",
            error=_bounded_error(exc),
            num_delivered=num_delivered,
        )

        if num_delivered >= DEFAULT_MAX_DELIVER:
            if msg.subject.startswith("dlq."):
                await msg.term()
                log.error(
                    "dead_letter_processing_abandoned",
                    num_delivered=num_delivered,
                    subject=msg.subject,
                )
                return

            raw_message = msg.data.decode(errors="replace")[:MAX_DLQ_RAW_MESSAGE_CHARS]
            subject = f"dlq.{self._role}"
            dlq_envelope = Envelope(
                sender=self.config.name,
                subject=subject,
                type="error",
                correlation_id=new_id(),
                traceparent=inject_traceparent(),
                payload={
                    "error": _bounded_error(exc),
                    "error_stage": "envelope_validation",
                    "raw_message": raw_message,
                },
            )
            if await self._publish_dead_letter_or_retry(
                msg,
                subject,
                dlq_envelope,
                log,
            ):
                log.error("dead_lettered", num_delivered=num_delivered, subject=subject)
            return

        delay = min(2**num_delivered, 30)
        await msg.nak(delay=delay)
        log.warning("nak_redelivery_scheduled", num_delivered=num_delivered, delay=delay)

    async def _handle_failure(self, msg, envelope: Envelope, exc: Exception, log) -> None:
        num_delivered = msg.metadata.num_delivered
        if num_delivered >= DEFAULT_MAX_DELIVER:
            if msg.subject.startswith("dlq."):
                await msg.term()
                log.error(
                    "dead_letter_processing_abandoned",
                    num_delivered=num_delivered,
                    subject=msg.subject,
                )
                return

            subject = f"dlq.{self._role}"
            dlq_envelope = envelope.model_copy(
                update={
                    "message_id": new_id(),
                    "sender": self.config.name,
                    "subject": subject,
                    "type": "error",
                    "traceparent": inject_traceparent(),
                    "created_at": datetime.now(UTC),
                    "payload": _bounded_dead_letter_payload(envelope, exc),
                }
            )
            if await self._publish_dead_letter_or_retry(
                msg,
                subject,
                dlq_envelope,
                log,
            ):
                log.error("dead_lettered", num_delivered=num_delivered, subject=subject)
        else:
            delay = min(2**num_delivered, 30)
            await msg.nak(delay=delay)
            log.warning("nak_redelivery_scheduled", num_delivered=num_delivered, delay=delay)

    async def _publish_dead_letter_or_retry(
        self,
        msg,
        subject: str,
        envelope: Envelope,
        log,
    ) -> bool:
        """Publish before terminating; explicitly reschedule if handoff fails."""
        try:
            await bus.publish(self._js, subject, envelope)
        except Exception:
            log.error(
                "dead_letter_publish_failed",
                subject=subject,
                retry_delay=DLQ_HANDOFF_RETRY_DELAY_SECONDS,
                exc_info=True,
            )
            await msg.nak(delay=DLQ_HANDOFF_RETRY_DELAY_SECONDS)
            log.warning(
                "dead_letter_handoff_retry_scheduled",
                subject=subject,
                delay=DLQ_HANDOFF_RETRY_DELAY_SECONDS,
            )
            return False

        await msg.term()
        return True

    async def _run_with_ack_progress(self, msg, operation) -> None:
        """Keep the ack lease alive through Redis, handler and settlement.

        The operation owns business retry/DLQ decisions. A lease-extension
        failure is transport-level: cancel the operation and let the supervised
        consumer exit so JetStream can redeliver after connectivity recovers.
        """
        operation_task = asyncio.create_task(operation)
        progress_task = asyncio.create_task(self._ack_progress_loop(msg))
        try:
            done, _ = await asyncio.wait(
                {operation_task, progress_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if operation_task in done:
                await operation_task
                return

            error = progress_task.exception()
            if error is not None:
                raise RuntimeError("failed to extend message ack deadline") from error
            raise RuntimeError("message ack progress loop stopped unexpectedly")
        finally:
            tasks_to_cancel = [task for task in (operation_task, progress_task) if not task.done()]
            for task in tasks_to_cancel:
                task.cancel()
            for task in tasks_to_cancel:
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def _ack_progress_loop(self, msg) -> None:
        while True:
            await msg.in_progress()
            await asyncio.sleep(ACK_PROGRESS_INTERVAL_SECONDS)

    async def _wait_before_fetch_retry(self, consecutive_failures: int) -> None:
        delay = min(2 ** (consecutive_failures - 1), MAX_FETCH_RETRY_DELAY_SECONDS)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay)

    async def _close_stores(self) -> None:
        if self._qdrant is not None:
            try:
                await self._qdrant.close()
            except Exception:
                self._log.warning("qdrant_close_failed", exc_info=True)
        if self._redis is not None:
            try:
                await self._redis.aclose()
            except Exception:
                self._log.warning("redis_close_failed", exc_info=True)

    async def _release_resources(self, *, publish_stopped: bool) -> None:
        if publish_stopped:
            try:
                await self._publish_event(
                    "events.agent.stopped",
                    {"agent": self.config.name},
                )
            except Exception:
                self._log.warning("agent_stopped_event_publish_failed", exc_info=True)
        try:
            await self.close()
        except Exception:
            self._log.warning("agent_resource_close_failed", exc_info=True)
        await self._close_stores()
        if self._nc is not None:
            try:
                await self._nc.drain()
            except Exception:
                self._log.warning("nats_drain_failed", exc_info=True)
                with contextlib.suppress(Exception):
                    await self._nc.close()
        with contextlib.suppress(OSError):
            await asyncio.to_thread(HEALTHY_MARKER.unlink, missing_ok=True)
        self._log.info("agent_stopped")
