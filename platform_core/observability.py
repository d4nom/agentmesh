from __future__ import annotations

import logging

import structlog
from opentelemetry import propagate, trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def _grpc_endpoint(endpoint: str) -> str:
    """OTLPSpanExporter's grpc channel wants host:port, not a URL."""
    return endpoint.removeprefix("http://").removeprefix("https://")


def _add_trace_id(logger: object, method_name: str, event_dict: dict) -> dict:
    span_context = trace.get_current_span().get_span_context()
    if span_context.is_valid:
        event_dict["trace_id"] = format(span_context.trace_id, "032x")
    return event_dict


def init_observability(service_name: str, otel_endpoint: str) -> None:
    resource = Resource.create({SERVICE_NAME: service_name})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=_grpc_endpoint(otel_endpoint), insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            _add_trace_id,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_tracer(name: str) -> trace.Tracer:
    return trace.get_tracer(name)


def get_logger(**bound: object) -> structlog.typing.FilteringBoundLogger:
    return structlog.get_logger().bind(**bound)


def inject_traceparent() -> str | None:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier.get("traceparent")


def extract_context(traceparent: str | None) -> Context:
    if not traceparent:
        return Context()
    return propagate.extract({"traceparent": traceparent})
