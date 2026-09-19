from __future__ import annotations

import json
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class Span:
    trace_id: str
    span_id: str
    name: str
    started_at: str
    duration_ms: float = 0.0
    status: str = "running"
    attributes: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


class TraceStore:
    """Tiny OpenTelemetry-shaped store used by the local demo.

    Production deployments can swap this for an OTLP exporter without changing
    the workflow or ranking code.
    """

    def __init__(self, max_spans: int = 500) -> None:
        self.max_spans = max_spans
        self._spans: list[Span] = []
        self._lock = threading.Lock()
        self._counters: dict[str, int] = {}
        self._gauges: dict[str, float] = {}

    @contextmanager
    def span(
        self,
        name: str,
        *,
        trace_id: str | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> Iterator[Span]:
        started = time.perf_counter()
        item = Span(
            trace_id=trace_id or uuid.uuid4().hex,
            span_id=uuid.uuid4().hex[:16],
            name=name,
            started_at=utc_now(),
            attributes=attributes or {},
        )
        try:
            yield item
            item.status = "ok"
        except Exception as exc:
            item.status = "error"
            item.error = f"{type(exc).__name__}: {exc}"
            self.increment("errors_total")
            raise
        finally:
            item.duration_ms = round((time.perf_counter() - started) * 1000, 3)
            with self._lock:
                self._spans.append(item)
                if len(self._spans) > self.max_spans:
                    self._spans = self._spans[-self.max_spans :]
            self.increment("spans_total")

    def increment(self, name: str, value: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + value

    def gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = round(float(value), 4)

    def snapshot(self, limit: int = 80) -> dict[str, Any]:
        with self._lock:
            spans = [asdict(span) for span in self._spans[-limit:]]
            counters = dict(self._counters)
            gauges = dict(self._gauges)
        latency = [span["duration_ms"] for span in spans if span["status"] == "ok"]
        latency.sort()
        p95 = latency[min(len(latency) - 1, int(len(latency) * 0.95))] if latency else 0.0
        return {
            "generated_at": utc_now(),
            "counters": counters,
            "gauges": gauges,
            "latency_ms": {
                "p50": latency[len(latency) // 2] if latency else 0.0,
                "p95": p95,
                "max": max(latency, default=0.0),
            },
            "recent_spans": list(reversed(spans)),
        }

    def json_lines(self, limit: int = 80) -> str:
        return "\n".join(json.dumps(row, ensure_ascii=False) for row in self.snapshot(limit)["recent_spans"])
