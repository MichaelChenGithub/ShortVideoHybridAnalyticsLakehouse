"""Event sinks for dry-run and Kafka emission."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from .constants import TOPIC_CDC_VIDEOS, TOPIC_CONTENT_EVENTS


@dataclass
class EmittedRecord:
    topic: str
    key: str
    value: Dict[str, Any]
    emitted_at: datetime


class EventSink:
    mode: str

    def emit_content_event(self, key: str, value: Dict[str, Any], emitted_at: datetime) -> None:
        raise NotImplementedError

    def emit_cdc_event(self, key: str, value: Dict[str, Any], emitted_at: datetime) -> None:
        raise NotImplementedError

    def flush(self) -> None:
        raise NotImplementedError


class InMemoryEventSink(EventSink):
    mode = "dry-run"

    def __init__(self) -> None:
        self.content_events: List[EmittedRecord] = []
        self.cdc_events: List[EmittedRecord] = []

    def emit_content_event(self, key: str, value: Dict[str, Any], emitted_at: datetime) -> None:
        self.content_events.append(EmittedRecord(TOPIC_CONTENT_EVENTS, key, value, emitted_at))

    def emit_cdc_event(self, key: str, value: Dict[str, Any], emitted_at: datetime) -> None:
        self.cdc_events.append(EmittedRecord(TOPIC_CDC_VIDEOS, key, value, emitted_at))

    def flush(self) -> None:
        return


class KafkaEventSink(EventSink):
    mode = "kafka"

    def __init__(
        self,
        bootstrap_servers: str,
        content_topic: str = TOPIC_CONTENT_EVENTS,
        cdc_topic: str = TOPIC_CDC_VIDEOS,
    ) -> None:
        try:
            from confluent_kafka import Producer  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "confluent_kafka is required for --sink kafka. "
                "Install with `pip install confluent-kafka`."
            ) from exc

        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "m1-bounded-run-generator",
                "linger.ms": 10,
                "compression.type": "lz4",
            }
        )
        self._content_topic = content_topic
        self._cdc_topic = cdc_topic
        self._delivery_errors: List[str] = []

    def _delivery_callback(self, err: Optional[Exception], msg: Any) -> None:
        if err is not None:
            self._delivery_errors.append(str(err))

    def _produce(self, topic: str, key: str, value: Dict[str, Any]) -> None:
        self._producer.produce(
            topic=topic,
            key=key,
            value=json.dumps(value, separators=(",", ":")),
            on_delivery=self._delivery_callback,
        )
        self._producer.poll(0)

    def emit_content_event(self, key: str, value: Dict[str, Any], emitted_at: datetime) -> None:
        del emitted_at
        self._produce(self._content_topic, key, value)

    def emit_cdc_event(self, key: str, value: Dict[str, Any], emitted_at: datetime) -> None:
        del emitted_at
        self._produce(self._cdc_topic, key, value)

    def flush(self) -> None:
        self._producer.flush()
        if self._delivery_errors:
            raise RuntimeError(
                "Kafka delivery failed for one or more messages: "
                + "; ".join(self._delivery_errors[:3])
            )
