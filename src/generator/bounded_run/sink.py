"""Event sinks for dry-run and Kafka emission."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from .constants import TOPIC_CDC_USERS, TOPIC_CDC_VIDEOS, TOPIC_CONTENT_EVENTS


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

    def emit_video_cdc_event(self, key: str, value: Dict[str, Any], emitted_at: datetime) -> None:
        raise NotImplementedError

    def emit_user_cdc_event(self, key: str, value: Dict[str, Any], emitted_at: datetime) -> None:
        raise NotImplementedError

    def flush(self) -> None:
        raise NotImplementedError


class InMemoryEventSink(EventSink):
    mode = "dry-run"

    def __init__(self) -> None:
        self.content_events: List[EmittedRecord] = []
        self.video_cdc_events: List[EmittedRecord] = []
        self.user_cdc_events: List[EmittedRecord] = []

    def emit_content_event(self, key: str, value: Dict[str, Any], emitted_at: datetime) -> None:
        self.content_events.append(EmittedRecord(TOPIC_CONTENT_EVENTS, key, value, emitted_at))

    def emit_video_cdc_event(self, key: str, value: Dict[str, Any], emitted_at: datetime) -> None:
        self.video_cdc_events.append(EmittedRecord(TOPIC_CDC_VIDEOS, key, value, emitted_at))

    def emit_user_cdc_event(self, key: str, value: Dict[str, Any], emitted_at: datetime) -> None:
        self.user_cdc_events.append(EmittedRecord(TOPIC_CDC_USERS, key, value, emitted_at))

    def flush(self) -> None:
        return


class KafkaEventSink(EventSink):
    mode = "kafka"

    def __init__(
        self,
        bootstrap_servers: str,
        content_topic: str = TOPIC_CONTENT_EVENTS,
        video_cdc_topic: str = TOPIC_CDC_VIDEOS,
        user_cdc_topic: str = TOPIC_CDC_USERS,
        use_iam_auth: bool = False,
        aws_region: str = "us-east-1",
    ) -> None:
        try:
            from confluent_kafka import Producer  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "confluent_kafka is required for --sink kafka. "
                "Install with `pip install confluent-kafka`."
            ) from exc

        producer_config: dict = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "benchmark-generator",
            "linger.ms": 10,
            "compression.type": "lz4",
        }

        if use_iam_auth:
            try:
                from aws_msk_iam_sasl_signer import MSKAuthTokenProvider  # type: ignore
            except ImportError as exc:
                raise RuntimeError(
                    "aws-msk-iam-sasl-signer-python is required for MSK IAM auth. "
                    "Install with `pip install aws-msk-iam-sasl-signer-python`."
                ) from exc

            _region = aws_region

            def _oauth_cb(config: dict) -> tuple:
                token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(_region)
                return token, expiry_ms / 1000.0

            producer_config.update(
                {
                    "security.protocol": "SASL_SSL",
                    "sasl.mechanisms": "OAUTHBEARER",
                    "oauth_cb": _oauth_cb,
                    "ssl.ca.location": "/etc/ssl/certs/ca-certificates.crt",
                }
            )

        self._producer = Producer(producer_config)
        self._content_topic = content_topic
        self._video_cdc_topic = video_cdc_topic
        self._user_cdc_topic = user_cdc_topic
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

    def emit_video_cdc_event(self, key: str, value: Dict[str, Any], emitted_at: datetime) -> None:
        del emitted_at
        self._produce(self._video_cdc_topic, key, value)

    def emit_user_cdc_event(self, key: str, value: Dict[str, Any], emitted_at: datetime) -> None:
        del emitted_at
        self._produce(self._user_cdc_topic, key, value)

    def flush(self) -> None:
        self._producer.flush()
        if self._delivery_errors:
            raise RuntimeError(
                "Kafka delivery failed for one or more messages: "
                + "; ".join(self._delivery_errors[:3])
            )
