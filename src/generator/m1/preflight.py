"""Kafka preflight contract checks for MIC-36."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Mapping, Sequence, Tuple

from .constants import TOPIC_CDC_VIDEOS, TOPIC_CONTENT_EVENTS


class KafkaPreflightError(RuntimeError):
    """Raised when Kafka preflight checks fail."""


@dataclass(frozen=True)
class TopicExpectation:
    topic: str
    min_partitions: int
    replication_factor: int = 1
    key_field: str = "video_id"


def build_default_topic_expectations(
    *,
    content_events_min_partitions: int = 6,
    cdc_videos_min_partitions: int = 3,
) -> Tuple[TopicExpectation, TopicExpectation]:
    return (
        TopicExpectation(
            topic=TOPIC_CONTENT_EVENTS,
            min_partitions=content_events_min_partitions,
            replication_factor=1,
            key_field="video_id",
        ),
        TopicExpectation(
            topic=TOPIC_CDC_VIDEOS,
            min_partitions=cdc_videos_min_partitions,
            replication_factor=1,
            key_field="video_id",
        ),
    )


def plan_topics_to_create(
    *,
    topic_partitions: Mapping[str, int],
    topic_errors: Mapping[str, str],
    expectations: Sequence[TopicExpectation],
) -> List[TopicExpectation]:
    """Return expectations for topics missing from broker metadata."""
    missing: List[TopicExpectation] = []

    for expected in expectations:
        topic = expected.topic.strip()
        if not topic:
            raise KafkaPreflightError("topic expectation must define a non-empty topic")
        if expected.min_partitions <= 0:
            raise KafkaPreflightError(
                f"topic '{topic}' has invalid min_partitions={expected.min_partitions}; must be > 0"
            )
        if expected.replication_factor <= 0:
            raise KafkaPreflightError(
                "topic "
                f"'{topic}' has invalid replication_factor={expected.replication_factor}; must be > 0"
            )

        topic_known = topic in topic_partitions or topic in topic_errors
        if not topic_known:
            missing.append(expected)

    return missing


def evaluate_topic_contract(
    *,
    topic_partitions: Mapping[str, int],
    topic_replication_factors: Mapping[str, int],
    topic_errors: Mapping[str, str],
    expectations: Sequence[TopicExpectation],
) -> Tuple[List[str], List[str]]:
    """Evaluate topic existence, partition, and replication-factor contract rules."""
    info: List[str] = []
    errors: List[str] = []

    for expected in expectations:
        topic = expected.topic.strip()
        key_field = expected.key_field.strip()

        if not topic:
            errors.append("topic expectation must define a non-empty topic")
            continue
        if expected.min_partitions <= 0:
            errors.append(
                f"topic '{topic}' has invalid min_partitions={expected.min_partitions}; must be > 0"
            )
            continue
        if expected.replication_factor <= 0:
            errors.append(
                "topic "
                f"'{topic}' has invalid replication_factor={expected.replication_factor}; must be > 0"
            )
            continue
        if not key_field:
            errors.append(f"topic '{topic}' has empty key_field")
            continue

        if topic in topic_errors:
            errors.append(f"topic '{topic}' metadata error: {topic_errors[topic]}")
            continue

        actual_partitions = topic_partitions.get(topic)
        if actual_partitions is None:
            errors.append(f"topic '{topic}' is missing")
            continue

        if actual_partitions < expected.min_partitions:
            errors.append(
                f"topic '{topic}' has {actual_partitions} partitions; expected >= {expected.min_partitions}"
            )
            continue

        actual_replication_factor = topic_replication_factors.get(topic)
        if actual_replication_factor is None:
            errors.append(f"topic '{topic}' replication factor metadata is unavailable")
            continue
        if actual_replication_factor < expected.replication_factor:
            errors.append(
                "topic "
                f"'{topic}' has replication factor {actual_replication_factor}; "
                f"expected >= {expected.replication_factor}"
            )
            continue

        info.append(
            f"topic '{topic}' ready: partitions={actual_partitions} "
            f"(required >= {expected.min_partitions}), "
            f"replication_factor={actual_replication_factor} "
            f"(required >= {expected.replication_factor}), key_field='{key_field}'"
        )

    return info, errors


def _resolve_admin_client_class():
    try:
        from confluent_kafka.admin import AdminClient  # type: ignore
    except ImportError as exc:
        raise KafkaPreflightError(
            "confluent_kafka is required for Kafka preflight. "
            "Install with `pip install confluent-kafka`."
        ) from exc
    return AdminClient


def _resolve_admin_types():
    try:
        from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore
    except ImportError as exc:
        raise KafkaPreflightError(
            "confluent_kafka is required for Kafka bootstrap/preflight. "
            "Install with `pip install confluent-kafka`."
        ) from exc
    return AdminClient, NewTopic


def _is_topic_already_exists_error(exc: Exception) -> bool:
    message = str(exc).upper()
    if "TOPIC_ALREADY_EXISTS" in message:
        return True

    error_obj = None
    if getattr(exc, "args", None):
        error_obj = exc.args[0]

    code_func = getattr(error_obj, "code", None)
    if callable(code_func):
        try:
            error_code = code_func()
        except Exception:  # pragma: no cover - defensive guard
            error_code = None
        else:
            try:
                from confluent_kafka import KafkaError  # type: ignore
            except ImportError:  # pragma: no cover - defensive guard
                KafkaError = None  # type: ignore[assignment]
            if KafkaError is not None and error_code == KafkaError.TOPIC_ALREADY_EXISTS:
                return True
            if error_code in (36,):  # Kafka TOPIC_ALREADY_EXISTS canonical code
                return True

    return False


def load_topic_metadata(
    *,
    bootstrap_servers: str,
    timeout_seconds: float = 10.0,
) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, str]]:
    """Load topic partition, replication-factor, and topic-level metadata errors."""
    admin_client_cls = _resolve_admin_client_class()
    client = admin_client_cls(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "m1-kafka-preflight",
        }
    )
    try:
        metadata = client.list_topics(timeout=timeout_seconds)
    except Exception as exc:  # pragma: no cover - integration path
        raise KafkaPreflightError(
            f"failed to fetch Kafka metadata from '{bootstrap_servers}': {exc}"
        ) from exc

    topic_partitions: Dict[str, int] = {}
    topic_replication_factors: Dict[str, int] = {}
    topic_errors: Dict[str, str] = {}

    for topic_name, topic_meta in metadata.topics.items():
        topic_error = getattr(topic_meta, "error", None)
        if topic_error:
            topic_errors[str(topic_name)] = str(topic_error)
            continue

        topic_str = str(topic_name)
        partition_count = len(topic_meta.partitions)
        topic_partitions[topic_str] = partition_count

        replica_counts: List[int] = []
        for partition_meta in topic_meta.partitions.values():
            replicas = getattr(partition_meta, "replicas", None)
            if replicas is None:
                continue
            replica_counts.append(len(replicas))

        if partition_count > 0:
            if not replica_counts:
                topic_errors[topic_str] = "replication factor metadata is unavailable"
                topic_partitions.pop(topic_str, None)
                continue
            topic_replication_factors[topic_str] = min(replica_counts)

    return topic_partitions, topic_replication_factors, topic_errors


def create_missing_topics(
    *,
    bootstrap_servers: str,
    expectations: Sequence[TopicExpectation],
    timeout_seconds: float = 10.0,
) -> List[str]:
    """Create missing Kafka topics and return the topic names created in this attempt."""
    if not expectations:
        return []

    admin_client_cls, new_topic_cls = _resolve_admin_types()
    client = admin_client_cls(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "m1-kafka-topic-bootstrap",
        }
    )
    new_topics = [
        new_topic_cls(
            expected.topic,
            num_partitions=expected.min_partitions,
            replication_factor=expected.replication_factor,
        )
        for expected in expectations
    ]
    futures = client.create_topics(
        new_topics,
        operation_timeout=timeout_seconds,
        request_timeout=timeout_seconds,
    )

    created: List[str] = []
    for expected in expectations:
        future = futures.get(expected.topic)
        if future is None:
            raise KafkaPreflightError(f"missing create topic result for '{expected.topic}'")
        try:
            future.result()
        except Exception as exc:  # pragma: no cover - integration path
            if _is_topic_already_exists_error(exc):
                continue
            raise KafkaPreflightError(f"failed to create topic '{expected.topic}': {exc}") from exc
        created.append(expected.topic)

    return created


def bootstrap_kafka_topics(
    *,
    bootstrap_servers: str,
    expectations: Sequence[TopicExpectation],
    logger: Callable[[str], None] = print,
    metadata_loader: Callable[
        ..., Tuple[Dict[str, int], Dict[str, int], Dict[str, str]]
    ] = load_topic_metadata,
    topic_creator: Callable[..., List[str]] = create_missing_topics,
) -> Dict[str, List[str]]:
    """Create missing topics, reload metadata, and return bootstrap status."""
    if not expectations:
        raise KafkaPreflightError("no topic expectations configured")

    logger(f"[preflight] bootstrapping Kafka topics at '{bootstrap_servers}'")
    topic_partitions, _, topic_errors = metadata_loader(bootstrap_servers=bootstrap_servers)
    to_create = plan_topics_to_create(
        topic_partitions=topic_partitions,
        topic_errors=topic_errors,
        expectations=expectations,
    )
    created: List[str] = []

    if to_create:
        topic_summary = ", ".join(
            f"{expected.topic}(partitions={expected.min_partitions},rf={expected.replication_factor})"
            for expected in to_create
        )
        logger(f"[preflight] creating missing topics: {topic_summary}")
        created_attempt = topic_creator(
            bootstrap_servers=bootstrap_servers,
            expectations=to_create,
        )
        created_attempt_set = set(created_attempt)
        created = [expected.topic for expected in expectations if expected.topic in created_attempt_set]
    else:
        logger("[preflight] no missing topics to create")

    topic_partitions, _, topic_errors = metadata_loader(bootstrap_servers=bootstrap_servers)
    still_missing = plan_topics_to_create(
        topic_partitions=topic_partitions,
        topic_errors=topic_errors,
        expectations=expectations,
    )
    if still_missing:
        missing_names = ", ".join(expected.topic for expected in still_missing)
        raise KafkaPreflightError(
            "Kafka topic bootstrap incomplete; topics still missing after create attempt: "
            f"{missing_names}"
        )

    present_after = set(topic_partitions.keys()) | set(topic_errors.keys())
    created_set = set(created)
    already_present = [
        expected.topic
        for expected in expectations
        if expected.topic in present_after and expected.topic not in created_set
    ]
    logger(
        "[preflight] topic bootstrap status: "
        f"created={created or ['none']}, already_present={already_present or ['none']}"
    )
    return {
        "created": created,
        "already_present": already_present,
    }


def run_kafka_preflight(
    *,
    bootstrap_servers: str,
    expectations: Sequence[TopicExpectation],
    logger: Callable[[str], None] = print,
    metadata_loader: Callable[
        ..., Tuple[Dict[str, int], Dict[str, int], Dict[str, str]]
    ] = load_topic_metadata,
) -> None:
    """Run Kafka preflight and fail fast when any contract check fails."""
    if not expectations:
        raise KafkaPreflightError("no topic expectations configured")

    logger(f"[preflight] checking Kafka topic readiness at '{bootstrap_servers}'")
    topic_partitions, topic_replication_factors, topic_errors = metadata_loader(
        bootstrap_servers=bootstrap_servers
    )

    info, errors = evaluate_topic_contract(
        topic_partitions=topic_partitions,
        topic_replication_factors=topic_replication_factors,
        topic_errors=topic_errors,
        expectations=expectations,
    )
    for line in info:
        logger(f"[preflight] {line}")

    if errors:
        details = "\n".join(f"- {line}" for line in errors)
        raise KafkaPreflightError(f"Kafka preflight checks failed:\n{details}")

    logger("[preflight] Kafka topic readiness checks passed")
