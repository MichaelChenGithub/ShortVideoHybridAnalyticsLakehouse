from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import unittest
from unittest.mock import Mock, patch

from generator.m1.preflight import (
    KafkaPreflightError,
    TopicExpectation,
    bootstrap_kafka_topics,
    build_default_topic_expectations,
    create_missing_topics,
    evaluate_topic_contract,
    load_topic_metadata,
    plan_topics_to_create,
    run_kafka_preflight,
)


class _FakeTopicMeta:
    def __init__(self, partitions: int = 1, error=None, replication_factor: int = 1) -> None:
        self.partitions = {
            idx: SimpleNamespace(replicas=list(range(replication_factor)))
            for idx in range(partitions)
        }
        self.error = error


class _FakeMetadata:
    def __init__(self, topics):
        self.topics = topics


class _FakeAdminClientWithTopicError:
    def __init__(self, cfg) -> None:
        self.cfg = cfg

    def list_topics(self, timeout):
        del timeout
        return _FakeMetadata(
            {
                "content_events": _FakeTopicMeta(error="KafkaError{code=LEADER_NOT_AVAILABLE}"),
                "cdc.content.videos": _FakeTopicMeta(partitions=3),
            }
        )


class _FakeAdminClientFail:
    def __init__(self, cfg) -> None:
        self.cfg = cfg

    def list_topics(self, timeout):
        del timeout
        raise RuntimeError("broker unavailable")


class _FakeNewTopic:
    def __init__(self, topic: str, num_partitions: int, replication_factor: int) -> None:
        self.topic = topic
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor


class _FakeFutureOk:
    def result(self):
        return None


class _FakeFutureExists:
    def result(self):
        raise RuntimeError("KafkaError{code=TOPIC_ALREADY_EXISTS}")


class _FakeFutureFail:
    def result(self):
        raise RuntimeError("KafkaError{code=INVALID_REPLICATION_FACTOR}")


class _FakeAdminClientCreate:
    futures = {}

    def __init__(self, cfg) -> None:
        self.cfg = cfg

    def create_topics(self, new_topics, operation_timeout, request_timeout):
        del operation_timeout, request_timeout
        return {topic.topic: self.futures[topic.topic] for topic in new_topics}


class KafkaPreflightEvaluatorTests(unittest.TestCase):
    def test_evaluator_passes_when_topics_meet_partition_minimums(self) -> None:
        info, errors = evaluate_topic_contract(
            topic_partitions={"content_events": 6, "cdc.content.videos": 3},
            topic_replication_factors={"content_events": 1, "cdc.content.videos": 1},
            topic_errors={},
            expectations=(
                TopicExpectation(topic="content_events", min_partitions=6),
                TopicExpectation(topic="cdc.content.videos", min_partitions=3),
            ),
        )

        self.assertEqual(errors, [])
        self.assertEqual(len(info), 2)

    def test_evaluator_fails_when_topic_is_missing(self) -> None:
        _, errors = evaluate_topic_contract(
            topic_partitions={"cdc.content.videos": 3},
            topic_replication_factors={"cdc.content.videos": 1},
            topic_errors={},
            expectations=(TopicExpectation(topic="content_events", min_partitions=6),),
        )
        self.assertEqual(errors, ["topic 'content_events' is missing"])

    def test_evaluator_fails_when_partitions_are_below_minimum(self) -> None:
        _, errors = evaluate_topic_contract(
            topic_partitions={"content_events": 2},
            topic_replication_factors={"content_events": 1},
            topic_errors={},
            expectations=(TopicExpectation(topic="content_events", min_partitions=6),),
        )
        self.assertEqual(
            errors,
            ["topic 'content_events' has 2 partitions; expected >= 6"],
        )

    def test_evaluator_fails_when_replication_factor_is_below_minimum(self) -> None:
        _, errors = evaluate_topic_contract(
            topic_partitions={"content_events": 6},
            topic_replication_factors={"content_events": 1},
            topic_errors={},
            expectations=(TopicExpectation(topic="content_events", min_partitions=6, replication_factor=2),),
        )
        self.assertEqual(
            errors,
            ["topic 'content_events' has replication factor 1; expected >= 2"],
        )

    def test_evaluator_fails_for_topic_metadata_error(self) -> None:
        _, errors = evaluate_topic_contract(
            topic_partitions={},
            topic_replication_factors={},
            topic_errors={"content_events": "KafkaError{code=LEADER_NOT_AVAILABLE}"},
            expectations=(TopicExpectation(topic="content_events", min_partitions=6),),
        )
        self.assertEqual(
            errors,
            ["topic 'content_events' metadata error: KafkaError{code=LEADER_NOT_AVAILABLE}"],
        )

    def test_evaluator_fails_for_invalid_expectation_values(self) -> None:
        _, errors = evaluate_topic_contract(
            topic_partitions={},
            topic_replication_factors={},
            topic_errors={},
            expectations=(
                TopicExpectation(topic="content_events", min_partitions=0),
                TopicExpectation(topic="cdc.content.videos", min_partitions=3, key_field=""),
            ),
        )
        self.assertEqual(
            errors,
            [
                "topic 'content_events' has invalid min_partitions=0; must be > 0",
                "topic 'cdc.content.videos' has empty key_field",
            ],
        )


class KafkaBootstrapPlanningTests(unittest.TestCase):
    def test_plans_both_topics_when_both_missing(self) -> None:
        expectations = build_default_topic_expectations()
        missing = plan_topics_to_create(
            topic_partitions={},
            topic_errors={},
            expectations=expectations,
        )
        self.assertEqual([item.topic for item in missing], ["content_events", "cdc.content.videos"])

    def test_plans_only_missing_topic(self) -> None:
        expectations = build_default_topic_expectations()
        missing = plan_topics_to_create(
            topic_partitions={"content_events": 6},
            topic_errors={},
            expectations=expectations,
        )
        self.assertEqual([item.topic for item in missing], ["cdc.content.videos"])

    def test_plans_none_when_topics_already_known(self) -> None:
        expectations = build_default_topic_expectations()
        missing = plan_topics_to_create(
            topic_partitions={"content_events": 6},
            topic_errors={"cdc.content.videos": "KafkaError{code=LEADER_NOT_AVAILABLE}"},
            expectations=expectations,
        )
        self.assertEqual(missing, [])


class KafkaMetadataLoaderTests(unittest.TestCase):
    def test_loader_returns_partition_and_topic_error_maps(self) -> None:
        with patch(
            "generator.m1.preflight._resolve_admin_client_class",
            return_value=_FakeAdminClientWithTopicError,
        ):
            partitions, replication_factors, errors = load_topic_metadata(
                bootstrap_servers="localhost:9092"
            )

        self.assertEqual(partitions, {"cdc.content.videos": 3})
        self.assertEqual(replication_factors, {"cdc.content.videos": 1})
        self.assertEqual(errors, {"content_events": "KafkaError{code=LEADER_NOT_AVAILABLE}"})

    def test_loader_raises_when_broker_metadata_fetch_fails(self) -> None:
        with patch(
            "generator.m1.preflight._resolve_admin_client_class",
            return_value=_FakeAdminClientFail,
        ):
            with self.assertRaisesRegex(
                KafkaPreflightError,
                "failed to fetch Kafka metadata from 'localhost:9092': broker unavailable",
            ):
                load_topic_metadata(bootstrap_servers="localhost:9092")


class KafkaTopicCreateTests(unittest.TestCase):
    def test_create_missing_topics_success(self) -> None:
        _FakeAdminClientCreate.futures = {
            "content_events": _FakeFutureOk(),
            "cdc.content.videos": _FakeFutureOk(),
        }
        expectations = build_default_topic_expectations()
        with patch(
            "generator.m1.preflight._resolve_admin_types",
            return_value=(_FakeAdminClientCreate, _FakeNewTopic),
        ):
            created = create_missing_topics(
                bootstrap_servers="localhost:9092",
                expectations=expectations,
            )

        self.assertEqual(created, ["content_events", "cdc.content.videos"])

    def test_create_missing_topics_topic_already_exists_is_non_fatal(self) -> None:
        _FakeAdminClientCreate.futures = {
            "content_events": _FakeFutureExists(),
            "cdc.content.videos": _FakeFutureOk(),
        }
        expectations = build_default_topic_expectations()
        with patch(
            "generator.m1.preflight._resolve_admin_types",
            return_value=(_FakeAdminClientCreate, _FakeNewTopic),
        ):
            created = create_missing_topics(
                bootstrap_servers="localhost:9092",
                expectations=expectations,
            )

        self.assertEqual(created, ["cdc.content.videos"])

    def test_create_missing_topics_raises_for_non_recoverable_error(self) -> None:
        _FakeAdminClientCreate.futures = {
            "content_events": _FakeFutureFail(),
        }
        expectations = (TopicExpectation(topic="content_events", min_partitions=6),)
        with patch(
            "generator.m1.preflight._resolve_admin_types",
            return_value=(_FakeAdminClientCreate, _FakeNewTopic),
        ):
            with self.assertRaisesRegex(
                KafkaPreflightError,
                "failed to create topic 'content_events'",
            ):
                create_missing_topics(
                    bootstrap_servers="localhost:9092",
                    expectations=expectations,
                )


class KafkaBootstrapOrchestrationTests(unittest.TestCase):
    def test_bootstrap_creates_missing_and_returns_status(self) -> None:
        expectations = build_default_topic_expectations()
        metadata_calls = [
            ({}, {}, {}),
            ({"content_events": 6, "cdc.content.videos": 3}, {"content_events": 1, "cdc.content.videos": 1}, {}),
        ]
        metadata_loader = Mock(side_effect=metadata_calls)
        topic_creator = Mock(return_value=["content_events", "cdc.content.videos"])
        logs = []

        status = bootstrap_kafka_topics(
            bootstrap_servers="localhost:9092",
            expectations=expectations,
            logger=logs.append,
            metadata_loader=metadata_loader,
            topic_creator=topic_creator,
        )

        self.assertEqual(status["created"], ["content_events", "cdc.content.videos"])
        self.assertEqual(status["already_present"], [])
        topic_creator.assert_called_once()
        self.assertTrue(any("creating missing topics" in line for line in logs))
        self.assertTrue(any("topic bootstrap status" in line for line in logs))

    def test_bootstrap_skips_create_when_no_topics_missing(self) -> None:
        expectations = build_default_topic_expectations()
        metadata_loader = Mock(
            return_value=(
                {"content_events": 6, "cdc.content.videos": 3},
                {"content_events": 1, "cdc.content.videos": 1},
                {},
            )
        )
        topic_creator = Mock()
        logs = []

        status = bootstrap_kafka_topics(
            bootstrap_servers="localhost:9092",
            expectations=expectations,
            logger=logs.append,
            metadata_loader=metadata_loader,
            topic_creator=topic_creator,
        )

        self.assertEqual(status["created"], [])
        self.assertEqual(status["already_present"], ["content_events", "cdc.content.videos"])
        topic_creator.assert_not_called()
        self.assertTrue(any("no missing topics to create" in line for line in logs))

    def test_bootstrap_status_marks_topic_already_present_after_create_race(self) -> None:
        expectations = build_default_topic_expectations()
        metadata_loader = Mock(
            side_effect=[
                ({}, {}, {}),
                (
                    {"content_events": 6, "cdc.content.videos": 3},
                    {"content_events": 1, "cdc.content.videos": 1},
                    {},
                ),
            ]
        )
        topic_creator = Mock(return_value=["content_events"])
        logs = []

        status = bootstrap_kafka_topics(
            bootstrap_servers="localhost:9092",
            expectations=expectations,
            logger=logs.append,
            metadata_loader=metadata_loader,
            topic_creator=topic_creator,
        )

        self.assertEqual(status["created"], ["content_events"])
        self.assertEqual(status["already_present"], ["cdc.content.videos"])
        self.assertTrue(
            any(
                "topic bootstrap status: created=['content_events'], already_present=['cdc.content.videos']"
                in line
                for line in logs
            )
        )

    def test_bootstrap_fails_when_topics_still_missing_after_create(self) -> None:
        expectations = build_default_topic_expectations()
        metadata_loader = Mock(
            side_effect=[
                ({}, {}, {}),
                ({"content_events": 6}, {"content_events": 1}, {}),
            ]
        )
        with self.assertRaisesRegex(
            KafkaPreflightError,
            "topics still missing after create attempt: cdc.content.videos",
        ):
            bootstrap_kafka_topics(
                bootstrap_servers="localhost:9092",
                expectations=expectations,
                metadata_loader=metadata_loader,
                topic_creator=Mock(return_value=["content_events"]),
            )


class KafkaPreflightRunnerTests(unittest.TestCase):
    def test_runner_logs_pass_result(self) -> None:
        messages = []
        run_kafka_preflight(
            bootstrap_servers="localhost:9092",
            expectations=build_default_topic_expectations(),
            logger=messages.append,
            metadata_loader=lambda **_: (
                {"content_events": 6, "cdc.content.videos": 3},
                {"content_events": 1, "cdc.content.videos": 1},
                {},
            ),
        )

        self.assertEqual(messages[0], "[preflight] checking Kafka topic readiness at 'localhost:9092'")
        self.assertTrue(any("topic 'content_events' ready" in line for line in messages))
        self.assertTrue(any("Kafka topic readiness checks passed" in line for line in messages))

    def test_runner_aggregates_failures(self) -> None:
        with self.assertRaisesRegex(
            KafkaPreflightError,
            "Kafka preflight checks failed:\\n- topic 'content_events' is missing\\n- topic 'cdc.content.videos' has 2 partitions; expected >= 3",
        ):
            run_kafka_preflight(
                bootstrap_servers="localhost:9092",
                expectations=build_default_topic_expectations(),
                metadata_loader=lambda **_: (
                    {"cdc.content.videos": 2},
                    {"cdc.content.videos": 1},
                    {},
                ),
            )


if __name__ == "__main__":
    unittest.main()
