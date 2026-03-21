from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.bt_user_activity_sessions_30m_sql import (  # noqa: E402
    ET_TIMEZONE,
    create_user_activity_sessions_30m_sql,
    default_d_minus_1_data_date_sql,
    delete_user_activity_sessions_30m_data_date_sql,
    insert_user_activity_sessions_30m_for_data_date_sql,
    required_user_activity_sessions_30m_columns,
)


class BtUserActivitySessions30mSqlTests(unittest.TestCase):
    @staticmethod
    def _pick_dominant_pair(events: list[dict[str, Any]]) -> tuple[str, str]:
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for event in events:
            key = (event["category"], event["region"])
            current = grouped.setdefault(
                key,
                {
                    "watch_time_sum_ms": 0,
                    "event_count": 0,
                    "last_event_timestamp": event["event_timestamp"],
                    "last_event_id": event["event_id"],
                },
            )
            current["watch_time_sum_ms"] += event.get("watch_time_ms", 0) or 0
            current["event_count"] += 1
            if (
                event["event_timestamp"] > current["last_event_timestamp"]
                or (
                    event["event_timestamp"] == current["last_event_timestamp"]
                    and event["event_id"] > current["last_event_id"]
                )
            ):
                current["last_event_timestamp"] = event["event_timestamp"]
                current["last_event_id"] = event["event_id"]

        winner = max(
            grouped.items(),
            key=lambda item: (
                item[1]["watch_time_sum_ms"],
                item[1]["event_count"],
                item[1]["last_event_timestamp"],
                item[1]["last_event_id"],
            ),
        )
        return winner[0]

    def test_required_columns_match_contract(self) -> None:
        expected = (
            ("session_id", "STRING"),
            ("user_id", "STRING"),
            ("session_start_ts", "TIMESTAMP"),
            ("session_end_ts", "TIMESTAMP"),
            ("category", "STRING"),
            ("region", "STRING"),
            ("new_vs_returning_user", "STRING"),
            ("session_duration_sec", "BIGINT"),
            ("event_count", "BIGINT"),
            ("watch_time_sum_ms", "BIGINT"),
            ("data_date", "DATE"),
        )
        self.assertEqual(required_user_activity_sessions_30m_columns(), expected)

    def test_create_sql_contains_contract_schema_and_layout(self) -> None:
        sql = create_user_activity_sessions_30m_sql()
        self.assertIn("CREATE TABLE IF NOT EXISTS lakehouse.silver.user_activity_sessions_30m", sql)
        for column_name, data_type in required_user_activity_sessions_30m_columns():
            self.assertIn(f"{column_name} {data_type}", sql)
        self.assertIn("PARTITIONED BY (data_date, bucket(64, user_id))", sql)

    def test_default_data_date_sql_uses_et_d_minus_1(self) -> None:
        self.assertEqual(
            default_d_minus_1_data_date_sql(),
            f"date_sub(to_date(from_utc_timestamp(current_timestamp(), '{ET_TIMEZONE}')), 1)",
        )

    def test_delete_sql_targets_d_minus_1_slice(self) -> None:
        sql = delete_user_activity_sessions_30m_data_date_sql(
            target_table="lakehouse.silver.user_activity_sessions_30m_canary"
        )
        self.assertIn("DELETE FROM lakehouse.silver.user_activity_sessions_30m_canary", sql)
        self.assertIn(
            f"WHERE data_date = date_sub(to_date(from_utc_timestamp(current_timestamp(), '{ET_TIMEZONE}')), 1)",
            sql,
        )

    def test_insert_sql_contains_sessionization_and_attribution_logic(self) -> None:
        sql = insert_user_activity_sessions_30m_for_data_date_sql(
            source_table="lakehouse.silver.events_conformed_canary",
            dim_users_table="lakehouse.dims.dim_users_scd2_canary",
            target_table="lakehouse.silver.user_activity_sessions_30m_canary",
        )
        self.assertIn("INSERT INTO lakehouse.silver.user_activity_sessions_30m_canary", sql)
        self.assertIn("FROM lakehouse.silver.events_conformed_canary", sql)
        self.assertIn("LEFT JOIN lakehouse.dims.dim_users_scd2_canary dim_users", sql)
        self.assertIn("ORDER BY event_timestamp ASC, event_id ASC", sql)
        self.assertIn("event_timestamp > prev_event_timestamp + INTERVAL 30 MINUTES", sql)
        self.assertIn("GROUP BY user_id, data_date, session_seq, category, region", sql)
        self.assertIn("CAST(COALESCE(SUM(watch_time_ms), 0) AS BIGINT) AS pair_watch_time_sum_ms", sql)
        self.assertIn("COUNT(*) AS pair_event_count", sql)
        self.assertIn("MAX(event_timestamp) AS pair_last_event_timestamp", sql)
        self.assertIn("MAX_BY(event_id, STRUCT(event_timestamp, event_id)) AS pair_last_event_id", sql)
        self.assertIn("ORDER BY pair_watch_time_sum_ms DESC,", sql)
        self.assertIn("pair_event_count DESC,", sql)
        self.assertIn("pair_last_event_timestamp DESC,", sql)
        self.assertIn("pair_last_event_id DESC", sql)
        self.assertIn("aggregated.session_end_ts >= dim_users.valid_from", sql)
        self.assertIn("aggregated.session_end_ts < dim_users.valid_to", sql)
        self.assertIn("COALESCE(new_vs_returning_user, 'unknown') AS new_vs_returning_user", sql)
        self.assertIn("SHA2(CONCAT_WS('|', aggregated.user_id, CAST(aggregated.session_start_ts AS STRING)), 256)", sql)
        self.assertIn("COUNT(*) AS event_count", sql)
        self.assertIn("CAST(COALESCE(SUM(watch_time_ms), 0) AS BIGINT) AS watch_time_sum_ms", sql)

    def test_dominant_pair_prefers_highest_watch_time_even_if_not_last_event(self) -> None:
        winner = self._pick_dominant_pair(
            [
                {
                    "category": "sports",
                    "region": "us",
                    "watch_time_ms": 300,
                    "event_timestamp": "2026-03-20T10:00:00",
                    "event_id": "evt-001",
                },
                {
                    "category": "music",
                    "region": "ca",
                    "watch_time_ms": 100,
                    "event_timestamp": "2026-03-20T10:05:00",
                    "event_id": "evt-002",
                },
                {
                    "category": "music",
                    "region": "ca",
                    "watch_time_ms": 100,
                    "event_timestamp": "2026-03-20T10:10:00",
                    "event_id": "evt-003",
                },
            ]
        )
        self.assertEqual(winner, ("sports", "us"))

    def test_dominant_pair_breaks_watch_time_ties_by_event_count(self) -> None:
        winner = self._pick_dominant_pair(
            [
                {
                    "category": "sports",
                    "region": "us",
                    "watch_time_ms": 100,
                    "event_timestamp": "2026-03-20T10:00:00",
                    "event_id": "evt-001",
                },
                {
                    "category": "sports",
                    "region": "us",
                    "watch_time_ms": 100,
                    "event_timestamp": "2026-03-20T10:03:00",
                    "event_id": "evt-002",
                },
                {
                    "category": "music",
                    "region": "ca",
                    "watch_time_ms": 200,
                    "event_timestamp": "2026-03-20T10:04:00",
                    "event_id": "evt-003",
                },
            ]
        )
        self.assertEqual(winner, ("sports", "us"))

    def test_dominant_pair_breaks_remaining_ties_by_latest_timestamp_then_event_id(self) -> None:
        winner = self._pick_dominant_pair(
            [
                {
                    "category": "sports",
                    "region": "us",
                    "watch_time_ms": 200,
                    "event_timestamp": "2026-03-20T10:00:00",
                    "event_id": "evt-001",
                },
                {
                    "category": "music",
                    "region": "ca",
                    "watch_time_ms": 200,
                    "event_timestamp": "2026-03-20T10:02:00",
                    "event_id": "evt-003",
                },
                {
                    "category": "sports",
                    "region": "us",
                    "watch_time_ms": 0,
                    "event_timestamp": "2026-03-20T10:05:00",
                    "event_id": "evt-004",
                },
                {
                    "category": "music",
                    "region": "ca",
                    "watch_time_ms": 0,
                    "event_timestamp": "2026-03-20T10:05:00",
                    "event_id": "evt-005",
                },
            ]
        )
        self.assertEqual(winner, ("music", "ca"))

    def test_dominant_pair_treats_null_watch_time_as_zero(self) -> None:
        winner = self._pick_dominant_pair(
            [
                {
                    "category": "sports",
                    "region": "us",
                    "watch_time_ms": None,
                    "event_timestamp": "2026-03-20T10:00:00",
                    "event_id": "evt-001",
                },
                {
                    "category": "music",
                    "region": "ca",
                    "watch_time_ms": 0,
                    "event_timestamp": "2026-03-20T10:01:00",
                    "event_id": "evt-002",
                },
                {
                    "category": "music",
                    "region": "ca",
                    "watch_time_ms": None,
                    "event_timestamp": "2026-03-20T10:02:00",
                    "event_id": "evt-003",
                },
            ]
        )
        self.assertEqual(winner, ("music", "ca"))


if __name__ == "__main__":
    unittest.main()
