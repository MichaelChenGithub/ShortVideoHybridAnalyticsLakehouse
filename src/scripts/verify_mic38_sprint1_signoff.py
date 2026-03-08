"""Unified MIC-38 Sprint 1 sign-off verifier."""

from __future__ import annotations

import argparse
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


CHECKPOINT_KEYS = [
    "content_raw",
    "content_gold",
    "content_invalid",
    "cdc_dim",
    "cdc_invalid",
]


def utc_now_ms() -> int:
    return int(time.time() * 1_000)


def _as_int(value: Any, *, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, *, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _extract_status(log_text: str) -> str:
    for line in log_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("PASS:"):
            return "PASS"
        if stripped.startswith("FAIL:"):
            return "FAIL"
    return "UNKNOWN"


def extract_last_json_object(log_text: str) -> dict[str, Any]:
    for line in reversed(log_text.splitlines()):
        stripped = line.strip()
        if not stripped or not stripped.startswith("{") or not stripped.endswith("}"):
            continue
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("no JSON object line found in verifier log")


def load_log_result(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    return {
        "status": _extract_status(text),
        "payload": extract_last_json_object(text),
        "path": str(path),
    }


def load_json_file(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return payload


def _safe_age_ms(newest_ts_ms: Any, *, now_ms: int) -> int | None:
    if newest_ts_ms is None:
        return None
    try:
        newest_ts_ms_int = int(newest_ts_ms)
    except (TypeError, ValueError):
        return None
    return now_ms - newest_ts_ms_int


def _parse_iso8601_ms(value: Any) -> int | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000)


def _p95(values: list[int]) -> int | None:
    if not values:
        return None
    sorted_values = sorted(values)
    rank = max(0, math.ceil(0.95 * len(sorted_values)) - 1)
    return sorted_values[rank]


def _checkpoint_count(snapshot: Mapping[str, Any], key: str) -> int:
    paths = snapshot.get("paths")
    if not isinstance(paths, Mapping):
        return 0
    value = paths.get(key)
    if not isinstance(value, Mapping):
        return 0
    return _as_int(value.get("file_count"), default=0)


def _gate(name: str, passed: bool, details: str, metrics: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "name": name,
        "passed": passed,
        "details": details,
        "metrics": dict(metrics),
    }


def build_signoff_report(
    *,
    run_id: str,
    run_start_ms: int,
    verifier_status: Mapping[str, str],
    content_metrics: Mapping[str, Any],
    content_contract: Mapping[str, Any],
    cdc_upsert: Mapping[str, Any],
    cdc_invalid: Mapping[str, Any],
    cdc_health: Mapping[str, Any],
    runtime_start: Mapping[str, Any],
    runtime_end: Mapping[str, Any],
    checkpoint_start: Mapping[str, Any],
    checkpoint_end: Mapping[str, Any],
    max_freshness_minutes: int,
    latency_threshold_minutes: int,
    max_content_invalid_rate: float,
    max_cdc_invalid_rate: float,
    input_errors: list[str] | None = None,
    checked_at_ms: int | None = None,
) -> dict[str, Any]:
    now_ms = checked_at_ms or _as_int(runtime_end.get("sample_at_ms"), default=utc_now_ms())
    checked_at = datetime.fromtimestamp(now_ms / 1_000, tz=timezone.utc).isoformat()
    content_metrics_checked_at_ms = _parse_iso8601_ms(content_metrics.get("checked_at")) or now_ms
    content_contract_checked_at_ms = _parse_iso8601_ms(content_contract.get("checked_at")) or now_ms
    cdc_health_checked_at_ms = _parse_iso8601_ms(cdc_health.get("checked_at")) or now_ms

    gates: list[dict[str, Any]] = []
    failures: list[str] = []
    input_errors = input_errors or []

    parse_gate = _gate(
        "input_artifacts_parseable",
        not input_errors,
        "All required verifier artifacts are parseable."
        if not input_errors
        else f"Parse errors detected: {len(input_errors)}",
        {"error_count": len(input_errors), "errors": input_errors},
    )
    gates.append(parse_gate)

    verifier_names = [
        "content_metrics",
        "content_contract",
        "cdc_upsert",
        "cdc_invalid",
        "cdc_health",
    ]
    failed_verifiers = [name for name in verifier_names if verifier_status.get(name) != "PASS"]
    gate = _gate(
        "underlying_verifiers_passed",
        not failed_verifiers,
        "All MIC-37/39/40/43 verifier commands reported PASS."
        if not failed_verifiers
        else "Underlying verifier did not report PASS: " + ", ".join(failed_verifiers),
        {"status": dict(verifier_status)},
    )
    gates.append(gate)

    content_raw_count = _as_int(content_metrics.get("raw_count"))
    content_gold_count = _as_int(content_metrics.get("gold_count"))
    content_invalid_count = _as_int(content_contract.get("invalid_count"))
    cdc_invalid_count = _as_int(cdc_invalid.get("row_count"))
    content_max_processed_at_ms = content_metrics.get("max_processed_at_ms")
    content_max_invalid_ingested_at_ms = content_contract.get("max_invalid_ingested_at_ms")

    run_scope_checks = {
        "raw_rows_present": content_raw_count > 0,
        "gold_rows_present": content_gold_count > 0,
        "content_invalid_rows_present": content_invalid_count > 0,
        "cdc_invalid_rows_present": cdc_invalid_count > 0,
        "dim_video_row_present": bool(cdc_upsert.get("video_id")),
        "gold_processed_after_run_start": _as_int(content_max_processed_at_ms, default=-1) >= run_start_ms,
        "content_invalid_after_run_start": _as_int(
            content_max_invalid_ingested_at_ms, default=-1
        )
        >= run_start_ms,
    }
    gate = _gate(
        "sprint1_key_tables_run_scoped",
        all(run_scope_checks.values()),
        "Sprint-1 key table outputs are present and run-scoped."
        if all(run_scope_checks.values())
        else "Missing run-scoped Sprint-1 evidence in one or more key tables.",
        {
            "checks": run_scope_checks,
            "run_start_ms": run_start_ms,
            "raw_count": content_raw_count,
            "gold_count": content_gold_count,
            "content_invalid_count": content_invalid_count,
            "cdc_invalid_count": cdc_invalid_count,
        },
    )
    gates.append(gate)

    duplicate_key_rows = _as_int(content_metrics.get("duplicate_key_rows"))
    negative_metric_rows = _as_int(content_metrics.get("negative_metric_rows"))
    null_required_rows = _as_int(content_metrics.get("null_required_rows"))
    gold_contract_checks = {
        "duplicate_key_rows_zero": duplicate_key_rows == 0,
        "negative_metric_rows_zero": negative_metric_rows == 0,
        "null_required_rows_zero": null_required_rows == 0,
    }
    gate = _gate(
        "gold_contract_valid",
        all(gold_contract_checks.values()),
        "gold.rt_video_stats_1min contract checks are satisfied."
        if all(gold_contract_checks.values())
        else "gold.rt_video_stats_1min contract checks failed.",
        {
            "checks": gold_contract_checks,
            "duplicate_key_rows": duplicate_key_rows,
            "negative_metric_rows": negative_metric_rows,
            "null_required_rows": null_required_rows,
        },
    )
    gates.append(gate)

    content_invalid_required_null_rows = _as_int(content_contract.get("invalid_required_null_rows"))
    cdc_null_counts_raw = cdc_invalid.get("null_counts")
    cdc_null_counts = {
        key: _as_int(value)
        for key, value in (cdc_null_counts_raw.items() if isinstance(cdc_null_counts_raw, Mapping) else [])
    }
    cdc_null_total = sum(cdc_null_counts.values())
    gate = _gate(
        "invalid_sinks_required_fields_non_null",
        content_invalid_required_null_rows == 0 and cdc_null_total == 0,
        "Content and CDC invalid sinks have required fields populated."
        if content_invalid_required_null_rows == 0 and cdc_null_total == 0
        else "Required-field nulls detected in invalid sink output.",
        {
            "content_invalid_required_null_rows": content_invalid_required_null_rows,
            "cdc_null_counts": cdc_null_counts,
        },
    )
    gates.append(gate)

    content_invalid_rate = _as_float(content_contract.get("invalid_rate"), default=0.0)
    cdc_invalid_rate_per_minute = _as_float(cdc_health.get("invalid_rate_per_minute"), default=0.0)
    invalid_rate_checks = {
        "content_invalid_rate_within_threshold": content_invalid_rate <= max_content_invalid_rate,
        "cdc_invalid_rate_within_threshold": cdc_invalid_rate_per_minute <= max_cdc_invalid_rate,
    }
    gate = _gate(
        "invalid_rates_within_threshold",
        all(invalid_rate_checks.values()),
        "Content and CDC invalid-rate gates passed."
        if all(invalid_rate_checks.values())
        else "Invalid-rate threshold breach detected.",
        {
            "checks": invalid_rate_checks,
            "content_invalid_rate": content_invalid_rate,
            "max_content_invalid_rate": max_content_invalid_rate,
            "cdc_invalid_rate_per_minute": cdc_invalid_rate_per_minute,
            "max_cdc_invalid_rate": max_cdc_invalid_rate,
        },
    )
    gates.append(gate)

    content_pid_start = _as_int(runtime_start.get("content_pid_count"))
    content_pid_end = _as_int(runtime_end.get("content_pid_count"))
    cdc_pid_start = _as_int(runtime_start.get("cdc_pid_count"))
    cdc_pid_end = _as_int(runtime_end.get("cdc_pid_count"))
    content_exception_end = _as_int(runtime_end.get("content_exception_lines"))
    cdc_exception_end = _as_int(runtime_end.get("cdc_exception_lines"))

    health_checks = {
        "content_pid_alive_start": content_pid_start >= 1,
        "content_pid_alive_end": content_pid_end >= 1,
        "cdc_pid_alive_start": cdc_pid_start >= 1,
        "cdc_pid_alive_end": cdc_pid_end >= 1,
        "content_exception_lines_zero": content_exception_end == 0,
        "cdc_exception_lines_zero": cdc_exception_end == 0,
    }
    gate = _gate(
        "query_health_active_no_exception",
        all(health_checks.values()),
        "Spark queries are active and exception-free in runtime snapshots."
        if all(health_checks.values())
        else "Spark query health gate failed (inactive query or exception lines found).",
        {
            "checks": health_checks,
            "content_pid_start": content_pid_start,
            "content_pid_end": content_pid_end,
            "cdc_pid_start": cdc_pid_start,
            "cdc_pid_end": cdc_pid_end,
            "content_exception_lines_end": content_exception_end,
            "cdc_exception_lines_end": cdc_exception_end,
        },
    )
    gates.append(gate)

    content_batch_start = _as_int(runtime_start.get("content_max_batch_id"), default=-1)
    content_batch_end = _as_int(runtime_end.get("content_max_batch_id"), default=-1)
    cdc_batch_start = _as_int(runtime_start.get("cdc_max_batch_id"), default=-1)
    cdc_batch_end = _as_int(runtime_end.get("cdc_max_batch_id"), default=-1)
    content_log_size_start = _as_int(runtime_start.get("content_log_size"), default=0)
    content_log_size_end = _as_int(runtime_end.get("content_log_size"), default=0)
    cdc_log_size_start = _as_int(runtime_start.get("cdc_log_size"), default=0)
    cdc_log_size_end = _as_int(runtime_end.get("cdc_log_size"), default=0)

    progress_checks = {
        "content_progress_moved": (
            content_batch_end > content_batch_start
            or content_log_size_end > content_log_size_start
        ),
        "cdc_progress_moved": (
            cdc_batch_end > cdc_batch_start or cdc_log_size_end > cdc_log_size_start
        ),
    }
    gate = _gate(
        "query_progress_moving",
        all(progress_checks.values()),
        "Both Spark queries show progress over the run window."
        if all(progress_checks.values())
        else "Spark query progress appears stagnant over sampled window.",
        {
            "checks": progress_checks,
            "content_batch_start": content_batch_start,
            "content_batch_end": content_batch_end,
            "cdc_batch_start": cdc_batch_start,
            "cdc_batch_end": cdc_batch_end,
            "content_log_size_start": content_log_size_start,
            "content_log_size_end": content_log_size_end,
            "cdc_log_size_start": cdc_log_size_start,
            "cdc_log_size_end": cdc_log_size_end,
        },
    )
    gates.append(gate)

    checkpoint_growth: dict[str, int] = {}
    checkpoint_checks: dict[str, bool] = {}
    for key in CHECKPOINT_KEYS:
        start_count = _checkpoint_count(checkpoint_start, key)
        end_count = _checkpoint_count(checkpoint_end, key)
        growth = end_count - start_count
        checkpoint_growth[key] = growth
        checkpoint_checks[key] = growth > 0

    gate = _gate(
        "checkpoint_growth",
        all(checkpoint_checks.values()),
        "All checkpoint paths grew during the run window."
        if all(checkpoint_checks.values())
        else "Checkpoint stagnation detected in one or more paths.",
        {
            "checks": checkpoint_checks,
            "growth": checkpoint_growth,
        },
    )
    gates.append(gate)

    content_freshness_age_ms = _safe_age_ms(
        content_max_processed_at_ms,
        now_ms=content_metrics_checked_at_ms,
    )
    content_invalid_freshness_age_ms = _safe_age_ms(
        content_max_invalid_ingested_at_ms,
        now_ms=content_contract_checked_at_ms,
    )
    cdc_freshness_age_ms_raw = cdc_health.get("freshness_age_ms")
    cdc_freshness_age_ms = _as_int(cdc_freshness_age_ms_raw, default=-1)
    if cdc_freshness_age_ms < 0:
        cdc_freshness_age_ms = _safe_age_ms(
            cdc_health.get("latest_source_ts_ms"),
            now_ms=cdc_health_checked_at_ms,
        )

    max_freshness_ms = max_freshness_minutes * 60 * 1_000
    freshness_checks = {
        "content_gold_fresh": (
            content_freshness_age_ms is not None and content_freshness_age_ms <= max_freshness_ms
        ),
        "content_invalid_fresh": (
            content_invalid_freshness_age_ms is not None
            and content_invalid_freshness_age_ms <= max_freshness_ms
        ),
        "cdc_fresh": (
            cdc_freshness_age_ms is not None and cdc_freshness_age_ms <= max_freshness_ms
        ),
    }
    gate = _gate(
        "freshness_sla",
        all(freshness_checks.values()),
        "Freshness gate passed against SLA breach threshold."
        if all(freshness_checks.values())
        else "Freshness breach detected against SLA threshold.",
        {
            "checks": freshness_checks,
            "max_freshness_minutes": max_freshness_minutes,
            "max_freshness_ms": max_freshness_ms,
            "content_freshness_age_ms": content_freshness_age_ms,
            "content_invalid_freshness_age_ms": content_invalid_freshness_age_ms,
            "cdc_freshness_age_ms": cdc_freshness_age_ms,
            "content_metrics_checked_at_ms": content_metrics_checked_at_ms,
            "content_contract_checked_at_ms": content_contract_checked_at_ms,
            "cdc_health_checked_at_ms": cdc_health_checked_at_ms,
        },
    )
    gates.append(gate)

    latency_samples_ms = [
        age
        for age in [
            content_freshness_age_ms,
            content_invalid_freshness_age_ms,
            cdc_freshness_age_ms,
        ]
        if age is not None
    ]
    p95_proxy_latency_ms = _p95(latency_samples_ms)
    latency_threshold_ms = latency_threshold_minutes * 60 * 1_000
    gate = _gate(
        "latency_p95_proxy_lt_threshold",
        p95_proxy_latency_ms is not None and p95_proxy_latency_ms < latency_threshold_ms,
        "Latency proxy p95 is below SLA threshold."
        if p95_proxy_latency_ms is not None and p95_proxy_latency_ms < latency_threshold_ms
        else "Latency proxy p95 breaches SLA threshold.",
        {
            "latency_samples_ms": latency_samples_ms,
            "p95_proxy_latency_ms": p95_proxy_latency_ms,
            "latency_threshold_minutes": latency_threshold_minutes,
            "latency_threshold_ms": latency_threshold_ms,
        },
    )
    gates.append(gate)

    for gate_row in gates:
        if not gate_row["passed"]:
            failures.append(f"{gate_row['name']}: {gate_row['details']}")

    status = "PASS" if not failures else "FAIL"
    report = {
        "run_id": run_id,
        "run_start_ms": run_start_ms,
        "checked_at": checked_at,
        "checked_at_ms": now_ms,
        "status": status,
        "thresholds": {
            "max_freshness_minutes": max_freshness_minutes,
            "freshness_breach_minutes": 3,
            "latency_threshold_minutes": latency_threshold_minutes,
            "latency_sla_minutes": 3,
            "max_content_invalid_rate": max_content_invalid_rate,
            "max_cdc_invalid_rate": max_cdc_invalid_rate,
        },
        "verifier_status": dict(verifier_status),
        "metrics": {
            "content_raw_count": content_raw_count,
            "content_gold_count": content_gold_count,
            "content_invalid_count": content_invalid_count,
            "cdc_invalid_count": cdc_invalid_count,
            "content_invalid_rate": content_invalid_rate,
            "cdc_invalid_rate_per_minute": cdc_invalid_rate_per_minute,
            "content_freshness_age_ms": content_freshness_age_ms,
            "content_invalid_freshness_age_ms": content_invalid_freshness_age_ms,
            "cdc_freshness_age_ms": cdc_freshness_age_ms,
            "latency_proxy_p95_ms": p95_proxy_latency_ms,
            "checkpoint_growth": checkpoint_growth,
        },
        "gates": gates,
        "failure_reasons": failures,
        "blockers": failures,
        "carry_over_items": [
            f"Resolve gate `{gate_row['name']}` and re-run MIC-38 acceptance."
            for gate_row in gates
            if not gate_row["passed"]
        ],
    }
    return report


def render_markdown_summary(report: Mapping[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# MIC-38 Sprint 1 Sign-off Summary")
    lines.append("")
    lines.append(f"- run_id: `{report.get('run_id')}`")
    lines.append(f"- checked_at: `{report.get('checked_at')}`")
    lines.append(f"- final_status: `{report.get('status')}`")
    lines.append("")
    lines.append("## Gate Results")
    lines.append("| Gate | Result | Details |")
    lines.append("|---|---|---|")
    for gate in report.get("gates", []):
        result = "PASS" if gate.get("passed") else "FAIL"
        details = str(gate.get("details", "")).replace("|", "\\|")
        lines.append(f"| `{gate.get('name')}` | {result} | {details} |")

    lines.append("")
    lines.append("## Failure Reasons")
    failure_reasons = report.get("failure_reasons", [])
    if failure_reasons:
        for reason in failure_reasons:
            lines.append(f"- {reason}")
    else:
        lines.append("- none")

    lines.append("")
    lines.append("## Carry-over Items")
    carry_over = report.get("carry_over_items", [])
    if carry_over:
        for item in carry_over:
            lines.append(f"- {item}")
    else:
        lines.append("- none")

    lines.append("")
    lines.append("## Key Metrics")
    lines.append("```json")
    lines.append(json.dumps(report.get("metrics", {}), indent=2, sort_keys=True))
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Unified MIC-38 Sprint 1 sign-off verifier")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-start-ms", type=int, required=True)

    parser.add_argument("--content-metrics-log", required=True)
    parser.add_argument("--content-contract-log", required=True)
    parser.add_argument("--cdc-upsert-log", required=True)
    parser.add_argument("--cdc-invalid-log", required=True)
    parser.add_argument("--cdc-health-log", required=True)

    parser.add_argument("--runtime-start-json", required=True)
    parser.add_argument("--runtime-end-json", required=True)
    parser.add_argument("--checkpoint-start-json", required=True)
    parser.add_argument("--checkpoint-end-json", required=True)

    parser.add_argument("--max-freshness-minutes", type=int, default=3)
    parser.add_argument("--latency-threshold-minutes", type=int, default=3)
    parser.add_argument("--max-content-invalid-rate", type=float, default=0.20)
    parser.add_argument("--max-cdc-invalid-rate", type=float, default=0.20)

    parser.add_argument("--report-json", required=True)
    parser.add_argument("--report-md", required=True)
    parser.add_argument("--now-ms", type=int)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    input_errors: list[str] = []

    log_targets = {
        "content_metrics": Path(args.content_metrics_log),
        "content_contract": Path(args.content_contract_log),
        "cdc_upsert": Path(args.cdc_upsert_log),
        "cdc_invalid": Path(args.cdc_invalid_log),
        "cdc_health": Path(args.cdc_health_log),
    }

    log_results: dict[str, dict[str, Any]] = {}
    for key, path in log_targets.items():
        try:
            log_results[key] = load_log_result(path)
        except Exception as exc:  # pragma: no cover - defensive path
            input_errors.append(f"{key} log parse failed at {path}: {exc}")
            log_results[key] = {"status": "UNKNOWN", "payload": {}}

    json_targets = {
        "runtime_start": Path(args.runtime_start_json),
        "runtime_end": Path(args.runtime_end_json),
        "checkpoint_start": Path(args.checkpoint_start_json),
        "checkpoint_end": Path(args.checkpoint_end_json),
    }
    json_payloads: dict[str, dict[str, Any]] = {}
    for key, path in json_targets.items():
        try:
            json_payloads[key] = load_json_file(path)
        except Exception as exc:  # pragma: no cover - defensive path
            input_errors.append(f"{key} JSON parse failed at {path}: {exc}")
            json_payloads[key] = {}

    report = build_signoff_report(
        run_id=args.run_id,
        run_start_ms=args.run_start_ms,
        verifier_status={key: value["status"] for key, value in log_results.items()},
        content_metrics=log_results["content_metrics"]["payload"],
        content_contract=log_results["content_contract"]["payload"],
        cdc_upsert=log_results["cdc_upsert"]["payload"],
        cdc_invalid=log_results["cdc_invalid"]["payload"],
        cdc_health=log_results["cdc_health"]["payload"],
        runtime_start=json_payloads["runtime_start"],
        runtime_end=json_payloads["runtime_end"],
        checkpoint_start=json_payloads["checkpoint_start"],
        checkpoint_end=json_payloads["checkpoint_end"],
        max_freshness_minutes=args.max_freshness_minutes,
        latency_threshold_minutes=args.latency_threshold_minutes,
        max_content_invalid_rate=args.max_content_invalid_rate,
        max_cdc_invalid_rate=args.max_cdc_invalid_rate,
        input_errors=input_errors,
        checked_at_ms=args.now_ms,
    )

    report_json_path = Path(args.report_json)
    report_md_path = Path(args.report_md)
    report_json_path.parent.mkdir(parents=True, exist_ok=True)
    report_md_path.parent.mkdir(parents=True, exist_ok=True)

    report_json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_md_path.write_text(render_markdown_summary(report), encoding="utf-8")

    if report["status"] == "PASS":
        print("PASS: MIC-38 unified sign-off verifier passed")
        print(json.dumps({"status": report["status"], "report_json": str(report_json_path)}, sort_keys=True))
        return 0

    print("FAIL: MIC-38 unified sign-off verifier failed")
    for reason in report["failure_reasons"]:
        print(f" - {reason}")
    print(json.dumps({"status": report["status"], "report_json": str(report_json_path)}, sort_keys=True))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
