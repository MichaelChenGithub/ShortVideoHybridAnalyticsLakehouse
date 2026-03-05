"""Artifact writing for run config, video registry, and expected actions."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from .config import RunConfig


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    return value


def _write_parquet_or_jsonl(path: Path, rows: List[Dict[str, Any]]) -> str:
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore

        if rows:
            table = pa.Table.from_pylist(rows)
        else:
            table = pa.table({"_empty": []})
        pq.write_table(table, path)
        return "parquet"
    except ImportError:
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, default=_json_default, separators=(",", ":")) + "\n")
        return "jsonl_fallback"


def write_run_artifacts(
    artifacts_root: Path,
    config: RunConfig,
    video_registry_rows: List[Dict[str, Any]],
    expected_action_rows: List[Dict[str, Any]],
    run_summary: Dict[str, Any],
) -> Dict[str, Any]:
    run_dir = artifacts_root / config.run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    run_config_path = run_dir / "run_config.json"
    with run_config_path.open("w", encoding="utf-8") as handle:
        json.dump(config.to_serializable(), handle, indent=2, sort_keys=True)

    video_registry_path = run_dir / "video_registry.parquet"
    expected_actions_path = run_dir / "expected_actions.parquet"

    video_registry_format = _write_parquet_or_jsonl(video_registry_path, video_registry_rows)
    expected_actions_format = _write_parquet_or_jsonl(expected_actions_path, expected_action_rows)

    run_summary_path = run_dir / "run_summary.json"
    with run_summary_path.open("w", encoding="utf-8") as handle:
        json.dump(run_summary, handle, indent=2, sort_keys=True)

    return {
        "run_dir": str(run_dir),
        "run_config": str(run_config_path),
        "video_registry": str(video_registry_path),
        "expected_actions": str(expected_actions_path),
        "run_summary": str(run_summary_path),
        "video_registry_format": video_registry_format,
        "expected_actions_format": expected_actions_format,
    }
