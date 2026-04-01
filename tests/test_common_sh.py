from __future__ import annotations

import re
import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
HELPER = REPO_ROOT / "src" / "scripts" / "common.sh"


class CommonShellTests(unittest.TestCase):
    def _run(self, script: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["/bin/bash", "-lc", script],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_deterministic_mode_uses_config_default(self) -> None:
        result = self._run(
            f"""
            source "{HELPER}"
            unset BOUNDED_RUN_TIME_MODE
            unset BOUNDED_RUN_STARTED_AT
            resolve_bounded_run_started_at "TEST"
            printf 'EFFECTIVE=%s\\n' "${{BOUNDED_RUN_EFFECTIVE_STARTED_AT:-}}"
            """
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("bounded_run_time_mode=deterministic started_at=config_default", result.stdout)
        self.assertIn("EFFECTIVE=", result.stdout)

    def test_dynamic_mode_generates_utc_started_at(self) -> None:
        result = self._run(
            f"""
            source "{HELPER}"
            export BOUNDED_RUN_TIME_MODE=dynamic
            unset BOUNDED_RUN_STARTED_AT
            resolve_bounded_run_started_at "TEST"
            printf 'EFFECTIVE=%s\\n' "$BOUNDED_RUN_EFFECTIVE_STARTED_AT"
            """
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        match = re.search(r"EFFECTIVE=([0-9T:\\-]+Z)", result.stdout)
        self.assertIsNotNone(match, msg=result.stdout)

    def test_explicit_override_takes_precedence(self) -> None:
        expected = "2026-03-20T14:00:00Z"
        result = self._run(
            f"""
            source "{HELPER}"
            export BOUNDED_RUN_TIME_MODE=dynamic
            export BOUNDED_RUN_STARTED_AT={expected}
            resolve_bounded_run_started_at "TEST"
            printf 'EFFECTIVE=%s\\n' "$BOUNDED_RUN_EFFECTIVE_STARTED_AT"
            """
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn(f"EFFECTIVE={expected}", result.stdout)

    def test_invalid_mode_fails_fast(self) -> None:
        result = self._run(
            f"""
            source "{HELPER}"
            export BOUNDED_RUN_TIME_MODE=fast
            resolve_bounded_run_started_at "TEST"
            """
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("BOUNDED_RUN_TIME_MODE must be deterministic or dynamic", result.stderr)

    def test_invalid_override_fails_fast(self) -> None:
        result = self._run(
            f"""
            source "{HELPER}"
            export BOUNDED_RUN_STARTED_AT=not-a-time
            resolve_bounded_run_started_at "TEST"
            """
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("BOUNDED_RUN_STARTED_AT must be valid ISO-8601 timestamp", result.stderr)


if __name__ == "__main__":
    unittest.main()
