"""Deterministic primitives for RNG and ID generation."""

from __future__ import annotations

import hashlib
import random
from collections import defaultdict
from typing import DefaultDict


def derive_seed(seed: int, purpose: str) -> int:
    digest = hashlib.sha256(f"{seed}|{purpose}".encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def make_rng(seed: int, purpose: str) -> random.Random:
    return random.Random(derive_seed(seed, purpose))


class DeterministicIdFactory:
    """Generates deterministic IDs tied to run_id and per-entity sequence."""

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self._sequences: DefaultDict[str, int] = defaultdict(int)

    def _make_id(self, entity: str, prefix: str, hex_length: int) -> str:
        seq = self._sequences[entity]
        self._sequences[entity] += 1
        payload = f"{self.run_id}|{entity}|{seq}".encode("utf-8")
        digest = hashlib.sha256(payload).hexdigest()[:hex_length]
        return f"{prefix}{digest}"

    def next_video_id(self) -> str:
        return self._make_id("video", "vid_", 16)

    def next_user_id(self) -> str:
        return self._make_id("user", "usr_", 16)

    def next_event_id(self) -> str:
        return self._make_id("event", "evt_", 20)
