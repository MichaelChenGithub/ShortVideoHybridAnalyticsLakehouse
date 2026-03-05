"""Clock abstractions for deterministic and wall-clock execution."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


class Clock:
    def now(self) -> datetime:
        raise NotImplementedError

    def sleep(self, seconds: float) -> None:
        raise NotImplementedError


@dataclass
class SimulatedClock(Clock):
    current: datetime

    def __post_init__(self) -> None:
        if self.current.tzinfo is None:
            self.current = self.current.replace(tzinfo=timezone.utc)
        else:
            self.current = self.current.astimezone(timezone.utc)

    def now(self) -> datetime:
        return self.current

    def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            return
        self.current = self.current + timedelta(seconds=seconds)


class RealClock(Clock):
    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            return
        time.sleep(seconds)
