from __future__ import annotations

import json
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator


@dataclass
class MetricsCollector:
    run_id: str
    started_at_epoch: float = field(default_factory=time.time)
    counters: dict[str, int] = field(default_factory=dict)
    timings: list[dict[str, Any]] = field(default_factory=list)
    attributes: dict[str, Any] = field(default_factory=dict)

    def increment(self, name: str, amount: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + amount

    def record_attribute(self, name: str, value: Any) -> None:
        self.attributes[name] = value

    @contextmanager
    def track(self, step: str, **context: Any) -> Iterator[None]:
        start = time.perf_counter()
        status = "success"
        try:
            yield
        except Exception:
            status = "failure"
            raise
        finally:
            duration_seconds = round(time.perf_counter() - start, 4)
            entry = {"step": step, "status": status, "duration_seconds": duration_seconds}
            if context:
                entry["context"] = context
            self.timings.append(entry)

    def write(self, metrics_dir: Path) -> Path:
        metrics_dir.mkdir(parents=True, exist_ok=True)
        metrics_path = metrics_dir / f"run_{self.run_id}.json"
        payload = {
            "run_id": self.run_id,
            "started_at_epoch": self.started_at_epoch,
            "finished_at_epoch": time.time(),
            "counters": self.counters,
            "timings": self.timings,
            "attributes": self.attributes,
        }
        metrics_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return metrics_path
