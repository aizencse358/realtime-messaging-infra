import os
import uuid

GATEWAY_URL = os.getenv("GATEWAY_URL", "ws://localhost:8080")


def ws_url(user_id: str) -> str:
    return f"{GATEWAY_URL}/ws/{user_id}"


def new_user_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def percentile(samples: list[float], pct: float) -> float:
    if not samples:
        return float("nan")
    s = sorted(samples)
    k = (len(s) - 1) * (pct / 100)
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


class Stats:
    """Summary stats for one measured metric, in milliseconds."""

    def __init__(self, name: str, samples_ms: list[float]):
        self.name = name
        self.samples_ms = samples_ms

    @property
    def count(self) -> int:
        return len(self.samples_ms)

    @property
    def avg(self) -> float:
        return sum(self.samples_ms) / len(self.samples_ms) if self.samples_ms else float("nan")

    @property
    def min(self) -> float:
        return min(self.samples_ms) if self.samples_ms else float("nan")

    @property
    def max(self) -> float:
        return max(self.samples_ms) if self.samples_ms else float("nan")

    @property
    def p50(self) -> float:
        return percentile(self.samples_ms, 50)

    @property
    def p95(self) -> float:
        return percentile(self.samples_ms, 95)

    @property
    def p99(self) -> float:
        return percentile(self.samples_ms, 99)


def print_table(stats: list[Stats], throughputs: dict[str, float] | None = None) -> None:
    header = f"{'Metric':<38}{'avg(ms)':>10}{'min(ms)':>10}{'p50(ms)':>10}{'p95(ms)':>10}{'p99(ms)':>10}{'max(ms)':>10}{'n':>8}"
    print(header)
    print("-" * len(header))
    for s in stats:
        print(
            f"{s.name:<38}{s.avg:>10.3f}{s.min:>10.3f}{s.p50:>10.3f}{s.p95:>10.3f}{s.p99:>10.3f}{s.max:>10.3f}{s.count:>8d}"
        )
    if throughputs:
        print()
        for name, val in throughputs.items():
            print(f"{name}: {val:.2f}")
