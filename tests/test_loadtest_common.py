from loadtest.common import Stats, percentile


def test_percentile_matches_known_values():
    samples = [10, 20, 30, 40, 50]
    assert percentile(samples, 0) == 10
    assert percentile(samples, 50) == 30
    assert percentile(samples, 100) == 50


def test_percentile_empty_is_nan():
    result = percentile([], 50)
    assert result != result  # NaN != NaN


def test_stats_summarizes_samples():
    stats = Stats("latency", [1.0, 2.0, 3.0, 4.0, 5.0])
    assert stats.count == 5
    assert stats.avg == 3.0
    assert stats.min == 1.0
    assert stats.max == 5.0
    assert stats.p50 == 3.0
