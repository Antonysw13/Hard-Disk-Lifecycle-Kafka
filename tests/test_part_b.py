import pytest

from part_b.balancing import concentration, key_skew_floor, partition_stats, time_stats


def test_partition_stats_perfect_balance():
    stats = partition_stats([10, 10, 10, 10])
    assert stats["max_over_mean"] == 1.0
    assert stats["cv"] == 0.0
    assert stats["empty"] == 0


def test_partition_stats_counts_empty_partitions_and_skew():
    stats = partition_stats([30, 10, 0, 0])
    assert stats["empty"] == 2
    assert stats["mean_rows"] == 10
    assert stats["max_over_mean"] == 3.0


@pytest.mark.parametrize(
    ("largest", "total", "partitions", "expected"),
    [
        (50, 100, 2, 1.0),  # la clave mayor cabe justo en una partición ideal
        (50, 100, 8, 4.0),  # con 8 particiones la ideal es 12,5 y la clave tiene 50
        (1, 100, 2, 1.0),  # nunca baja de 1
    ],
)
def test_key_skew_floor(largest, total, partitions, expected):
    assert key_skew_floor(largest, total, partitions) == expected


def test_concentration_shares_and_keys_needed():
    conc = concentration({"a": 50, "b": 30, "c": 10, "d": 10}, tops=(1, 3))
    assert conc["top_share_pct"] == {1: 50.0, 3: 90.0}
    assert conc["keys_for_pct"] == {50: 1, 80: 2, 90: 3}


def test_time_stats_single_run_has_zero_stdev():
    assert time_stats([2.0]) == {"median_s": 2.0, "min_s": 2.0, "max_s": 2.0, "stdev_s": 0.0}
