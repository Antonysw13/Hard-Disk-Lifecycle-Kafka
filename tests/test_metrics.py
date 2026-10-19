import pytest

from drivestats.metrics import afr_ci95_pct, afr_pct, poisson_ci


def test_afr_reproduces_backblaze_q2_2026():
    # Cifras del enunciado: 1.498 fallas y 31.553.350 días-disco → 1,73 %
    assert round(afr_pct(1498, 31_553_350), 2) == 1.73


# Valores de referencia: chi2.ppf(0.025, 2k) / 2 y chi2.ppf(0.975, 2k + 2) / 2
@pytest.mark.parametrize(
    ("events", "exact_lower", "exact_upper"),
    [
        (0, 0.0, 3.688879),
        (1, 0.025318, 5.571643),
        (10, 4.795389, 18.390356),
        (100, 81.363569, 121.625995),
    ],
)
def test_poisson_ci_matches_chi_square(events, exact_lower, exact_upper):
    lower, upper = poisson_ci(events)
    assert lower == pytest.approx(exact_lower, rel=1e-5)
    assert upper == pytest.approx(exact_upper, rel=1e-5)


def test_poisson_ci_handles_many_events():
    lower, upper = poisson_ci(1498)
    assert lower < 1498 < upper
    assert upper - lower == pytest.approx(2 * 1.96 * 1498**0.5, rel=0.02)


def test_afr_ci_contains_point_estimate():
    lower, upper = afr_ci95_pct(5, 70_000)
    assert lower < afr_pct(5, 70_000) < upper
