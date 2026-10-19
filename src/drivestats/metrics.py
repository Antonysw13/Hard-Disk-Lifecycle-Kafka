"""Tasa de falla anualizada (AFR) e intervalo de confianza, en Python puro."""

from __future__ import annotations

import math

DAYS_PER_YEAR = 365


def afr_pct(failures: float, drive_days: int) -> float:
    """AFR (%) = fallas ÷ (días-disco ÷ 365) × 100, la fórmula que usa Backblaze."""
    return failures / (drive_days / DAYS_PER_YEAR) * 100


def _poisson_cdf(k: int, mu: float) -> float:
    """P(X ≤ k) para X ~ Poisson(mu), sumando en escala logarítmica para no desbordar."""
    if k < 0:
        return 0.0
    if mu == 0:
        return 1.0
    logs = [i * math.log(mu) - mu - math.lgamma(i + 1) for i in range(k + 1)]
    peak = max(logs)
    return min(1.0, math.exp(peak) * sum(math.exp(x - peak) for x in logs))


def _solve_decreasing(f, target: float, lo: float, hi: float) -> float:
    """Bisección de f(mu) = target para f decreciente en [lo, hi]."""
    for _ in range(200):
        mid = (lo + hi) / 2
        if f(mid) > target:
            lo = mid
        else:
            hi = mid
        if hi - lo < 1e-12 * max(1.0, hi):
            break
    return (lo + hi) / 2


def poisson_ci(events: int, confidence: float = 0.95) -> tuple[float, float]:
    """Intervalo exacto (Garwood) para la media de una Poisson con `events` observados."""
    alpha = 1 - confidence
    hi = events + 10 * math.sqrt(events + 1) + 10
    while _poisson_cdf(events, hi) > alpha / 2:
        hi *= 2
    upper = _solve_decreasing(lambda mu: _poisson_cdf(events, mu), alpha / 2, 0.0, hi)
    lower = 0.0
    if events > 0:
        lower = _solve_decreasing(
            lambda mu: _poisson_cdf(events - 1, mu), 1 - alpha / 2, 0.0, float(events)
        )
    return lower, upper


def afr_ci95_pct(failures: int, drive_days: int) -> tuple[float, float]:
    """IC del 95 % de la AFR: con pocas fallas o pocos días-disco el intervalo se abre mucho."""
    lower, upper = poisson_ci(failures)
    return afr_pct(lower, drive_days), afr_pct(upper, drive_days)
