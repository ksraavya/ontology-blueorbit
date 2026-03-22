from __future__ import annotations

from typing import Iterable, List, Optional


"""
Growth Calculations

Used for:
- Defense spending growth
- Trade growth
- Emission trends
- Time-based analysis

Core:
    growth = (current - previous) / previous
"""


# =========================================================
# CORE GROWTH FUNCTION
# =========================================================

def compute_growth(current: float, previous: float) -> float:
    """
    Compute growth rate.

    Returns 0.0 if previous is zero.
    """
    if previous == 0:
        return 0.0

    return (current - previous) / previous


# =========================================================
# PERCENTAGE GROWTH
# =========================================================

def compute_growth_percentage(current: float, previous: float) -> float:
    """
    Return growth as percentage.
    """
    return compute_growth(current, previous) * 100


# =========================================================
# TIME SERIES GROWTH
# =========================================================

def compute_growth_series(values: Iterable[float]) -> List[float]:
    """
    Compute growth for a time series.

    Example:
        [100, 120, 150]

    Output:
        [0.2, 0.25]
    """
    vals: List[float] = list(values)

    if len(vals) < 2:
        return []

    growth_rates: List[float] = []

    for i in range(1, len(vals)):
        growth_rates.append(
            compute_growth(vals[i], vals[i - 1])
        )

    return growth_rates


# =========================================================
# AVERAGE GROWTH
# =========================================================

def average_growth(values: Iterable[float]) -> float:
    """
    Average growth across time series.
    """
    series = compute_growth_series(values)

    if not series:
        return 0.0

    return sum(series) / len(series)


# =========================================================
# COMPOUND GROWTH (OPTIONAL ADVANCED)
# =========================================================

def compound_growth(
    initial: float,
    final: float,
    periods: int
) -> float:
    """
    Compute compound annual growth rate (CAGR).

    Formula:
        (final / initial)^(1/periods) - 1
    """

    if initial <= 0 or periods <= 0:
        return 0.0

    return (final / initial) ** (1 / periods) - 1


# =========================================================
# TREND DIRECTION
# =========================================================

def growth_trend(values: Iterable[float]) -> str:
    """
    Identify trend direction.

    Returns:
        "increasing", "decreasing", or "stable"
    """
    series = compute_growth_series(values)

    if not series:
        return "stable"

    avg = sum(series) / len(series)

    if avg > 0:
        return "increasing"
    elif avg < 0:
        return "decreasing"
    else:
        return "stable"