from __future__ import annotations

from typing import Iterable, List, Optional


"""
Aggregation Utilities

Provides:
- sum
- average
- max / min
- count

Used across:
- economy
- defense
- climate
- geopolitics

Design:
- Safe (handles empty inputs)
- Pure (no side effects)
"""


# =========================================================
# BASIC AGGREGATIONS
# =========================================================

def sum_values(values: Iterable[float]) -> float:
    """
    Sum of values.

    Example:
        total trade, total spending
    """
    return float(sum(values))


def count_values(values: Iterable[float]) -> int:
    """
    Count number of elements.
    """
    return len(list(values))


def average(values: Iterable[float]) -> float:
    """
    Compute average safely.

    Returns 0.0 if empty.
    """
    vals: List[float] = list(values)
    if not vals:
        return 0.0
    return sum(vals) / len(vals)


# =========================================================
# EXTREME VALUES
# =========================================================

def max_value(values: Iterable[float]) -> float:
    """
    Return max value safely.

    Used in:
    - vulnerability (max dependency)
    """
    vals: List[float] = list(values)
    if not vals:
        return 0.0
    return max(vals)


def min_value(values: Iterable[float]) -> float:
    """
    Return min value safely.
    """
    vals: List[float] = list(values)
    if not vals:
        return 0.0
    return min(vals)


# =========================================================
# NORMALIZED AGGREGATIONS
# =========================================================

def average_normalized(values: Iterable[float], max_value_ref: float) -> float:
    """
    Normalize each value by max reference, then average.

    Example:
        severity normalization
    """
    vals: List[float] = list(values)
    if not vals or max_value_ref == 0:
        return 0.0

    normalized = [v / max_value_ref for v in vals]
    return sum(normalized) / len(normalized)


# =========================================================
# ADVANCED UTILITIES
# =========================================================

def weighted_sum(values: Iterable[float], weights: Iterable[float]) -> float:
    """
    Compute weighted sum.

    Example:
        composite metrics
    """
    vals = list(values)
    wts = list(weights)

    if len(vals) != len(wts):
        raise ValueError("Values and weights must have same length")

    return sum(v * w for v, w in zip(vals, wts))


def weighted_average(values: Iterable[float], weights: Iterable[float]) -> float:
    """
    Compute weighted average safely.
    """
    vals = list(values)
    wts = list(weights)

    if not vals or not wts or sum(wts) == 0:
        return 0.0

    return weighted_sum(vals, wts) / sum(wts)
