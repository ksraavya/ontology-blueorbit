from __future__ import annotations

from typing import Iterable, List, Dict


"""
Normalization Utilities

Used for:
- Scaling values to comparable range
- Dependency calculations
- Weight normalization

Core:
    normalized = value / reference
"""


# =========================================================
# BASIC SAFE DIVISION
# =========================================================

def safe_divide(a: float, b: float) -> float:
    """
    Safe division.

    Returns 0.0 if denominator is zero.
    """
    if b == 0:
        return 0.0
    return a / b


# =========================================================
# NORMALIZATION (TOTAL BASED)
# =========================================================

def normalize(value: float, total: float) -> float:
    """
    Normalize by total.

    Example:
        dependency = value / total
    """
    return safe_divide(value, total)


# =========================================================
# NORMALIZATION (MAX BASED)
# =========================================================

def normalize_by_max(value: float, max_value: float) -> float:
    """
    Normalize by maximum value.

    Example:
        trade_weight = value / max_trade
    """
    return safe_divide(value, max_value)


# =========================================================
# NORMALIZE LIST (BY TOTAL)
# =========================================================

def normalize_distribution(values: Iterable[float]) -> List[float]:
    """
    Normalize list so sum = 1.

    Example:
        [100, 300] → [0.25, 0.75]
    """
    vals = list(values)
    total = sum(vals)

    if total == 0:
        return [0.0 for _ in vals]

    return [v / total for v in vals]


# =========================================================
# NORMALIZE DICTIONARY (BY TOTAL)
# =========================================================

def normalize_dict(values: Dict[str, float]) -> Dict[str, float]:
    """
    Normalize dictionary values.

    Example:
        {"A": 100, "B": 300}
        → {"A": 0.25, "B": 0.75}
    """
    total = sum(values.values())

    if total == 0:
        return {k: 0.0 for k in values}

    return {
        k: v / total
        for k, v in values.items()
    }


# =========================================================
# MIN-MAX NORMALIZATION
# =========================================================

def min_max_normalize(value: float, min_value: float, max_value: float) -> float:
    """
    Scale value to 0–1 range using min-max scaling.

    Formula:
        (value - min) / (max - min)
    """
    if max_value == min_value:
        return 0.0

    return (value - min_value) / (max_value - min_value)


# =========================================================
# CLAMP VALUE
# =========================================================

def clamp(value: float, min_val: float = 0.0, max_val: float = 1.0) -> float:
    """
    Restrict value to given range.
    """
    return max(min_val, min(value, max_val))