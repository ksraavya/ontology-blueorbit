from __future__ import annotations

from typing import Dict, Iterable

from common.intelligence.aggregation import weighted_sum


"""
Composite Scoring Utilities

Used for:
- Global Risk
- Strategic Influence
- Vulnerability indices

Design:
- Accept named metrics (dict-based)
- Apply weights cleanly
- Ensure consistency across domains
"""


# =========================================================
# CORE FUNCTION
# =========================================================

def weighted_score(
    metrics: Dict[str, float],
    weights: Dict[str, float]
) -> float:
    """
    Compute weighted score from named metrics.

    Example:
        Global Risk =
            trade * 0.4 +
            defense * 0.3 +
            climate * 0.3
    """

    if not metrics:
        return 0.0

    score = 0.0

    for key, value in metrics.items():
        weight = weights.get(key, 0.0)
        score += value * weight

    return score


# =========================================================
# SAFE VARIANT (STRICT MATCH)
# =========================================================

def strict_weighted_score(
    metrics: Dict[str, float],
    weights: Dict[str, float]
) -> float:
    """
    Ensures all required keys exist.

    Raises error if mismatch.
    """

    if set(metrics.keys()) != set(weights.keys()):
        raise ValueError("Metrics and weights keys must match")

    return weighted_score(metrics, weights)


# =========================================================
# NORMALIZED COMPOSITE SCORE
# =========================================================

def normalized_composite_score(
    metrics: Dict[str, float],
    weights: Dict[str, float]
) -> float:
    """
    Compute weighted score and normalize result to 0–1.

    Useful when combining different scales.
    """

    score = weighted_score(metrics, weights)

    max_possible = sum(weights.values())

    if max_possible == 0:
        return 0.0

    return score / max_possible


# =========================================================
# MAX-BASED COMPOSITE (VULNERABILITY STYLE)
# =========================================================

def max_component_score(metrics: Dict[str, float]) -> float:
    """
    Return maximum contributing metric.

    Example:
        System Vulnerability =
        max(trade_dep, energy_dep, climate_risk)
    """

    if not metrics:
        return 0.0

    return max(metrics.values())
