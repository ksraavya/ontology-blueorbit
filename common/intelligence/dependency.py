from __future__ import annotations

from typing import Iterable, Dict, List

from common.intelligence.normalization import normalize


"""
Dependency Calculations

Used for:
- Trade dependency
- Energy dependency
- Arms dependency
- Resource dependency

Core idea:
    dependency = value / total
"""


# =========================================================
# CORE DEPENDENCY
# =========================================================

def compute_dependency(value: float, total: float) -> float:
    """
    Compute dependency of A on B.

    Example:
        trade(A→B) / total_trade(A)
    """
    return normalize(value, total)


# =========================================================
# BULK DEPENDENCY (FOR MULTIPLE TARGETS)
# =========================================================

def compute_dependency_distribution(
    values: Dict[str, float]
) -> Dict[str, float]:
    """
    Compute dependency distribution across multiple targets.

    Example:
        {
            "USA": 100,
            "China": 300
        }

    Output:
        {
            "USA": 0.25,
            "China": 0.75
        }
    """

    total = sum(values.values())

    if total == 0:
        return {k: 0.0 for k in values}

    return {
        k: v / total
        for k, v in values.items()
    }


# =========================================================
# MAX DEPENDENCY (VULNERABILITY)
# =========================================================

def max_dependency(dependencies: Iterable[float]) -> float:
    """
    Return highest dependency value.

    Used in:
        vulnerability(A) = max dependency
    """
    deps: List[float] = list(dependencies)
    if not deps:
        return 0.0
    return max(deps)


# =========================================================
# DIVERSIFICATION SCORE
# =========================================================

def diversification_score(dependencies: Iterable[float]) -> float:
    """
    Compute diversification.

    Formula:
        1 - max_dependency
    """
    max_dep = max_dependency(dependencies)
    return 1.0 - max_dep


# =========================================================
# THRESHOLD CHECK (OPTIONAL)
# =========================================================

def is_high_dependency(
    dependency: float,
    threshold: float
) -> bool:
    """
    Check if dependency exceeds threshold.

    Example:
        > 0.7 → high dependency
    """
    return dependency >= threshold
