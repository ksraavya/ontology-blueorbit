from __future__ import annotations
import os
from typing import Dict, Any


"""
Global configuration for Intelligence Engine.

Responsibilities:
- Centralized constants
- Edge schema standardization
- Metric weights
- Thresholds
"""


# =========================================================
# HELPERS
# =========================================================

def _get_float_env(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except (TypeError, ValueError):
        return default


# =========================================================
# GENERAL SETTINGS
# =========================================================

DEFAULT_YEAR_FORMAT: str = os.getenv("DEFAULT_YEAR_FORMAT", "%Y")
DEFAULT_CURRENCY: str = os.getenv("DEFAULT_CURRENCY", "USD")
DEFAULT_CONFIDENCE: float = _get_float_env("DEFAULT_CONFIDENCE", 0.8)


# =========================================================
# EDGE SCHEMA (🔥 ENFORCED EVERYWHERE)
# =========================================================

EDGE_SCHEMA: Dict[str, Any] = {
    "value": 0.0,
    "normalized_weight": 0.0,
    "year": None,
    "confidence": DEFAULT_CONFIDENCE,
}


# =========================================================
# ANALYTICS THRESHOLDS
# =========================================================

DEFAULT_HIGH_DEPENDENCY_THRESHOLD: float = _get_float_env(
    "DEFAULT_HIGH_DEPENDENCY_THRESHOLD", 0.7
)

DEFAULT_CONFLICT_RISK_THRESHOLD: float = _get_float_env(
    "DEFAULT_CONFLICT_RISK_THRESHOLD", 0.7
)

DEFAULT_INFLUENCE_THRESHOLD: float = _get_float_env(
    "DEFAULT_INFLUENCE_THRESHOLD", 0.7
)


# =========================================================
# CROSS-DOMAIN WEIGHTS (🔥 IMPORTANT)
# =========================================================

GLOBAL_RISK_WEIGHTS: Dict[str, float] = {
    "trade": 0.4,
    "defense": 0.3,
    "climate": 0.3,
}

INFLUENCE_WEIGHTS: Dict[str, float] = {
    "economic": 0.4,
    "defense": 0.3,
    "geopolitical": 0.3,
}


# =========================================================
# VALIDATION
# =========================================================

def validate_weights(weights: Dict[str, float]) -> None:
    total = sum(weights.values())
    if not (0.99 <= total <= 1.01):
        raise ValueError(f"Weights must sum to 1. Got: {total}")
