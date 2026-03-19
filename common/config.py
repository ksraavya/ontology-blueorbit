# ===== common/config.py =====

from __future__ import annotations

import os


"""
Global configuration shared across modules.

- Values can be overridden via environment variables
- Keep this file simple and minimal
"""


# ==============================
# HELPERS
# ==============================

def _get_float_env(key: str, default: float) -> float:
    """
    Safely parse float environment variables.
    Falls back to default if parsing fails.
    """
    try:
        return float(os.getenv(key, str(default)))
    except (TypeError, ValueError):
        return default


# ==============================
# FORMATTING
# ==============================

DEFAULT_YEAR_FORMAT: str = os.getenv("DEFAULT_YEAR_FORMAT", "%Y")
DEFAULT_CURRENCY: str = os.getenv("DEFAULT_CURRENCY", "USD")


# ==============================
# ANALYTICS THRESHOLDS
# ==============================

DEFAULT_HIGH_DEPENDENCY_THRESHOLD: float = _get_float_env(
    "DEFAULT_HIGH_DEPENDENCY_THRESHOLD", 0.7
)

DEFAULT_CONFLICT_RISK_THRESHOLD: float = _get_float_env(
    "DEFAULT_CONFLICT_RISK_THRESHOLD", 0.7
)

DEFAULT_INFLUENCE_SCORE_THRESHOLD: float = _get_float_env(
    "DEFAULT_INFLUENCE_SCORE_THRESHOLD", 0.7
)