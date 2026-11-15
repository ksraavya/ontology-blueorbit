"""
One exposure model for the economic scenarios.

Before: every handler sized its shock differently (share of total trade,
dependency share, flat constants) with its own severity cut-offs, so
"critical" in one scenario was not comparable to "critical" in another,
and a $574B shock to China rated "low" while a $1B shock to Kyrgyzstan
rated "critical".

Now:
    exposure  = value at risk (USD) / GDP (USD)          -- a share of the economy
    delta     = K x exposure x magnitude                  -- clamped to the 0..1 score range
    severity  = same thresholds for every scenario        -- see SEVERITY_EXPOSURE

GDP comes from IMF WEO (gdp_usd_bn, loaded by modules/indicators), with
the World Bank HAS_GDP series as fallback.

Coefficients (K) are modelling assumptions, documented here in one place
so they can be cited and tuned; scripts/backtest_sanctions.py compares the
model's exposure for the 2022 Russia sanctions with what actually happened
to that trade in the BACI data.
"""
from __future__ import annotations

from typing import Optional

from common.intelligence.normalization import clamp

# Score change per unit of exposure (exposure 1.0 = shock equal to 100% of GDP).
# Calibration anchor: losing trade worth 5% of GDP moves trade vulnerability
# by +0.20 on the 0..1 scale (K_TRADE = 4.0); influence and risk move less.
K_TRADE = 4.0        # trade_vulnerability_score / energy-related trade exposure
K_INFLUENCE = 3.0    # economic_influence_score (negative direction)
K_RISK = 2.5         # global_risk_score
K_ENERGY_DEP = 0.20  # energy_vulnerability per unit of supplier dependency share
ACTOR_DAMPING = 0.5  # the side imposing a measure absorbs it more easily (chose it, can re-route)

# Severity by exposure (share of GDP), identical for every scenario.
SEVERITY_EXPOSURE = (("critical", 0.05), ("high", 0.02), ("medium", 0.005))
MIN_CASCADE_EXPOSURE = 0.001   # 0.1% of GDP: smaller spill-overs are not reported
CASCADE_DAMPING = 0.5          # second-order effects: partners re-route part of the disrupted trade
MAX_EXPOSURE = 0.5             # cap: a single customs record above 50% of GDP is treated as an artefact


def severity_from_exposure(exposure: float) -> str:
    for label, threshold in SEVERITY_EXPOSURE:
        if exposure >= threshold:
            return label
    return "low"


def max_severity(*labels: str) -> str:
    order = ["low", "medium", "high", "critical"]
    return max(labels, key=order.index)


def exposure_delta(exposure: float, k: float, magnitude: float) -> float:
    return clamp(k * min(MAX_EXPOSURE, max(0.0, exposure)) * magnitude)


def gdp_usd(conn, name: Optional[str]) -> Optional[float]:
    """GDP in USD: IMF WEO latest actual, else latest World Bank HAS_GDP."""
    if not name:
        return None
    rows = conn.run_query(
        """
        MATCH (c:Country {name: $n})
        OPTIONAL MATCH (c)-[g:HAS_GDP]->()
        WITH c, g ORDER BY g.year DESC
        RETURN c.gdp_usd_bn AS imf_bn, head(collect(g.value)) AS wb
        """,
        {"n": name},
    )
    if not rows:
        return None
    r = rows[0]
    if r.get("imf_bn"):
        return float(r["imf_bn"]) * 1e9
    return float(r["wb"]) if r.get("wb") else None


def share(value: Optional[float], gdp: Optional[float]) -> float:
    if not value or not gdp or gdp <= 0:
        return 0.0
    return float(value) / float(gdp)
