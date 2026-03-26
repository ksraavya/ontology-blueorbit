from __future__ import annotations

import logging

from common.db import Neo4jConnection
from common.intelligence.normalization import clamp
from simulation.models import (
    AffectedCountry,
    CascadeEffect,
    ScenarioRequest,
    ScenarioResult,
    ScenarioType,
    ScoreDelta,
)

logger = logging.getLogger(__name__)


def _run(conn: Neo4jConnection, query: str, params: dict | None = None) -> list[dict]:
    return conn.run_query(query, params or {})


def _make_delta(score_name: str, current: float | None, delta: float, confidence: float = 0.8) -> ScoreDelta:
    c = current or 0.0
    projected = clamp(c + delta)
    return ScoreDelta(
        score_name=score_name,
        current=round(c, 4),
        delta=round(delta, 4),
        projected=round(projected, 4),
        direction="increase" if delta > 0.001 else "decrease" if delta < -0.001 else "unchanged",
        confidence=confidence,
    )


def _severity(score: float) -> str:
    if score >= 0.6:   return "critical"
    if score >= 0.4:   return "high"
    if score >= 0.2:   return "medium"
    return "low"


# =========================================================
# GDP SHOCK
# =========================================================

def run_gdp_shock(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: major GDP contraction or growth in a target country.

    Logic:
    - Target's economic_power_score falls/rises sharply
    - Target's economic_influence_score contracts
    - Trade partners with high dependency on target face spillover
    - If contraction: global_risk_score rises; if growth: opportunity for partners

    Reads:
        HAS_GDP, HAS_TRADE_DEPENDENCY_ON, HAS_TRADE_VOLUME_WITH
        economic_power_score, economic_influence_score, trade_vulnerability_score
    """
    target = req.target or req.actor
    mag    = req.magnitude
    year   = req.year

    # magnitude interpretation: < 1.0 = contraction, >= 1.0 = growth shock
    is_contraction = req.extra_params.get("direction", "contraction") != "growth"

    if not target:
        return _empty_result(req, "gdp_shock requires a target country")

    conn = Neo4jConnection()
    try:
        target_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.economic_power_score AS econ_power,
                   c.economic_influence_score AS econ_influence,
                   c.trade_vulnerability_score AS trade_vuln,
                   c.global_risk_score AS global_risk
        """, {"name": target})
        tr = target_rows[0] if target_rows else {}

        gdp_rows = _run(conn, """
            MATCH (c:Country {name: $name})-[r:HAS_GDP]->(:Metric)
            WHERE r.year = $year
            RETURN r.value AS gdp_usd
        """, {"name": target, "year": year})
        gdp_usd = float(gdp_rows[0].get("gdp_usd") or 0.0) if gdp_rows else 0.0

        if is_contraction:
            econ_power_delta   = -clamp(0.20 * mag)
            econ_influence_delta = -clamp(0.15 * mag)
            risk_delta         = clamp(0.20 * mag)
            trade_vuln_delta   = clamp(0.10 * mag)
        else:
            econ_power_delta   = clamp(0.15 * mag)
            econ_influence_delta = clamp(0.10 * mag)
            risk_delta         = -clamp(0.05 * mag)
            trade_vuln_delta   = -clamp(0.05 * mag)

        direction_str = "contraction" if is_contraction else "expansion"

        target_affected = AffectedCountry(
            country=target,
            impact_type="direct",
            severity=_severity(abs(econ_power_delta) * mag),
            score_deltas=[
                _make_delta("economic_power_score",     tr.get("econ_power"),     econ_power_delta),
                _make_delta("economic_influence_score", tr.get("econ_influence"), econ_influence_delta),
                _make_delta("global_risk_score",        tr.get("global_risk"),    risk_delta),
                _make_delta("trade_vulnerability_score",tr.get("trade_vuln"),     trade_vuln_delta),
            ],
            exposure_usd=gdp_usd * abs(econ_power_delta) if gdp_usd else None,
            summary=(
                f"{target} faces major GDP {direction_str}; "
                f"economic power score {'falls' if is_contraction else 'rises'} "
                f"by {abs(econ_power_delta)*100:.1f} points."
            ),
        )

        # Trade partner cascade
        partner_rows = _run(conn, """
            MATCH (other:Country)-[r:HAS_TRADE_DEPENDENCY_ON]->(c:Country {name: $target})
            WHERE r.year = $year AND r.dependency >= 0.08
            RETURN other.name AS country,
                   r.dependency AS dependency,
                   other.trade_vulnerability_score AS trade_vuln
            ORDER BY r.dependency DESC
            LIMIT 10
        """, {"target": target, "year": year})

        partner_affected: list[AffectedCountry] = []
        for row in partner_rows:
            country = row.get("country")
            if not country:
                continue
            dep   = float(row.get("dependency") or 0.0)
            spill = clamp(dep * abs(econ_power_delta) * 0.4)
            if spill < 0.01:
                continue
            partner_affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity=_severity(spill),
                score_deltas=[
                    _make_delta("trade_vulnerability_score", row.get("trade_vuln"),
                                spill if is_contraction else -spill * 0.5, 0.65),
                ],
                summary=(
                    f"{country} has {dep*100:.0f}% trade exposure to {target}; "
                    f"GDP {direction_str} {'reduces' if is_contraction else 'expands'} demand for exports."
                ),
            ))

        cascades: list[CascadeEffect] = [
            CascadeEffect(
                mechanism=f"GDP {direction_str}",
                affected=target,
                severity=_severity(abs(econ_power_delta) * mag),
                description=(
                    f"{'Contracting' if is_contraction else 'Expanding'} output "
                    f"{'reduces imports and investment' if is_contraction else 'boosts import demand and regional growth'}."
                ),
            ),
            CascadeEffect(
                mechanism="Trade partner spillover",
                affected=f"{len(partner_rows)} dependent economies",
                severity="medium" if partner_rows else "low",
                description=(
                    f"{target}'s GDP {'decline' if is_contraction else 'growth'} "
                    f"{'reduces' if is_contraction else 'increases'} demand for partner exports."
                ),
            ),
        ]

        return ScenarioResult(
            scenario_type=ScenarioType.GDP_SHOCK,
            actor=req.actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=[target_affected] + partner_affected,
            cascade_effects=cascades,
            data_sources=["HAS_GDP", "HAS_TRADE_DEPENDENCY_ON", "economic_power_score"],
            confidence=0.80 if gdp_usd > 0 else 0.55,
        )
    finally:
        conn.close()


# =========================================================
# DEBT CRISIS
# =========================================================

def run_debt_crisis(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: sovereign default, currency collapse, or IMF bailout in target.

    Logic:
    - Target's economic_power_score and economic_influence_score collapse
    - trade_vulnerability_score rises sharply (reduced FX reserves)
    - global_risk_score spikes — political instability follows financial crisis
    - Creditor countries face write-down losses (cascade)
    - Regional contagion risk elevates neighbors

    Reads:
        HAS_GDP, HAS_TRADE_VOLUME_WITH, BELONGS_TO,
        economic_power_score, global_risk_score, trade_vulnerability_score
    """
    target = req.target or req.actor
    mag    = req.magnitude
    year   = req.year

    if not target:
        return _empty_result(req, "debt_crisis requires a target country")

    conn = Neo4jConnection()
    try:
        target_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.economic_power_score AS econ_power,
                   c.economic_influence_score AS econ_influence,
                   c.trade_vulnerability_score AS trade_vuln,
                   c.global_risk_score AS global_risk,
                   c.political_stability_score AS political_stability
        """, {"name": target})
        tr = target_rows[0] if target_rows else {}

        # Debt crisis deltas — significant deterioration
        econ_power_delta   = -clamp(0.25 * mag)
        econ_influence_delta = -clamp(0.20 * mag)
        trade_vuln_delta   = clamp(0.35 * mag)
        risk_delta         = clamp(0.30 * mag)
        stability_delta    = -clamp(0.25 * mag)

        target_affected = AffectedCountry(
            country=target,
            impact_type="direct",
            severity=_severity(abs(econ_power_delta) * mag * 1.5),
            score_deltas=[
                _make_delta("economic_power_score",      tr.get("econ_power"),        econ_power_delta),
                _make_delta("economic_influence_score",  tr.get("econ_influence"),    econ_influence_delta),
                _make_delta("trade_vulnerability_score", tr.get("trade_vuln"),        trade_vuln_delta),
                _make_delta("global_risk_score",         tr.get("global_risk"),       risk_delta),
                _make_delta("political_stability_score", tr.get("political_stability"), stability_delta),
            ],
            summary=(
                f"{target} enters debt crisis; sovereign default risk forces "
                f"emergency financing. Currency depreciation amplifies import costs."
            ),
        )

        # Regional neighbors face contagion
        region_rows = _run(conn, """
            MATCH (c:Country {name: $target})-[:BELONGS_TO]->(r:Region)<-[:BELONGS_TO]-(neighbor:Country)
            WHERE neighbor.name <> $target
            RETURN neighbor.name AS country,
                   neighbor.trade_vulnerability_score AS trade_vuln,
                   neighbor.global_risk_score AS global_risk
            LIMIT 6
        """, {"target": target})

        neighbor_affected: list[AffectedCountry] = []
        for row in region_rows:
            country = row.get("country")
            if not country:
                continue
            contagion = clamp(0.08 * mag)
            neighbor_affected.append(AffectedCountry(
                country=country,
                impact_type="regional",
                severity="low",
                score_deltas=[
                    _make_delta("global_risk_score",         row.get("global_risk"),  contagion, 0.5),
                    _make_delta("trade_vulnerability_score", row.get("trade_vuln"),   clamp(contagion * 0.5), 0.5),
                ],
                summary=f"{country} faces regional contagion from {target}'s debt crisis.",
            ))

        cascades = [
            CascadeEffect(
                mechanism="Currency crisis",
                affected=target,
                severity="critical",
                description=(
                    f"{target}'s currency depreciates sharply; import costs spike "
                    f"and external debt burden surges in local currency terms."
                ),
            ),
            CascadeEffect(
                mechanism="Credit market freeze",
                affected=f"{target} and regional neighbors",
                severity="high",
                description=(
                    "Sovereign risk premium rises across the region; "
                    "private sector borrowing costs increase for regional peers."
                ),
            ),
            CascadeEffect(
                mechanism="IMF conditionality",
                affected=target,
                severity="medium",
                description=(
                    "Austerity measures required for IMF access compress domestic "
                    "demand and constrain government investment for 2-5 years."
                ),
            ),
        ]

        return ScenarioResult(
            scenario_type=ScenarioType.DEBT_CRISIS,
            actor=req.actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=[target_affected] + neighbor_affected,
            cascade_effects=cascades,
            data_sources=["HAS_GDP", "BELONGS_TO", "economic_power_score", "global_risk_score"],
            confidence=0.75,
        )
    finally:
        conn.close()


# =========================================================
# EXPORT BAN
# =========================================================

def run_export_ban(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: actor bans export of a strategic commodity (rare earths, chips, grain) to target.

    Logic:
    - Target faces supply shock in the specific commodity
    - If commodity = strategic tech: target's economic competitiveness falls
    - If commodity = food/grain: target's social stability at risk
    - Actor loses export revenue but gains leverage
    - Third countries that also import this commodity from actor face disruption

    Reads:
        EXPORTS_TO, HAS_TRADE_DEPENDENCY_ON, IMPORTS_ENERGY_FROM,
        trade_vulnerability_score, economic_influence_score
    """
    actor     = req.actor
    target    = req.target
    mag       = req.magnitude
    year      = req.year
    commodity = req.extra_params.get("commodity", "strategic goods")

    if not actor or not target:
        return _empty_result(req, "export_ban requires both actor and target")

    conn = Neo4jConnection()
    try:
        # How much does actor export to target overall
        export_rows = _run(conn, """
            MATCH (a:Country {name: $actor})-[r:EXPORTS_TO]->(t:Country {name: $target})
            WHERE r.year = $year
            RETURN r.value AS export_value, r.dependency AS dependency
        """, {"actor": actor, "target": target, "year": year})
        export_value = float(export_rows[0].get("export_value") or 0.0) if export_rows else 0.0
        actor_dep    = float(export_rows[0].get("dependency") or 0.0) if export_rows else 0.0

        target_scores = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.trade_vulnerability_score AS trade_vuln,
                   c.economic_influence_score AS econ_influence,
                   c.global_risk_score AS global_risk,
                   c.economic_power_score AS econ_power
        """, {"name": target})
        ts = target_scores[0] if target_scores else {}

        actor_scores = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.economic_influence_score AS econ_influence,
                   c.trade_vulnerability_score AS trade_vuln
        """, {"name": actor})
        as_ = actor_scores[0] if actor_scores else {}

        # Severity depends on commodity type
        commodity_lower = commodity.lower()
        if any(k in commodity_lower for k in ["rare earth", "chip", "semiconductor", "tech"]):
            commodity_severity_mult = 1.5
            commodity_note = "technology supply disruption"
        elif any(k in commodity_lower for k in ["grain", "food", "wheat", "rice"]):
            commodity_severity_mult = 1.3
            commodity_note = "food supply disruption"
        elif any(k in commodity_lower for k in ["gas", "oil", "energy", "fuel"]):
            commodity_severity_mult = 1.4
            commodity_note = "energy supply disruption"
        else:
            commodity_severity_mult = 1.0
            commodity_note = "commodity supply disruption"

        target_vuln_delta  = clamp(0.20 * mag * commodity_severity_mult)
        target_econ_delta  = -clamp(0.15 * mag * commodity_severity_mult)
        target_risk_delta  = clamp(0.12 * mag * commodity_severity_mult)

        actor_revenue_loss = clamp(actor_dep * 0.08 * mag)

        target_affected = AffectedCountry(
            country=target,
            impact_type="direct",
            severity=_severity(target_vuln_delta),
            score_deltas=[
                _make_delta("trade_vulnerability_score", ts.get("trade_vuln"),   target_vuln_delta),
                _make_delta("economic_influence_score",  ts.get("econ_influence"), target_econ_delta),
                _make_delta("global_risk_score",         ts.get("global_risk"),  target_risk_delta),
            ],
            exposure_usd=export_value,
            summary=(
                f"{target} faces {commodity_note} as {actor} bans {commodity} exports; "
                f"~${export_value/1e9:.1f}B in bilateral trade disrupted."
            ),
        )

        actor_affected = AffectedCountry(
            country=actor,
            impact_type="direct",
            severity="low",
            score_deltas=[
                _make_delta("trade_vulnerability_score", as_.get("trade_vuln"),    actor_revenue_loss * 0.3, 0.6),
                _make_delta("economic_influence_score",  as_.get("econ_influence"), clamp(0.05 * mag), 0.6),
            ],
            exposure_usd=export_value * 0.5,
            summary=(
                f"{actor} loses export revenue from {commodity} ban "
                f"but gains strategic leverage; retaliation risk is low."
            ),
        )

        # Other countries that also rely on this commodity from actor
        other_importers = _run(conn, """
            MATCH (other:Country)-[r:HAS_TRADE_DEPENDENCY_ON]->(a:Country {name: $actor})
            WHERE r.year = $year AND r.dependency >= 0.10 AND other.name <> $target
            RETURN other.name AS country,
                   r.dependency AS dependency,
                   other.trade_vulnerability_score AS trade_vuln
            ORDER BY r.dependency DESC
            LIMIT 6
        """, {"actor": actor, "year": year})

        other_affected: list[AffectedCountry] = []
        for row in other_importers:
            country = row.get("country")
            if not country:
                continue
            dep   = float(row.get("dependency") or 0.0)
            spill = clamp(dep * 0.12 * mag)
            other_affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity=_severity(spill),
                score_deltas=[
                    _make_delta("trade_vulnerability_score", row.get("trade_vuln"), spill * 0.5, 0.5),
                ],
                summary=(
                    f"{country} depends on {actor} for {commodity}; "
                    f"ban signals potential supply disruption risk."
                ),
            ))

        return ScenarioResult(
            scenario_type=ScenarioType.EXPORT_BAN,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=[target_affected, actor_affected] + other_affected,
            cascade_effects=[
                CascadeEffect(
                    mechanism=commodity_note.title(),
                    affected=target,
                    severity=_severity(target_vuln_delta),
                    description=(
                        f"Abrupt halt of {commodity} exports forces {target} to "
                        f"find alternative suppliers — a process taking 6-24 months for complex goods."
                    ),
                ),
                CascadeEffect(
                    mechanism="Market price spike",
                    affected=f"All importers of {actor}'s {commodity}",
                    severity="medium",
                    description=(
                        f"Removal of {actor}'s supply from the market drives up "
                        f"global prices for {commodity}, affecting all dependent importers."
                    ),
                ),
            ],
            data_sources=["EXPORTS_TO", "HAS_TRADE_DEPENDENCY_ON", "trade_vulnerability_score"],
            confidence=0.78 if export_value > 0 else 0.50,
        )
    finally:
        conn.close()


# =========================================================
# HELPERS
# =========================================================

def _empty_result(req: ScenarioRequest, reason: str) -> ScenarioResult:
    return ScenarioResult(
        scenario_type=req.scenario_type,
        actor=req.actor,
        target=req.target,
        raw_query=req.raw_query,
        year=req.year,
        headline="Insufficient data to run simulation",
        summary=reason,
        confidence=0.0,
        missing_data=[reason],
    )