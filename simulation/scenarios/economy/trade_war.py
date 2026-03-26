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
# TRADE WAR
# =========================================================

def run_trade_war(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: escalating tariff war between actor and target.

    Logic:
    - Both sides face trade_vulnerability increases proportional to bilateral volume
    - Both lose economic_influence as trade flows contract
    - Third-party countries with high trade dependency on either party face cascade
    - Countries with trade agreements with both may act as intermediaries (slight gain)
    - Supply chain disruption cascades through dependent economies

    Reads:
        EXPORTS_TO, HAS_TRADE_VOLUME_WITH, HAS_TRADE_DEPENDENCY_ON,
        trade_vulnerability_score, economic_influence_score, economic_power_score
    """
    actor  = req.actor
    target = req.target
    mag    = req.magnitude
    year   = req.year

    if not actor or not target:
        return _empty_result(req, "trade_war requires both actor and target")

    conn = Neo4jConnection()
    try:
        # Bilateral trade volume
        vol_rows = _run(conn, """
            MATCH (a:Country {name: $actor})-[r:HAS_TRADE_VOLUME_WITH]-(t:Country {name: $target})
            WHERE r.year = $year
            RETURN r.value AS bilateral_volume, r.normalized_weight AS weight
        """, {"actor": actor, "target": target, "year": year})
        bilateral_volume = float(vol_rows[0].get("bilateral_volume") or 0.0) if vol_rows else 0.0
        bilateral_weight = float(vol_rows[0].get("weight") or 0.0) if vol_rows else 0.0

        # Actor's total trade volume
        actor_vol = _run(conn, """
            MATCH (a:Country {name: $actor})-[r:HAS_TRADE_VOLUME_WITH]-(:Country)
            WHERE r.year = $year
            RETURN sum(r.value) AS total
        """, {"actor": actor, "year": year})
        actor_total = float((actor_vol[0].get("total") or 1.0)) if actor_vol else 1.0

        # Target's total trade volume
        target_vol = _run(conn, """
            MATCH (t:Country {name: $target})-[r:HAS_TRADE_VOLUME_WITH]-(:Country)
            WHERE r.year = $year
            RETURN sum(r.value) AS total
        """, {"target": target, "year": year})
        target_total = float((target_vol[0].get("total") or 1.0)) if target_vol else 1.0

        actor_dep_on_target  = clamp(bilateral_volume / actor_total)  if actor_total > 0 else 0.0
        target_dep_on_actor  = clamp(bilateral_volume / target_total) if target_total > 0 else 0.0

        # Actor scores
        actor_scores = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.trade_vulnerability_score AS trade_vuln,
                   c.economic_influence_score AS econ_influence,
                   c.economic_power_score AS econ_power,
                   c.global_risk_score AS global_risk
        """, {"name": actor})
        as_ = actor_scores[0] if actor_scores else {}

        # Target scores
        target_scores = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.trade_vulnerability_score AS trade_vuln,
                   c.economic_influence_score AS econ_influence,
                   c.economic_power_score AS econ_power,
                   c.global_risk_score AS global_risk
        """, {"name": target})
        ts = target_scores[0] if target_scores else {}

        # Compute deltas — both sides hurt, target typically more if smaller
        actor_trade_delta   = clamp(actor_dep_on_target * 0.25 * mag)
        actor_econ_delta    = -clamp(actor_dep_on_target * 0.15 * mag)
        actor_risk_delta    = clamp(actor_dep_on_target * 0.10 * mag)

        target_trade_delta  = clamp(target_dep_on_actor * 0.30 * mag)
        target_econ_delta   = -clamp(target_dep_on_actor * 0.20 * mag)
        target_risk_delta   = clamp(target_dep_on_actor * 0.15 * mag)

        actor_affected = AffectedCountry(
            country=actor,
            impact_type="direct",
            severity=_severity(actor_dep_on_target * mag),
            score_deltas=[
                _make_delta("trade_vulnerability_score", as_.get("trade_vuln"),   actor_trade_delta),
                _make_delta("economic_influence_score",  as_.get("econ_influence"), actor_econ_delta),
                _make_delta("global_risk_score",         as_.get("global_risk"),  actor_risk_delta),
            ],
            exposure_usd=bilateral_volume * actor_dep_on_target,
            summary=(
                f"{actor} initiates trade war; {actor_dep_on_target*100:.1f}% of its trade "
                f"with {target} at risk (~${bilateral_volume/1e9:.1f}B bilateral volume)."
            ),
        )

        target_affected = AffectedCountry(
            country=target,
            impact_type="direct",
            severity=_severity(target_dep_on_actor * mag),
            score_deltas=[
                _make_delta("trade_vulnerability_score", ts.get("trade_vuln"),   target_trade_delta),
                _make_delta("economic_influence_score",  ts.get("econ_influence"), target_econ_delta),
                _make_delta("global_risk_score",         ts.get("global_risk"),  target_risk_delta),
            ],
            exposure_usd=bilateral_volume * target_dep_on_actor,
            summary=(
                f"{target} faces retaliatory tariffs; {target_dep_on_actor*100:.1f}% of its trade "
                f"at risk. Domestic industries exposed to reduced export demand."
            ),
        )

        # Third-country cascade — those dependent on either actor or target
        cascade_rows = _run(conn, """
            MATCH (other:Country)-[r:HAS_TRADE_DEPENDENCY_ON]->(c:Country)
            WHERE c.name IN [$actor, $target]
              AND r.year = $year AND r.dependency >= 0.08
              AND other.name <> $actor AND other.name <> $target
            RETURN other.name AS country,
                   c.name AS exposed_to,
                   r.dependency AS dependency,
                   other.trade_vulnerability_score AS trade_vuln
            ORDER BY r.dependency DESC
            LIMIT 10
        """, {"actor": actor, "target": target, "year": year})

        cascade_countries: list[AffectedCountry] = []
        seen: set[str] = set()
        for row in cascade_rows:
            country = row.get("country")
            if not country or country in seen:
                continue
            seen.add(country)
            dep    = float(row.get("dependency") or 0.0)
            spill  = clamp(dep * 0.15 * mag)
            cascade_countries.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity=_severity(spill),
                score_deltas=[
                    _make_delta("trade_vulnerability_score", row.get("trade_vuln"), spill, 0.6),
                ],
                summary=(
                    f"{country} has {dep*100:.0f}% trade dependency on {row.get('exposed_to')}; "
                    f"trade war disrupts supply chains and export routes."
                ),
            ))

        cascades: list[CascadeEffect] = [
            CascadeEffect(
                mechanism="Tariff escalation",
                affected=f"{actor} and {target}",
                severity=_severity(max(actor_dep_on_target, target_dep_on_actor) * mag),
                description=(
                    f"Reciprocal tariffs on ${bilateral_volume/1e9:.1f}B trade reduce "
                    f"competitiveness and raise consumer prices in both economies."
                ),
            ),
            CascadeEffect(
                mechanism="Supply chain disruption",
                affected=f"{len(cascade_countries)} third-party economies",
                severity="medium",
                description=(
                    "Global value chains embedded in bilateral trade face rerouting costs; "
                    "intermediate goods most affected."
                ),
            ),
        ]
        if mag >= 1.5:
            cascades.append(CascadeEffect(
                mechanism="Investment freeze",
                affected=f"{actor} and {target}",
                severity="high",
                description=(
                    "Prolonged trade war uncertainty deters FDI inflows and delays "
                    "capital investment decisions in both countries."
                ),
            ))

        all_affected = [actor_affected, target_affected] + cascade_countries

        return ScenarioResult(
            scenario_type=ScenarioType.TRADE_WAR,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=all_affected,
            cascade_effects=cascades,
            data_sources=[
                "HAS_TRADE_VOLUME_WITH", "HAS_TRADE_DEPENDENCY_ON",
                "trade_vulnerability_score", "economic_influence_score",
            ],
            confidence=0.82 if bilateral_volume > 0 else 0.50,
        )
    finally:
        conn.close()


# =========================================================
# TRADE AGREEMENT
# =========================================================

def run_trade_agreement(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: actor joins or forms a new trade agreement with target/bloc.

    Logic:
    - Actor gains trade_integration_score and partner_diversification improvement
    - Actor's trade_vulnerability decreases (less reliance on any single partner)
    - Existing members gain a new market (small economic_influence bump)
    - Countries outside the agreement may face mild trade diversion

    Reads:
        HAS_TRADE_AGREEMENT_WITH, HAS_TRADE_VOLUME_WITH,
        trade_vulnerability_score, economic_influence_score, trade_integration_score
    """
    actor    = req.actor
    agreement= req.target    # target = agreement/bloc name
    mag      = req.magnitude
    year     = req.year
    agreement_name = req.extra_params.get("agreement_name", agreement or "trade agreement")

    if not actor:
        return _empty_result(req, "trade_agreement requires an actor country")

    conn = Neo4jConnection()
    try:
        actor_scores = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.trade_vulnerability_score AS trade_vuln,
                   c.economic_influence_score AS econ_influence,
                   c.partner_diversification_score AS diversification,
                   c.trade_integration_score AS trade_integration
        """, {"name": actor})
        as_ = actor_scores[0] if actor_scores else {}

        # Existing partners in the agreement
        existing_rows = _run(conn, """
            MATCH (c:Country)-[r:HAS_TRADE_AGREEMENT_WITH]->(other:Country)
            WHERE r.agreement_name = $agreement_name AND c.name <> $actor
            RETURN DISTINCT c.name AS country,
                   c.economic_influence_score AS econ_influence
            LIMIT 10
        """, {"agreement_name": agreement_name, "actor": actor})

        # Actor gains: reduced vulnerability, better integration
        actor_vuln_delta  = -clamp(0.12 * mag)
        actor_integ_delta = clamp(0.15 * mag)
        actor_econ_delta  = clamp(0.08 * mag)

        actor_affected = AffectedCountry(
            country=actor,
            impact_type="direct",
            severity="medium",
            score_deltas=[
                _make_delta("trade_vulnerability_score",   as_.get("trade_vuln"),        actor_vuln_delta),
                _make_delta("trade_integration_score",     as_.get("trade_integration"),  actor_integ_delta),
                _make_delta("economic_influence_score",    as_.get("econ_influence"),     actor_econ_delta),
            ],
            summary=(
                f"{actor} joins {agreement_name}; gains market access to "
                f"{len(existing_rows)} existing member economies, reducing export concentration risk."
            ),
        )

        # Existing members gain access to actor's market
        member_affected: list[AffectedCountry] = []
        for row in existing_rows[:5]:
            country = row.get("country")
            if not country:
                continue
            member_econ_delta = clamp(0.03 * mag)
            member_affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity="low",
                score_deltas=[
                    _make_delta("economic_influence_score", row.get("econ_influence"), member_econ_delta, 0.6),
                ],
                summary=f"{country} gains access to {actor}'s market under {agreement_name}.",
            ))

        cascades: list[CascadeEffect] = [
            CascadeEffect(
                mechanism="Market integration",
                affected=f"{actor} and {agreement_name} members",
                severity="medium",
                description=(
                    f"Tariff reductions and regulatory harmonization open new "
                    f"export channels for {actor} and existing members."
                ),
            ),
        ]
        if len(existing_rows) >= 5:
            cascades.append(CascadeEffect(
                mechanism="Trade diversion",
                affected="Non-member competitors",
                severity="low",
                description=(
                    f"Countries outside {agreement_name} face relative disadvantage "
                    f"as preferential access shifts trade flows."
                ),
            ))

        return ScenarioResult(
            scenario_type=ScenarioType.TRADE_AGREEMENT,
            actor=actor,
            target=agreement,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=[actor_affected] + member_affected,
            cascade_effects=cascades,
            data_sources=["HAS_TRADE_AGREEMENT_WITH", "trade_vulnerability_score", "trade_integration_score"],
            confidence=0.78,
        )
    finally:
        conn.close()


# =========================================================
# TRADE AGREEMENT COLLAPSE
# =========================================================

def run_trade_agreement_collapse(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: an existing trade agreement between actor and target collapses.
    Inverse of trade_agreement — reintroduction of tariffs, loss of market access.

    Reads:
        HAS_TRADE_AGREEMENT_WITH, HAS_TRADE_VOLUME_WITH,
        trade_vulnerability_score, economic_influence_score
    """
    actor    = req.actor
    target   = req.target
    mag      = req.magnitude
    year     = req.year
    agreement_name = req.extra_params.get("agreement_name", "trade agreement")

    if not actor or not target:
        return _empty_result(req, "trade_agreement_collapse requires both actor and target")

    conn = Neo4jConnection()
    try:
        # Check agreement exists
        agreement_rows = _run(conn, """
            MATCH (a:Country {name: $actor})-[r:HAS_TRADE_AGREEMENT_WITH]->(t:Country {name: $target})
            RETURN count(r) AS count, r.agreement_name AS agreement_name
        """, {"actor": actor, "target": target})
        has_agreement = (agreement_rows[0].get("count") or 0) > 0 if agreement_rows else False

        # Bilateral volume at risk
        vol_rows = _run(conn, """
            MATCH (a:Country {name: $actor})-[r:HAS_TRADE_VOLUME_WITH]-(t:Country {name: $target})
            WHERE r.year = $year
            RETURN r.value AS volume
        """, {"actor": actor, "target": target, "year": year})
        bilateral_volume = float(vol_rows[0].get("volume") or 0.0) if vol_rows else 0.0

        actor_scores = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.trade_vulnerability_score AS trade_vuln,
                   c.economic_influence_score AS econ_influence
        """, {"name": actor})
        as_ = actor_scores[0] if actor_scores else {}

        target_scores = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.trade_vulnerability_score AS trade_vuln,
                   c.economic_influence_score AS econ_influence,
                   c.global_risk_score AS global_risk
        """, {"name": target})
        ts = target_scores[0] if target_scores else {}

        # Collapse worsens vulnerability, reduces economic influence
        trade_delta = clamp(0.18 * mag)
        econ_delta  = -clamp(0.12 * mag)

        affected = [
            AffectedCountry(
                country=actor,
                impact_type="direct",
                severity=_severity(trade_delta),
                score_deltas=[
                    _make_delta("trade_vulnerability_score", as_.get("trade_vuln"),    trade_delta),
                    _make_delta("economic_influence_score",  as_.get("econ_influence"), econ_delta),
                ],
                exposure_usd=bilateral_volume,
                summary=(
                    f"{actor} loses preferential access under {agreement_name}; "
                    f"~${bilateral_volume/1e9:.1f}B in trade faces new tariff barriers."
                    + (" (agreement confirmed in graph)" if has_agreement else " (no active agreement found in graph)")
                ),
            ),
            AffectedCountry(
                country=target,
                impact_type="direct",
                severity=_severity(trade_delta * 0.8),
                score_deltas=[
                    _make_delta("trade_vulnerability_score", ts.get("trade_vuln"),    clamp(trade_delta * 0.8)),
                    _make_delta("economic_influence_score",  ts.get("econ_influence"), econ_delta),
                    _make_delta("global_risk_score",         ts.get("global_risk"),   clamp(0.05 * mag)),
                ],
                exposure_usd=bilateral_volume,
                summary=(
                    f"{target} faces disruption of established trade flows as "
                    f"preferential framework collapses."
                ),
            ),
        ]

        return ScenarioResult(
            scenario_type=ScenarioType.TRADE_AGREEMENT_COLLAPSE,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=affected,
            cascade_effects=[
                CascadeEffect(
                    mechanism="Tariff reintroduction",
                    affected=f"{actor} and {target}",
                    severity=_severity(trade_delta),
                    description=(
                        f"WTO MFN tariffs replace preferential rates; "
                        f"industries reliant on zero-tariff trade face cost increases."
                    ),
                ),
                CascadeEffect(
                    mechanism="Supply chain restructuring",
                    affected="Integrated industries",
                    severity="medium",
                    description=(
                        "Companies embedded in cross-border supply chains must "
                        "restructure sourcing and production to manage new tariff costs."
                    ),
                ),
            ],
            data_sources=["HAS_TRADE_AGREEMENT_WITH", "HAS_TRADE_VOLUME_WITH", "trade_vulnerability_score"],
            confidence=0.78 if has_agreement else 0.55,
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