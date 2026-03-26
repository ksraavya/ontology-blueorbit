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
    return ScoreDelta(
        score_name=score_name,
        current=round(c, 4),
        delta=round(delta, 4),
        projected=round(clamp(c + delta), 4),
        direction="increase" if delta > 0.001 else "decrease" if delta < -0.001 else "unchanged",
        confidence=confidence,
    )


def _severity(score: float) -> str:
    if score >= 0.6: return "critical"
    if score >= 0.4: return "high"
    if score >= 0.2: return "medium"
    return "low"


# =========================================================
# STATE FRAGILITY — "which countries are most at risk?"
# =========================================================

def run_state_fragility(req: ScenarioRequest) -> ScenarioResult:
    """
    Not a counterfactual — a current-state assessment.
    Reads all composite and component scores to identify
    the most vulnerable, high-risk, low-influence countries.

    Returns: ranked list of fragile states with score breakdowns.
    """
    year = req.year

    conn = Neo4jConnection()
    try:
        rows = _run(conn, """
            MATCH (c:Country)
            WHERE c.overall_vulnerability_score IS NOT NULL
              AND c.global_risk_score IS NOT NULL
            WITH c,
                 (
                     coalesce(c.overall_vulnerability_score, 0) * 0.4 +
                     coalesce(c.global_risk_score, 0) * 0.4 +
                     (1.0 - coalesce(c.strategic_influence_score, 0.5)) * 0.2
                 ) AS fragility_score
            WHERE fragility_score > 0.3
            RETURN
                c.name                                              AS country,
                fragility_score,
                c.overall_vulnerability_score                       AS vulnerability,
                c.global_risk_score                                 AS global_risk,
                c.strategic_influence_score                         AS influence,
                c.primary_vulnerability                             AS primary_vuln,
                c.conflict_risk_score                               AS conflict_risk,
                c.climate_vulnerability_score                       AS climate_vuln,
                c.trade_vulnerability_score                         AS trade_vuln,
                c.energy_vulnerability_score                        AS energy_vuln,
                c.political_stability_score                         AS political_stability,
                c.live_risk_score                                   AS live_risk
            ORDER BY fragility_score DESC
            LIMIT 25
        """)

        affected: list[AffectedCountry] = []
        for row in rows:
            country = row.get("country")
            if not country:
                continue
            frag = float(row.get("fragility_score") or 0.0)
            affected.append(AffectedCountry(
                country=country,
                impact_type="direct",
                severity=_severity(frag),
                score_deltas=[],   # assessment, not projection
                summary=(
                    f"Fragility score: {frag:.3f} | "
                    f"primary vulnerability: {row.get('primary_vuln', 'N/A')} | "
                    f"conflict risk: {(row.get('conflict_risk') or 0):.3f} | "
                    f"climate: {(row.get('climate_vuln') or 0):.3f}"
                ),
            ))

        return ScenarioResult(
            scenario_type=ScenarioType.STATE_FRAGILITY,
            actor=None,
            target=None,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=affected,
            cascade_effects=[],
            data_sources=[
                "overall_vulnerability_score", "global_risk_score",
                "strategic_influence_score", "conflict_risk_score",
                "climate_vulnerability_score", "live_risk_score",
            ],
            confidence=0.90 if rows else 0.3,
        )
    finally:
        conn.close()


# =========================================================
# POWER VACUUM — "what if the US reduces global presence?"
# =========================================================

def run_power_vacuum(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: a major power (actor) significantly reduces global military/diplomatic presence.

    Logic:
    - Actor's IS_INFLUENTIAL_TO partners lose influence coverage
    - Countries heavily dependent on actor for defense face vulnerability rise
    - Second-tier powers (high strategic_influence) may gain influence
    - Global average conflict_risk rises

    Reads:
        IS_INFLUENTIAL_TO, MEMBER_OF, strategic_influence_score,
        military_strength_score, overall_vulnerability_score
    """
    actor = req.actor or "United States"
    mag   = req.magnitude
    year  = req.year

    conn = Neo4jConnection()
    try:
        # Actor's influence network
        influence_rows = _run(conn, """
            MATCH (a:Country {name: $actor})-[r:IS_INFLUENTIAL_TO]->(b:Country)
            WHERE r.year = $year
            RETURN
                b.name                              AS country,
                r.normalized_weight                 AS influence_weight,
                b.strategic_influence_score         AS strat_influence,
                b.overall_vulnerability_score       AS vulnerability,
                b.military_strength_score           AS military
            ORDER BY r.normalized_weight DESC
            LIMIT 20
        """, {"actor": actor, "year": year})

        # Actor's alliance partners
        alliance_rows = _run(conn, """
            MATCH (a:Country {name: $actor})-[:MEMBER_OF]->(al:Alliance)<-[:MEMBER_OF]-(b:Country)
            WHERE b.name <> $actor
            RETURN DISTINCT b.name AS country
        """, {"actor": actor})
        alliance_members = {r["country"] for r in alliance_rows}

        # Actor's own scores
        actor_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN
                c.strategic_influence_score     AS strat_influence,
                c.military_strength_score       AS military,
                c.economic_influence_score      AS econ_influence
        """, {"name": actor})
        ar = actor_rows[0] if actor_rows else {}

        # Second-tier powers who may fill the vacuum
        rising_rows = _run(conn, """
            MATCH (c:Country)
            WHERE c.strategic_influence_score >= 0.4
              AND c.name <> $actor
            RETURN
                c.name                          AS country,
                c.strategic_influence_score     AS strat_influence,
                c.military_strength_score       AS military
            ORDER BY c.strategic_influence_score DESC
            LIMIT 5
        """, {"actor": actor})

        affected: list[AffectedCountry] = []

        # Actor loses influence but gains reduced exposure
        actor_influence_delta = -clamp(0.15 * mag)
        actor_military_delta  = -clamp(0.10 * mag)
        affected.append(AffectedCountry(
            country=actor,
            impact_type="direct",
            severity="medium",
            score_deltas=[
                _make_delta("strategic_influence_score", ar.get("strat_influence"), actor_influence_delta),
                _make_delta("military_strength_score",   ar.get("military"),        actor_military_delta),
            ],
            summary=f"{actor} reduces global engagement; strategic influence contracts but domestic exposure falls.",
        ))

        # Influenced countries lose coverage
        for row in influence_rows:
            country = row.get("country")
            if not country:
                continue
            weight = float(row.get("influence_weight") or 0.0)
            vuln_delta = clamp(weight * 0.25 * mag)
            in_alliance = country in alliance_members
            affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity=_severity(vuln_delta),
                score_deltas=[
                    _make_delta("overall_vulnerability_score", row.get("vulnerability"), vuln_delta, 0.65),
                    _make_delta("military_strength_score",     row.get("military"),      -weight * 0.1 * mag, 0.55),
                ],
                summary=(
                    f"{country} loses {actor}'s influence coverage (weight={weight:.2f})"
                    + ("; alliance security guarantee weakened" if in_alliance else "")
                    + f"; vulnerability rises +{vuln_delta:.3f}."
                ),
            ))

        # Rising powers gain
        for row in rising_rows:
            country = row.get("country")
            if not country:
                continue
            gain = clamp(0.08 * mag)
            affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity="low",
                score_deltas=[
                    _make_delta("strategic_influence_score", row.get("strat_influence"), gain, 0.55),
                ],
                summary=f"{country} positioned to expand influence into {actor}'s receding sphere.",
            ))

        cascades = [
            CascadeEffect(
                mechanism="Security vacuum",
                affected=f"{len(influence_rows)} countries in {actor}'s influence network",
                severity=_severity(mag * 0.5),
                description=(
                    f"Countries relying on {actor} for security guarantees must increase "
                    f"own defense spending or seek alternative patrons."
                ),
            ),
            CascadeEffect(
                mechanism="Multipolarity acceleration",
                affected="Global order",
                severity="medium",
                description=(
                    f"Reduced {actor} engagement accelerates shift to multipolar world; "
                    f"regional powers gain relative influence."
                ),
            ),
        ]

        return ScenarioResult(
            scenario_type=ScenarioType.POWER_VACUUM,
            actor=actor,
            target=None,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=affected,
            cascade_effects=cascades,
            data_sources=["IS_INFLUENTIAL_TO", "MEMBER_OF", "strategic_influence_score", "overall_vulnerability_score"],
            confidence=0.72,
        )
    finally:
        conn.close()


# =========================================================
# HEGEMONY SHIFT
# =========================================================

def run_hegemony_shift(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: long-term power balance shift — e.g. China surpasses US in global influence.
    Actor = rising power, Target = incumbent power.
    Projects what scores look like if actor's influence exceeds target's.

    This is more of a comparative analysis than a time-bound projection.
    """
    actor  = req.actor   # rising power
    target = req.target  # incumbent
    year   = req.year

    if not actor or not target:
        return _empty_result(req, "hegemony_shift requires both actor (rising) and target (incumbent)")

    conn = Neo4jConnection()
    try:
        both_rows = _run(conn, """
            MATCH (c:Country)
            WHERE c.name IN [$actor, $target]
            RETURN
                c.name                          AS country,
                c.strategic_influence_score     AS strat_influence,
                c.economic_influence_score      AS econ_influence,
                c.military_strength_score       AS military,
                c.geopolitical_influence_score  AS geo_influence,
                c.global_risk_score             AS global_risk
        """, {"actor": actor, "target": target})

        scores_by_country = {r["country"]: r for r in both_rows}
        ar = scores_by_country.get(actor, {})
        tr = scores_by_country.get(target, {})

        actor_si  = float(ar.get("strat_influence") or 0.0)
        target_si = float(tr.get("strat_influence") or 0.0)
        gap       = target_si - actor_si   # how far actor needs to close

        affected = [
            AffectedCountry(
                country=actor,
                impact_type="direct",
                severity="high",
                score_deltas=[
                    _make_delta("strategic_influence_score", ar.get("strat_influence"), clamp(gap * 0.5), 0.5),
                    _make_delta("economic_influence_score",  ar.get("econ_influence"),  clamp(0.10), 0.5),
                    _make_delta("geopolitical_influence_score", ar.get("geo_influence"), clamp(0.10), 0.5),
                ],
                summary=(
                    f"{actor} currently at strategic_influence={actor_si:.3f}; "
                    f"projected +{gap*0.5:.3f} in hegemony-shift scenario "
                    f"(gap to {target}: {gap:.3f})."
                ),
            ),
            AffectedCountry(
                country=target,
                impact_type="direct",
                severity="high",
                score_deltas=[
                    _make_delta("strategic_influence_score", tr.get("strat_influence"), -clamp(gap * 0.3), 0.5),
                    _make_delta("economic_influence_score",  tr.get("econ_influence"),  -clamp(0.05), 0.5),
                ],
                summary=(
                    f"{target} currently at strategic_influence={target_si:.3f}; "
                    f"projected -{gap*0.3:.3f} as {actor} closes gap."
                ),
            ),
        ]

        cascades = [
            CascadeEffect(
                mechanism="Alliance reconfiguration",
                affected="Global alliance network",
                severity="critical",
                description=(
                    f"Smaller states recalibrate alignment as {actor}'s influence "
                    f"approaches {target}'s; hedging strategies proliferate."
                ),
            ),
            CascadeEffect(
                mechanism="Trade and technology decoupling",
                affected="Global supply chains",
                severity="high",
                description=(
                    f"Competing economic spheres harden as {actor} and {target} "
                    f"offer parallel trade, finance, and technology systems."
                ),
            ),
        ]

        return ScenarioResult(
            scenario_type=ScenarioType.HEGEMONY_SHIFT,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=affected,
            cascade_effects=cascades,
            data_sources=["strategic_influence_score", "economic_influence_score", "military_strength_score"],
            confidence=0.55,   # long-run projection, lower confidence
        )
    finally:
        conn.close()


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