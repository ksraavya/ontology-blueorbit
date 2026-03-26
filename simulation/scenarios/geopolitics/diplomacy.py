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
# DIPLOMATIC BREAKDOWN
# =========================================================

def run_diplomatic_breakdown(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: diplomatic relations severed or severely degraded between actor and target.

    Logic:
    - Both countries lose diplomatic_centrality_score (fewer connections)
    - Both face increase in global_risk_score
    - Trade may be disrupted if sanctions follow
    - Third parties with ties to both may face alignment pressure
    - Blocs they belong to face cohesion stress

    Reads:
        diplomatic_centrality_score, global_risk_score, geopolitical_influence_score,
        trade_vulnerability_score, DIPLOMATIC_INTERACTION, ALIGNED_WITH, bloc_id
    """
    actor  = req.actor
    target = req.target
    mag    = req.magnitude
    year   = req.year

    if not actor or not target:
        return _empty_result(req, "diplomatic_breakdown requires both actor and target")

    conn = Neo4jConnection()
    try:
        actor_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.diplomatic_centrality_score AS centrality,
                   c.geopolitical_influence_score AS geo_influence,
                   c.global_risk_score AS global_risk,
                   c.trade_vulnerability_score AS trade_vuln,
                   c.bloc_id AS bloc_id
        """, {"name": actor})
        ar = actor_rows[0] if actor_rows else {}

        target_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.diplomatic_centrality_score AS centrality,
                   c.geopolitical_influence_score AS geo_influence,
                   c.global_risk_score AS global_risk,
                   c.trade_vulnerability_score AS trade_vuln,
                   c.bloc_id AS bloc_id
        """, {"name": target})
        tr = target_rows[0] if target_rows else {}

        # Alignment score between them (how aligned they were)
        alignment_rows = _run(conn, """
            MATCH (a:Country {name: $actor})-[r:DIPLOMATIC_INTERACTION]->(t:Country {name: $target})
            RETURN max(r.alignment_score) AS alignment_score
        """, {"actor": actor, "target": target})
        prior_alignment = float(alignment_rows[0].get("alignment_score") or 0.5) if alignment_rows else 0.5

        # Higher prior alignment = bigger loss from breakdown
        centrality_delta  = -clamp(prior_alignment * 0.15 * mag)
        geo_delta         = -clamp(prior_alignment * 0.12 * mag)
        risk_delta        = clamp(0.10 * mag)

        actor_affected = AffectedCountry(
            country=actor,
            impact_type="direct",
            severity=_severity(abs(centrality_delta) * mag + risk_delta),
            score_deltas=[
                _make_delta("diplomatic_centrality_score",   ar.get("centrality"),    centrality_delta),
                _make_delta("geopolitical_influence_score",  ar.get("geo_influence"), geo_delta),
                _make_delta("global_risk_score",             ar.get("global_risk"),   risk_delta),
            ],
            summary=(
                f"{actor} severs diplomatic ties with {target}; "
                f"loses diplomatic channels and shared multilateral leverage."
            ),
        )

        target_affected = AffectedCountry(
            country=target,
            impact_type="direct",
            severity=_severity(abs(centrality_delta) * mag + risk_delta),
            score_deltas=[
                _make_delta("diplomatic_centrality_score",   tr.get("centrality"),    centrality_delta * 0.9),
                _make_delta("geopolitical_influence_score",  tr.get("geo_influence"), geo_delta * 0.9),
                _make_delta("global_risk_score",             tr.get("global_risk"),   risk_delta),
            ],
            summary=(
                f"{target} faces diplomatic isolation from {actor}; "
                f"joint negotiations and crisis communication channels close."
            ),
        )

        # Countries with high alignment to both face pressure
        bridge_rows = _run(conn, """
            MATCH (c:Country)-[r1:DIPLOMATIC_INTERACTION]->(a:Country {name: $actor})
            MATCH (c)-[r2:DIPLOMATIC_INTERACTION]->(t:Country {name: $target})
            WHERE c.name <> $actor AND c.name <> $target
            RETURN c.name AS country,
                   r1.alignment_score AS align_actor,
                   r2.alignment_score AS align_target,
                   c.diplomatic_centrality_score AS centrality
            ORDER BY (r1.alignment_score + r2.alignment_score) DESC
            LIMIT 5
        """, {"actor": actor, "target": target})

        bridge_affected: list[AffectedCountry] = []
        for row in bridge_rows:
            country = row.get("country")
            if not country:
                continue
            bridge_affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity="low",
                score_deltas=[
                    _make_delta("diplomatic_centrality_score", row.get("centrality"), -clamp(0.03 * mag), 0.5),
                ],
                summary=(
                    f"{country} had strong ties to both {actor} and {target}; "
                    f"must navigate alignment pressure as mediator."
                ),
            ))

        cascades = [
            CascadeEffect(
                mechanism="Diplomatic channel closure",
                affected=f"{actor} and {target}",
                severity=_severity(abs(centrality_delta) * mag),
                description=(
                    "Direct communication channels close; crisis escalation risk rises "
                    "due to misperception and absence of back-channel negotiation."
                ),
            ),
            CascadeEffect(
                mechanism="Alliance pressure",
                affected="Third-party allies and partners",
                severity="medium",
                description=(
                    f"Countries allied with both {actor} and {target} face pressure "
                    f"to choose sides or broker reconciliation."
                ),
            ),
        ]

        return ScenarioResult(
            scenario_type=ScenarioType.DIPLOMATIC_BREAKDOWN,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=[actor_affected, target_affected] + bridge_affected,
            cascade_effects=cascades,
            data_sources=[
                "diplomatic_centrality_score", "DIPLOMATIC_INTERACTION",
                "geopolitical_influence_score",
            ],
            confidence=0.78,
        )
    finally:
        conn.close()


# =========================================================
# DIPLOMATIC NORMALIZATION
# =========================================================

def run_diplomatic_normalization(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: hostile countries restore or improve diplomatic relations.
    Inverse of diplomatic_breakdown — scores improve for both.

    Reads:
        diplomatic_centrality_score, geopolitical_influence_score,
        global_risk_score, IMPOSED_SANCTIONS_ON
    """
    actor  = req.actor
    target = req.target
    mag    = req.magnitude
    year   = req.year

    if not actor or not target:
        return _empty_result(req, "diplomatic_normalization requires both actor and target")

    conn = Neo4jConnection()
    try:
        # Check if there are active sanctions (makes normalization more significant)
        sanction_rows = _run(conn, """
            MATCH (a:Country {name: $actor})-[r:IMPOSED_SANCTIONS_ON]->(t:Country {name: $target})
            RETURN count(r) AS count
            UNION ALL
            MATCH (t:Country {name: $target})-[r:IMPOSED_SANCTIONS_ON]->(a:Country {name: $actor})
            RETURN count(r) AS count
        """, {"actor": actor, "target": target})
        has_sanctions = any((r.get("count") or 0) > 0 for r in sanction_rows)

        actor_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.diplomatic_centrality_score AS centrality,
                   c.geopolitical_influence_score AS geo_influence,
                   c.global_risk_score AS global_risk,
                   c.economic_influence_score AS econ_influence
        """, {"name": actor})
        ar = actor_rows[0] if actor_rows else {}

        target_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.diplomatic_centrality_score AS centrality,
                   c.geopolitical_influence_score AS geo_influence,
                   c.global_risk_score AS global_risk,
                   c.trade_vulnerability_score AS trade_vuln
        """, {"name": target})
        tr = target_rows[0] if target_rows else {}

        sanction_mult = 1.3 if has_sanctions else 1.0

        centrality_delta = clamp(0.10 * mag * sanction_mult)
        risk_delta       = -clamp(0.08 * mag * sanction_mult)
        econ_delta       = clamp(0.06 * mag)

        affected = [
            AffectedCountry(
                country=actor,
                impact_type="direct",
                severity="medium",
                score_deltas=[
                    _make_delta("diplomatic_centrality_score",  ar.get("centrality"),    centrality_delta),
                    _make_delta("geopolitical_influence_score", ar.get("geo_influence"), clamp(0.05 * mag)),
                    _make_delta("global_risk_score",            ar.get("global_risk"),   risk_delta),
                    _make_delta("economic_influence_score",     ar.get("econ_influence"), econ_delta),
                ],
                summary=(
                    f"{actor} normalizes relations with {target}; "
                    f"diplomatic network expands, bilateral economic opportunities emerge."
                    + (" (sanctions context)" if has_sanctions else "")
                ),
            ),
            AffectedCountry(
                country=target,
                impact_type="direct",
                severity="medium",
                score_deltas=[
                    _make_delta("diplomatic_centrality_score",  tr.get("centrality"),    centrality_delta),
                    _make_delta("global_risk_score",            tr.get("global_risk"),   risk_delta),
                    _make_delta("trade_vulnerability_score",    tr.get("trade_vuln"),    -clamp(0.06 * mag)),
                ],
                summary=(
                    f"{target} normalizes relations with {actor}; "
                    f"improved market access and reduced diplomatic isolation."
                ),
            ),
        ]

        return ScenarioResult(
            scenario_type=ScenarioType.DIPLOMATIC_NORMALIZATION,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=affected,
            cascade_effects=[
                CascadeEffect(
                    mechanism="Diplomatic reopening",
                    affected=f"{actor} and {target}",
                    severity="medium",
                    description=(
                        "Embassies reopen, direct communication channels restored; "
                        "trade and investment flows gradually normalize over 6-24 months."
                    ),
                ),
            ],
            data_sources=["diplomatic_centrality_score", "IMPOSED_SANCTIONS_ON"],
            confidence=0.78,
        )
    finally:
        conn.close()


# =========================================================
# BLOC REALIGNMENT
# =========================================================

def run_bloc_realignment(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: a country shifts its geopolitical alignment from one bloc to another.
    Actor = country realigning. Target = new alignment destination.
    extra_params.from_bloc = previous alignment.

    Reads:
        bloc_alignment_score, geopolitical_influence_score, diplomatic_centrality_score,
        strategic_influence_score, ALIGNED_WITH, DIPLOMATIC_INTERACTION
    """
    actor    = req.actor
    new_bloc = req.target
    old_bloc = req.extra_params.get("from_bloc") or req.extra_params.get("realignment_target")
    mag      = req.magnitude
    year     = req.year

    if not actor:
        return _empty_result(req, "bloc_realignment requires an actor country")

    conn = Neo4jConnection()
    try:
        actor_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.bloc_alignment_score AS bloc_align,
                   c.geopolitical_influence_score AS geo_influence,
                   c.diplomatic_centrality_score AS centrality,
                   c.overall_vulnerability_score AS vulnerability,
                   c.strategic_influence_score AS strat_influence,
                   c.bloc_id AS current_bloc
        """, {"name": actor})
        ar = actor_rows[0] if actor_rows else {}

        # Realignment: loses old bloc network, gains new bloc benefits
        # Net effect depends on relative strength of blocs
        old_bloc_loss_delta   = -clamp(0.12 * mag)
        new_bloc_gain_delta   = clamp(0.10 * mag)
        net_centrality_change = old_bloc_loss_delta + new_bloc_gain_delta

        vuln_delta = clamp(0.08 * mag)  # transition period vulnerability

        actor_affected = AffectedCountry(
            country=actor,
            impact_type="direct",
            severity=_severity(mag * 0.5),
            score_deltas=[
                _make_delta("bloc_alignment_score",        ar.get("bloc_align"),    new_bloc_gain_delta),
                _make_delta("diplomatic_centrality_score", ar.get("centrality"),    net_centrality_change),
                _make_delta("overall_vulnerability_score", ar.get("vulnerability"), vuln_delta),
                _make_delta("geopolitical_influence_score", ar.get("geo_influence"), net_centrality_change * 0.8),
            ],
            summary=(
                f"{actor} shifts alignment"
                + (f" from {old_bloc}" if old_bloc else "")
                + (f" toward {new_bloc}" if new_bloc else "")
                + f"; loses established network ties but gains new strategic relationships."
            ),
        )

        # Old bloc members lose a partner
        if old_bloc:
            old_bloc_rows = _run(conn, """
                MATCH (c:Country)-[:MEMBER_OF]->(a:Alliance {name: $bloc})
                WHERE c.name <> $actor
                RETURN c.name AS country,
                       c.geopolitical_influence_score AS geo_influence,
                       c.bloc_alignment_score AS bloc_align
                LIMIT 8
            """, {"bloc": old_bloc, "actor": actor})

            old_affected: list[AffectedCountry] = []
            for row in (old_bloc_rows or [])[:4]:
                country = row.get("country")
                if not country:
                    continue
                old_affected.append(AffectedCountry(
                    country=country,
                    impact_type="cascade",
                    severity="low",
                    score_deltas=[
                        _make_delta("bloc_alignment_score", row.get("bloc_align"), -clamp(0.02 * mag), 0.5),
                    ],
                    summary=f"{country} loses {actor} as a {old_bloc} bloc partner.",
                ))
        else:
            old_affected = []

        cascades = [
            CascadeEffect(
                mechanism="Strategic reorientation",
                affected=actor,
                severity=_severity(mag * 0.5),
                description=(
                    f"{actor}'s shift"
                    + (f" toward {new_bloc}" if new_bloc else "")
                    + " reshapes diplomatic, military, and economic partnerships "
                    + "over a 2-5 year transition period."
                ),
            ),
            CascadeEffect(
                mechanism="Regional balance shift",
                affected=f"Regional neighbors and rival blocs",
                severity="medium",
                description=(
                    f"Realignment of {actor} alters the balance of influence in its region; "
                    f"neighboring states recalibrate their own alignment strategies."
                ),
            ),
        ]

        return ScenarioResult(
            scenario_type=ScenarioType.BLOC_REALIGNMENT,
            actor=actor,
            target=new_bloc,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=[actor_affected] + old_affected,
            cascade_effects=cascades,
            data_sources=["bloc_alignment_score", "diplomatic_centrality_score", "MEMBER_OF"],
            confidence=0.72,
        )
    finally:
        conn.close()


# =========================================================
# INTERNATIONAL ISOLATION
# =========================================================

def run_international_isolation(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: a country is expelled from or isolated by international bodies.

    Logic:
    - Target loses diplomatic_centrality_score significantly
    - Target's sanctions_vulnerability rises (more sanctions likely)
    - Target's geopolitical_influence collapses
    - Trade partners face pressure to reduce engagement
    - If UN P5 member is isolated: global governance implications

    Reads:
        diplomatic_centrality_score, geopolitical_influence_score,
        sanctions_vulnerability_score, un_p5, strategic_influence_score
    """
    target = req.target or req.actor
    actor  = req.actor
    mag    = req.magnitude
    year   = req.year

    if not target:
        return _empty_result(req, "international_isolation requires a target country")

    conn = Neo4jConnection()
    try:
        target_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.diplomatic_centrality_score AS centrality,
                   c.geopolitical_influence_score AS geo_influence,
                   c.strategic_influence_score AS strat_influence,
                   c.global_risk_score AS global_risk,
                   c.trade_vulnerability_score AS trade_vuln,
                   c.un_p5 AS un_p5,
                   c.overall_vulnerability_score AS vulnerability
        """, {"name": target})
        tr = target_rows[0] if target_rows else {}

        is_p5 = bool(tr.get("un_p5"))

        centrality_delta  = -clamp(0.30 * mag)
        geo_delta         = -clamp(0.25 * mag)
        strat_delta       = -clamp(0.20 * mag)
        risk_delta        = clamp(0.25 * mag)
        vuln_delta        = clamp(0.20 * mag)

        target_affected = AffectedCountry(
            country=target,
            impact_type="direct",
            severity=_severity(abs(centrality_delta) * mag * 1.2),
            score_deltas=[
                _make_delta("diplomatic_centrality_score",   tr.get("centrality"),    centrality_delta),
                _make_delta("geopolitical_influence_score",  tr.get("geo_influence"), geo_delta),
                _make_delta("strategic_influence_score",     tr.get("strat_influence"), strat_delta),
                _make_delta("global_risk_score",             tr.get("global_risk"),   risk_delta),
                _make_delta("overall_vulnerability_score",   tr.get("vulnerability"), vuln_delta),
            ],
            summary=(
                f"{target} faces international isolation; expelled from key bodies, "
                f"diplomatic and economic access severely curtailed."
                + (" (UN P5 status complicates isolation)" if is_p5 else "")
            ),
        )

        # Trade partners face pressure to reduce engagement
        trade_rows = _run(conn, """
            MATCH (other:Country)-[r:HAS_TRADE_DEPENDENCY_ON]->(t:Country {name: $target})
            WHERE r.year = $year AND r.dependency >= 0.08
            RETURN other.name AS country,
                   r.dependency AS dependency,
                   other.trade_vulnerability_score AS trade_vuln
            ORDER BY r.dependency DESC
            LIMIT 6
        """, {"target": target, "year": year})

        partner_affected: list[AffectedCountry] = []
        for row in trade_rows:
            country = row.get("country")
            if not country:
                continue
            dep   = float(row.get("dependency") or 0.0)
            spill = clamp(dep * 0.15 * mag)
            partner_affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity=_severity(spill),
                score_deltas=[
                    _make_delta("trade_vulnerability_score", row.get("trade_vuln"), spill, 0.6),
                ],
                summary=(
                    f"{country} has {dep*100:.0f}% trade dependency on {target}; "
                    f"faces pressure to reduce engagement under international norms."
                ),
            ))

        cascades = [
            CascadeEffect(
                mechanism="Diplomatic exclusion",
                affected=target,
                severity="critical",
                description=(
                    f"{target} loses access to multilateral forums, international financial "
                    f"institutions, and diplomatic back-channels."
                ),
            ),
            CascadeEffect(
                mechanism="Secondary sanctions pressure",
                affected="Trade and financial partners",
                severity="high",
                description=(
                    f"Countries maintaining economic ties with {target} face pressure "
                    f"from the isolating coalition — trade partner compliance likely."
                ),
            ),
        ]

        if is_p5:
            cascades.append(CascadeEffect(
                mechanism="Global governance paralysis",
                affected="UN Security Council and multilateral bodies",
                severity="critical",
                description=(
                    f"Isolation of a P5 member creates institutional deadlock; "
                    f"UN Security Council decision-making severely impaired."
                ),
            ))

        return ScenarioResult(
            scenario_type=ScenarioType.INTERNATIONAL_ISOLATION,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=[target_affected] + partner_affected,
            cascade_effects=cascades,
            data_sources=["diplomatic_centrality_score", "geopolitical_influence_score", "un_p5"],
            confidence=0.75,
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