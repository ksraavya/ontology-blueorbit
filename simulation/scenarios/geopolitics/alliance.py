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
# ALLIANCE EXIT
# =========================================================

def run_alliance_exit(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: actor leaves target alliance (e.g. Turkey leaves NATO).

    Logic:
    - Actor loses bloc_alignment_score benefit from that alliance
    - Remaining members lose a contributor (defense spending, trade)
    - Actor's diplomatic_centrality falls (loses alliance network)
    - Actor's military_strength_score falls (loses collective defense)
    - If actor realigns (extra_params.realignment_target), their
      geopolitical_influence may shift rather than purely fall

    Reads:
        MEMBER_OF, HAS_MILITARY_ALLIANCE_WITH, SPENDS_ON_DEFENSE
        bloc_alignment_score, military_strength_score, diplomatic_centrality_score
    """
    actor    = req.actor
    alliance = req.target   # target = alliance name
    mag      = req.magnitude
    year     = req.year
    realign  = req.extra_params.get("realignment_target")

    if not actor or not alliance:
        return _empty_result(req, "alliance_exit requires actor (country) and target (alliance name)")

    conn = Neo4jConnection()
    try:
        # Verify membership
        member_rows = _run(conn, """
            MATCH (c:Country {name: $actor})-[:MEMBER_OF]->(a:Alliance {name: $alliance})
            RETURN count(*) AS is_member
        """, {"actor": actor, "alliance": alliance})
        is_member = (member_rows[0].get("is_member") or 0) > 0 if member_rows else False

        # Actor's current scores
        actor_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN
                c.bloc_alignment_score          AS bloc_align,
                c.military_strength_score       AS military,
                c.diplomatic_centrality_score   AS centrality,
                c.geopolitical_influence_score  AS geo_influence,
                c.overall_vulnerability_score   AS vulnerability
        """, {"name": actor})
        ar = actor_rows[0] if actor_rows else {}

        # Other alliance members
        member_rows2 = _run(conn, """
            MATCH (other:Country)-[:MEMBER_OF]->(a:Alliance {name: $alliance})
            WHERE other.name <> $actor
            RETURN
                other.name                          AS country,
                other.military_strength_score       AS military,
                other.bloc_alignment_score          AS bloc_align,
                other.economic_influence_score      AS econ_influence,
                other.defense_spending_score        AS defense_spending
            LIMIT 20
        """, {"alliance": alliance, "actor": actor})

        # Actor's spending contribution to alliance
        spending_rows = _run(conn, """
            MATCH (c:Country {name: $actor})-[r:SPENDS_ON_DEFENSE]->(y:Year)
            WITH r ORDER BY y.year DESC
            RETURN r.normalized_weight AS spending_weight
            LIMIT 1
        """, {"actor": actor})
        actor_spending_weight = float(spending_rows[0].get("spending_weight") or 0.1) if spending_rows else 0.1

        # ── Actor score deltas ──────────────────────────────────────────
        actor_bloc_delta      = -clamp(0.25 * mag)
        actor_military_delta  = -clamp(0.15 * mag)
        actor_centrality_delta = -clamp(0.20 * mag)
        actor_vuln_delta      = clamp(0.20 * mag)   # more exposed without collective defense

        # If realigning, some scores shift rather than just fall
        actor_geo_delta = -clamp(0.10 * mag)
        if realign:
            actor_geo_delta = -clamp(0.05 * mag)  # partial recovery via new alignment

        actor_affected = AffectedCountry(
            country=actor,
            impact_type="direct",
            severity=_severity(mag * 0.5),
            score_deltas=[
                _make_delta("bloc_alignment_score",        ar.get("bloc_align"),  actor_bloc_delta),
                _make_delta("military_strength_score",     ar.get("military"),    actor_military_delta),
                _make_delta("diplomatic_centrality_score", ar.get("centrality"),  actor_centrality_delta),
                _make_delta("overall_vulnerability_score", ar.get("vulnerability"), actor_vuln_delta),
                _make_delta("geopolitical_influence_score", ar.get("geo_influence"), actor_geo_delta),
            ],
            summary=(
                f"{actor} exits {alliance}; loses collective defense guarantee, "
                f"alliance network access, and bloc alignment benefit."
                + (f" May realign toward {realign}." if realign else "")
                + (" (membership confirmed in graph)" if is_member else " (membership not found in graph)")
            ),
        )

        # ── Remaining member score deltas ───────────────────────────────
        # Members lose actor's defense contribution — small but real
        member_affected: list[AffectedCountry] = []
        for row in member_rows2[:10]:
            country = row.get("country")
            if not country:
                continue
            # Impact proportional to actor's spending weight in the alliance
            member_military_delta = -clamp(actor_spending_weight * 0.15 * mag)
            if abs(member_military_delta) < 0.005:
                continue
            member_affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity="low",
                score_deltas=[
                    _make_delta("military_strength_score", row.get("military"), member_military_delta, 0.6),
                    _make_delta("bloc_alignment_score",    row.get("bloc_align"), -0.02 * mag, 0.5),
                ],
                summary=(
                    f"{country} loses {actor}'s defense contribution to {alliance}; "
                    f"collective security slightly weakened."
                ),
            ))

        cascades: list[CascadeEffect] = [
            CascadeEffect(
                mechanism="Collective defense degradation",
                affected=f"Remaining {len(member_rows2)} {alliance} members",
                severity="medium",
                description=(
                    f"{actor}'s exit reduces {alliance}'s combined defense spending "
                    f"and strategic depth. Members near {actor}'s geography most exposed."
                ),
            ),
            CascadeEffect(
                mechanism="Actor's security vacuum",
                affected=actor,
                severity=_severity(mag * 0.5),
                description=(
                    f"{actor} loses Article 5-equivalent protection; "
                    f"adversaries may perceive window of opportunity."
                ),
            ),
        ]

        if realign:
            cascades.append(CascadeEffect(
                mechanism="Bloc realignment",
                affected=f"{actor} → {realign} sphere",
                severity="high",
                description=(
                    f"{actor} shifts alignment toward {realign}; "
                    f"strategic balance in the region changes meaningfully."
                ),
            ))

        return ScenarioResult(
            scenario_type=ScenarioType.ALLIANCE_EXIT,
            actor=actor,
            target=alliance,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=[actor_affected] + member_affected,
            cascade_effects=cascades,
            data_sources=["MEMBER_OF", "SPENDS_ON_DEFENSE", "bloc_alignment_score", "military_strength_score"],
            confidence=0.80 if is_member else 0.60,
        )
    finally:
        conn.close()


# =========================================================
# ALLIANCE FORMATION
# =========================================================

def run_alliance_formation(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: actor joins or forms a new alliance with target/third parties.
    Inverse of alliance_exit — scores improve.
    """
    actor    = req.actor
    alliance = req.target
    mag      = req.magnitude
    year     = req.year

    if not actor:
        return _empty_result(req, "alliance_formation requires an actor")

    conn = Neo4jConnection()
    try:
        actor_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN
                c.bloc_alignment_score          AS bloc_align,
                c.military_strength_score       AS military,
                c.diplomatic_centrality_score   AS centrality,
                c.overall_vulnerability_score   AS vulnerability
        """, {"name": actor})
        ar = actor_rows[0] if actor_rows else {}

        affected = [AffectedCountry(
            country=actor,
            impact_type="direct",
            severity="medium",
            score_deltas=[
                _make_delta("bloc_alignment_score",        ar.get("bloc_align"),  clamp(0.20 * mag)),
                _make_delta("military_strength_score",     ar.get("military"),    clamp(0.12 * mag)),
                _make_delta("diplomatic_centrality_score", ar.get("centrality"),  clamp(0.15 * mag)),
                _make_delta("overall_vulnerability_score", ar.get("vulnerability"), -clamp(0.15 * mag)),
            ],
            summary=(
                f"{actor} joins{f' {alliance}' if alliance else ' new alliance'}; "
                f"gains collective defense, expanded diplomatic network, "
                f"and reduced vulnerability."
            ),
        )]

        return ScenarioResult(
            scenario_type=ScenarioType.ALLIANCE_FORMATION,
            actor=actor,
            target=alliance,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=affected,
            cascade_effects=[
                CascadeEffect(
                    mechanism="Security guarantee",
                    affected=actor,
                    severity="medium",
                    description=f"{actor} gains collective defense commitment from alliance partners.",
                )
            ],
            data_sources=["MEMBER_OF", "bloc_alignment_score"],
            confidence=0.75,
        )
    finally:
        conn.close()


# =========================================================
# ALLIANCE EXPANSION
# =========================================================

def run_alliance_expansion(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: an existing alliance (target) admits new member (actor).
    E.g. Ukraine joins NATO.
    Affects both the new member and the existing alliance members.
    """
    new_member = req.actor
    alliance   = req.target
    mag        = req.magnitude
    year       = req.year

    if not new_member or not alliance:
        return _empty_result(req, "alliance_expansion requires actor (new member) and target (alliance)")

    conn = Neo4jConnection()
    try:
        # New member current scores
        nm_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN
                c.bloc_alignment_score       AS bloc_align,
                c.military_strength_score    AS military,
                c.overall_vulnerability_score AS vulnerability,
                c.conflict_risk_score        AS conflict_risk
        """, {"name": new_member})
        nm = nm_rows[0] if nm_rows else {}

        # Existing members — do they gain or face increased risk?
        existing_rows = _run(conn, """
            MATCH (c:Country)-[:MEMBER_OF]->(a:Alliance {name: $alliance})
            RETURN
                c.name                          AS country,
                c.military_strength_score       AS military,
                c.global_risk_score             AS global_risk
            LIMIT 15
        """, {"alliance": alliance})

        new_member_conflict_risk = float(nm.get("conflict_risk") or 0.0)

        affected: list[AffectedCountry] = [
            AffectedCountry(
                country=new_member,
                impact_type="direct",
                severity="high",
                score_deltas=[
                    _make_delta("bloc_alignment_score",        nm.get("bloc_align"),    clamp(0.30 * mag)),
                    _make_delta("overall_vulnerability_score", nm.get("vulnerability"), -clamp(0.25 * mag)),
                    _make_delta("military_strength_score",     nm.get("military"),      clamp(0.15 * mag)),
                ],
                summary=(
                    f"{new_member} joins {alliance}; gains collective defense guarantee, "
                    f"significantly reducing strategic vulnerability."
                ),
            )
        ]

        # Existing members: small risk increase if new member has active conflicts
        for row in existing_rows[:8]:
            country = row.get("country")
            if not country:
                continue
            risk_delta = clamp(new_member_conflict_risk * 0.05 * mag)
            if risk_delta < 0.005:
                continue
            affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity="low",
                score_deltas=[
                    _make_delta("global_risk_score", row.get("global_risk"), risk_delta, 0.5),
                ],
                summary=(
                    f"{country} assumes partial collective defense obligations "
                    f"for {new_member}; risk exposure slightly increases."
                ),
            ))

        cascades = [
            CascadeEffect(
                mechanism="Security perimeter expansion",
                affected=alliance,
                severity="medium",
                description=f"{alliance} extends security guarantee to {new_member}; border with adversaries potentially expands.",
            ),
        ]
        if new_member_conflict_risk > 0.5:
            cascades.append(CascadeEffect(
                mechanism="Inherited conflict exposure",
                affected=f"All {len(existing_rows)} {alliance} members",
                severity="high",
                description=(
                    f"{new_member}'s high conflict risk ({new_member_conflict_risk:.2f}) "
                    f"is partially inherited by the alliance under Article 5 equivalents."
                ),
            ))

        return ScenarioResult(
            scenario_type=ScenarioType.ALLIANCE_EXPANSION,
            actor=new_member,
            target=alliance,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=affected,
            cascade_effects=cascades,
            data_sources=["MEMBER_OF", "conflict_risk_score", "bloc_alignment_score"],
            confidence=0.78,
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