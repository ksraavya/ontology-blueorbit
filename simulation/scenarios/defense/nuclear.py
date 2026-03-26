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
# NUCLEAR THREAT
# =========================================================

def run_nuclear_threat(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: nuclear test, doctrine change, or explicit nuclear threat.

    Logic:
    - Actor's military_strength_score jumps (credible deterrent signal)
    - Target's conflict_risk_score and global_risk_score spike
    - Global financial markets and energy prices react
    - Alliance members of target face existential reassurance pressure
    - Non-proliferation regime stress: regional powers may accelerate programs

    Reads:
        military_strength_score, conflict_risk_score, global_risk_score,
        nuclear_status, MEMBER_OF, strategic_influence_score
    """
    actor  = req.actor
    target = req.target
    mag    = req.magnitude
    year   = req.year
    threat_type = req.extra_params.get("threat_type", "doctrine_change")
    # threat_type: "test" | "doctrine_change" | "explicit_threat"

    if not actor:
        return _empty_result(req, "nuclear_threat requires an actor country")

    conn = Neo4jConnection()
    try:
        actor_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.military_strength_score AS military,
                   c.strategic_influence_score AS strat_influence,
                   c.nuclear_status AS nuclear_status,
                   c.global_risk_score AS global_risk
        """, {"name": actor})
        ar = actor_rows[0] if actor_rows else {}

        is_confirmed_nuclear = (ar.get("nuclear_status") in ["confirmed", "undeclared"])

        # Actor: nuclear signal increases deterrence, also raises own risk profile
        if threat_type == "test":
            actor_military_delta = clamp(0.15 * mag)
            actor_influence_delta = clamp(0.08 * mag)
        elif threat_type == "explicit_threat":
            actor_military_delta = clamp(0.05 * mag)
            actor_influence_delta = -clamp(0.05 * mag)  # reputational cost
        else:
            actor_military_delta = clamp(0.08 * mag)
            actor_influence_delta = clamp(0.04 * mag)

        actor_risk_delta = clamp(0.10 * mag)  # actor also escalates own exposure

        actor_affected = AffectedCountry(
            country=actor,
            impact_type="direct",
            severity=_severity(mag * 0.5),
            score_deltas=[
                _make_delta("military_strength_score",      ar.get("military"),       actor_military_delta),
                _make_delta("strategic_influence_score",    ar.get("strat_influence"), actor_influence_delta),
                _make_delta("global_risk_score",            ar.get("global_risk"),    actor_risk_delta),
            ],
            summary=(
                f"{actor} {'conducts nuclear test' if threat_type == 'test' else 'issues nuclear threat/doctrine change'}; "
                f"deterrence posture {'confirmed' if is_confirmed_nuclear else 'signaled'}, "
                f"international pressure escalates."
            ),
        )

        affected: list[AffectedCountry] = [actor_affected]

        # Target: if specified, faces existential risk spike
        if target:
            target_rows = _run(conn, """
                MATCH (c:Country {name: $name})
                RETURN c.conflict_risk_score AS conflict_risk,
                       c.global_risk_score AS global_risk,
                       c.overall_vulnerability_score AS vulnerability,
                       c.military_strength_score AS military
            """, {"name": target})
            tr = target_rows[0] if target_rows else {}

            target_conflict_delta = clamp(0.30 * mag)
            target_risk_delta     = clamp(0.35 * mag)
            target_vuln_delta     = clamp(0.20 * mag)

            affected.append(AffectedCountry(
                country=target,
                impact_type="direct",
                severity="critical",
                score_deltas=[
                    _make_delta("conflict_risk_score",         tr.get("conflict_risk"),  target_conflict_delta),
                    _make_delta("global_risk_score",           tr.get("global_risk"),    target_risk_delta),
                    _make_delta("overall_vulnerability_score", tr.get("vulnerability"), target_vuln_delta),
                ],
                summary=(
                    f"{target} faces direct nuclear threat from {actor}; "
                    f"existential risk signal triggers emergency defense posture."
                ),
            ))

        # Alliance members of target — reassurance crisis
        if target:
            alliance_rows = _run(conn, """
                MATCH (t:Country {name: $target})-[:MEMBER_OF]->(al:Alliance)<-[:MEMBER_OF]-(member:Country)
                WHERE member.name <> $target AND member.name <> $actor
                RETURN member.name AS country,
                       member.military_strength_score AS military,
                       member.global_risk_score AS global_risk,
                       al.name AS alliance
                LIMIT 8
            """, {"target": target, "actor": actor})

            for row in alliance_rows[:5]:
                country = row.get("country")
                if not country:
                    continue
                affected.append(AffectedCountry(
                    country=country,
                    impact_type="cascade",
                    severity="medium",
                    score_deltas=[
                        _make_delta("global_risk_score", row.get("global_risk"), clamp(0.12 * mag), 0.6),
                        _make_delta("military_strength_score", row.get("military"), -clamp(0.02 * mag), 0.5),
                    ],
                    summary=(
                        f"{country} ({row.get('alliance')}) faces extended deterrence credibility crisis — "
                        f"must demonstrate commitment to {target}'s defense."
                    ),
                ))

        # Regional nuclear proliferation pressure
        nuclear_candidates = _run(conn, """
            MATCH (c:Country)
            WHERE c.nuclear_status IS NULL
              AND c.military_strength_score >= 0.4
              AND c.name <> $actor
            RETURN c.name AS country,
                   c.military_strength_score AS military,
                   c.global_risk_score AS global_risk
            ORDER BY c.military_strength_score DESC
            LIMIT 5
        """, {"actor": actor})

        for row in nuclear_candidates[:3]:
            country = row.get("country")
            if not country:
                continue
            affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity="low",
                score_deltas=[
                    _make_delta("global_risk_score", row.get("global_risk"), clamp(0.05 * mag), 0.4),
                ],
                summary=(
                    f"{country} faces increased pressure to reconsider nuclear posture "
                    f"as regional deterrence balance shifts."
                ),
            ))

        cascades = [
            CascadeEffect(
                mechanism=f"Nuclear {threat_type.replace('_', ' ')}",
                affected=target or "global order",
                severity="critical",
                description=(
                    f"{actor}'s nuclear {threat_type.replace('_', ' ')} crosses a critical threshold; "
                    f"international community faces credibility test of deterrence architecture."
                ),
            ),
            CascadeEffect(
                mechanism="Non-proliferation regime stress",
                affected="NPT and global disarmament efforts",
                severity="high",
                description=(
                    "Nuclear signal undermines confidence in non-proliferation norms; "
                    "other states may accelerate latent nuclear programs."
                ),
            ),
            CascadeEffect(
                mechanism="Financial market shock",
                affected="Global financial markets",
                severity="high",
                description=(
                    "Nuclear escalation signals trigger risk-off in global markets; "
                    "energy, gold, and safe-haven assets spike."
                ),
            ),
        ]

        return ScenarioResult(
            scenario_type=ScenarioType.NUCLEAR_THREAT,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=affected,
            cascade_effects=cascades,
            global_risk_delta=clamp(0.15 * mag),
            data_sources=[
                "military_strength_score", "conflict_risk_score",
                "nuclear_status", "MEMBER_OF", "strategic_influence_score",
            ],
            confidence=0.70,
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