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
# CYBER ATTACK
# =========================================================

def run_cyber_attack(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: large-scale state-sponsored cyber offensive against target.

    Logic:
    - Target's economic_power_score falls (infrastructure damage, recovery costs)
    - Target's global_risk_score rises (credibility of state protection questioned)
    - Target's overall_vulnerability_score increases
    - If target is critical energy/finance infrastructure: spillover to supply chain
    - Actor gains some strategic_influence (demonstrated capability) but faces retaliation
    - Allies of target face similar exposure (shared infrastructure)

    Reads:
        economic_power_score, global_risk_score, overall_vulnerability_score,
        energy_vulnerability_score, MEMBER_OF, IS_INFLUENTIAL_TO
    """
    actor  = req.actor
    target = req.target
    mag    = req.magnitude
    year   = req.year
    target_sector = req.extra_params.get("sector", "general")
    # sector: "energy" | "finance" | "government" | "general"

    if not target:
        return _empty_result(req, "cyber_attack requires a target country")

    conn = Neo4jConnection()
    try:
        target_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.economic_power_score AS econ_power,
                   c.global_risk_score AS global_risk,
                   c.overall_vulnerability_score AS vulnerability,
                   c.energy_vulnerability_score AS energy_vuln,
                   c.trade_vulnerability_score AS trade_vuln
        """, {"name": target})
        tr = target_rows[0] if target_rows else {}

        # Sector-specific impact multipliers
        if target_sector == "energy":
            sector_mult = 1.5
            sector_note = "critical energy infrastructure attack"
        elif target_sector == "finance":
            sector_mult = 1.4
            sector_note = "financial system attack"
        elif target_sector == "government":
            sector_mult = 1.2
            sector_note = "government systems attack"
        else:
            sector_mult = 1.0
            sector_note = "broad-spectrum cyber attack"

        target_econ_delta  = -clamp(0.12 * mag * sector_mult)
        target_risk_delta  = clamp(0.18 * mag * sector_mult)
        target_vuln_delta  = clamp(0.15 * mag * sector_mult)

        # Additional energy vulnerability if energy sector targeted
        energy_delta = clamp(0.20 * mag) if target_sector == "energy" else 0.0

        target_deltas = [
            _make_delta("economic_power_score",      tr.get("econ_power"),   target_econ_delta),
            _make_delta("global_risk_score",         tr.get("global_risk"),  target_risk_delta),
            _make_delta("overall_vulnerability_score", tr.get("vulnerability"), target_vuln_delta),
        ]
        if energy_delta > 0:
            target_deltas.append(
                _make_delta("energy_vulnerability_score", tr.get("energy_vuln"), energy_delta)
            )

        target_affected = AffectedCountry(
            country=target,
            impact_type="direct",
            severity=_severity(target_risk_delta),
            score_deltas=target_deltas,
            summary=(
                f"{target} suffers {sector_note}; critical systems disrupted, "
                f"recovery costs estimated at billions. State authority questioned."
            ),
        )

        affected: list[AffectedCountry] = [target_affected]

        # Actor: gains capability signal but faces attribution and retaliation
        if actor:
            actor_rows = _run(conn, """
                MATCH (c:Country {name: $name})
                RETURN c.strategic_influence_score AS strat_influence,
                       c.global_risk_score AS global_risk
            """, {"name": actor})
            ar = actor_rows[0] if actor_rows else {}

            actor_influence_delta = clamp(0.04 * mag)   # demonstrated capability
            actor_risk_delta      = clamp(0.06 * mag)   # retaliation / attribution risk

            affected.append(AffectedCountry(
                country=actor,
                impact_type="direct",
                severity="low",
                score_deltas=[
                    _make_delta("strategic_influence_score", ar.get("strat_influence"), actor_influence_delta, 0.5),
                    _make_delta("global_risk_score",         ar.get("global_risk"),     actor_risk_delta, 0.5),
                ],
                summary=(
                    f"{actor} demonstrates advanced cyber capability; "
                    f"faces attribution risk and potential retaliatory cyber operations."
                ),
            ))

        # Allies sharing critical infrastructure face spillover
        alliance_rows = _run(conn, """
            MATCH (t:Country {name: $target})-[:MEMBER_OF]->(al:Alliance)<-[:MEMBER_OF]-(member:Country)
            WHERE member.name <> $target
            RETURN member.name AS country,
                   member.overall_vulnerability_score AS vulnerability
            LIMIT 6
        """, {"target": target})

        for row in alliance_rows[:4]:
            country = row.get("country")
            if not country:
                continue
            spill = clamp(0.05 * mag * sector_mult)
            affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity="low",
                score_deltas=[
                    _make_delta("overall_vulnerability_score", row.get("vulnerability"), spill, 0.5),
                ],
                summary=(
                    f"{country} shares digital infrastructure with {target}; "
                    f"lateral movement risk and attack spillover."
                ),
            ))

        cascades = [
            CascadeEffect(
                mechanism=sector_note.title(),
                affected=target,
                severity=_severity(target_risk_delta),
                description=(
                    f"Coordinated cyber offensive disrupts {target}'s {target_sector} sector; "
                    f"recovery timeline 3-18 months depending on damage scope."
                ),
            ),
            CascadeEffect(
                mechanism="Cyber deterrence erosion",
                affected="Global cyberspace norms",
                severity="medium",
                description=(
                    "Successful state-sponsored attack demonstrates offensive cyber "
                    "capability; other states re-evaluate cyber defense postures."
                ),
            ),
        ]
        if target_sector in ("energy", "finance"):
            cascades.append(CascadeEffect(
                mechanism="Economic disruption cascade",
                affected=f"{target} and trading partners",
                severity="high",
                description=(
                    f"Disruption of {target}'s {target_sector} sector creates "
                    f"downstream effects on supply chains and financial flows."
                ),
            ))

        return ScenarioResult(
            scenario_type=ScenarioType.CYBER_ATTACK,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=affected,
            cascade_effects=cascades,
            data_sources=[
                "economic_power_score", "global_risk_score",
                "overall_vulnerability_score", "MEMBER_OF",
            ],
            confidence=0.68,
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