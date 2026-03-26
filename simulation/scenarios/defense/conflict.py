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
# CONFLICT ESCALATION
# =========================================================

def run_conflict_escalation(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: conflict in target country (or between actor and target) escalates.

    Logic:
    - Target's conflict_risk_score rises significantly
    - Countries with trade dependency on target face supply chain disruption
    - Countries with energy dependency on target/actor face energy vulnerability rise
    - Neighboring countries (same region) face spillover risk
    - NATO/alliance trigger check if actor is in an alliance

    Reads:
        conflict_risk_score, HAS_CONFLICT_STATS, EXPORTS_TO, IMPORTS_ENERGY_FROM
        MEMBER_OF, BELONGS_TO, trade_vulnerability_score, energy_vulnerability_score
    """
    actor  = req.actor    # may be None for "conflict in X"
    target = req.target
    mag    = req.magnitude
    year   = req.year

    primary = target or actor
    if not primary:
        return _empty_result(req, "conflict_escalation requires at least a target country")

    conn = Neo4jConnection()
    try:
        # Primary country scores
        primary_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN
                c.conflict_risk_score           AS conflict_risk,
                c.global_risk_score             AS global_risk,
                c.economic_influence_score      AS econ_influence,
                c.overall_vulnerability_score   AS vulnerability,
                c.trade_vulnerability_score     AS trade_vuln,
                c.region                        AS region
        """, {"name": primary})
        pr = primary_rows[0] if primary_rows else {}

        current_conflict = float(pr.get("conflict_risk") or 0.3)
        conflict_delta   = clamp(0.35 * mag)
        risk_delta       = clamp(0.30 * mag)
        econ_delta       = -clamp(0.20 * mag)
        vuln_delta       = clamp(0.25 * mag)

        primary_affected = AffectedCountry(
            country=primary,
            impact_type="direct",
            severity=_severity((current_conflict + conflict_delta) * mag),
            score_deltas=[
                _make_delta("conflict_risk_score",       pr.get("conflict_risk"),   conflict_delta),
                _make_delta("global_risk_score",         pr.get("global_risk"),     risk_delta),
                _make_delta("economic_influence_score",  pr.get("econ_influence"),  econ_delta),
                _make_delta("overall_vulnerability_score", pr.get("vulnerability"), vuln_delta),
            ],
            summary=(
                f"Conflict escalation in {primary} drives conflict_risk to "
                f"{clamp(current_conflict + conflict_delta):.2f}; "
                f"economic activity disrupted, vulnerability rises sharply."
            ),
        )

        # Trade dependency cascade
        trade_rows = _run(conn, """
            MATCH (other:Country)-[r:HAS_TRADE_DEPENDENCY_ON]->(c:Country {name: $primary})
            WHERE r.year = $year AND r.dependency >= 0.1
            RETURN
                other.name                      AS country,
                r.dependency                    AS dependency,
                other.trade_vulnerability_score AS trade_vuln,
                other.global_risk_score         AS global_risk
            ORDER BY r.dependency DESC
            LIMIT 10
        """, {"primary": primary, "year": year})

        # Energy dependency cascade
        energy_rows = _run(conn, """
            MATCH (other:Country)-[r:IMPORTS_ENERGY_FROM]->(c:Country {name: $primary})
            WHERE r.year = $year AND r.dependency >= 0.1
            RETURN
                other.name                          AS country,
                r.dependency                        AS dependency,
                other.energy_vulnerability_score    AS energy_vuln
            ORDER BY r.dependency DESC
            LIMIT 8
        """, {"primary": primary, "year": year})

        # Regional neighbors
        region_rows = _run(conn, """
            MATCH (c:Country {name: $primary})-[:BELONGS_TO]->(r:Region)<-[:BELONGS_TO]-(neighbor:Country)
            WHERE neighbor.name <> $primary
            RETURN
                neighbor.name                       AS country,
                neighbor.conflict_risk_score        AS conflict_risk,
                neighbor.global_risk_score          AS global_risk
            LIMIT 8
        """, {"primary": primary})

        affected: list[AffectedCountry] = [primary_affected]
        seen: set[str] = {primary}

        for row in trade_rows:
            country = row.get("country")
            if not country or country in seen:
                continue
            seen.add(country)
            dep   = float(row.get("dependency") or 0.0)
            spill = clamp(dep * conflict_delta * 0.4)
            affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity=_severity(spill),
                score_deltas=[
                    _make_delta("trade_vulnerability_score", row.get("trade_vuln"), spill, 0.65),
                    _make_delta("global_risk_score",         row.get("global_risk"), spill * 0.5, 0.55),
                ],
                summary=f"{country} has {dep*100:.0f}% trade dependency on {primary}; conflict disrupts supply.",
            ))

        for row in energy_rows:
            country = row.get("country")
            if not country or country in seen:
                continue
            seen.add(country)
            dep   = float(row.get("dependency") or 0.0)
            spill = clamp(dep * conflict_delta * 0.5)
            affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity=_severity(spill),
                score_deltas=[
                    _make_delta("energy_vulnerability_score", row.get("energy_vuln"), spill, 0.70),
                ],
                summary=f"{country} imports {dep*100:.0f}% of energy from {primary}; conflict disrupts supply.",
            ))

        for row in region_rows:
            country = row.get("country")
            if not country or country in seen:
                continue
            seen.add(country)
            neighbor_risk = float(row.get("conflict_risk") or 0.0)
            spill = clamp(conflict_delta * 0.2 * mag)
            if spill < 0.02:
                continue
            affected.append(AffectedCountry(
                country=country,
                impact_type="regional",
                severity="low",
                score_deltas=[
                    _make_delta("conflict_risk_score", row.get("conflict_risk"), spill, 0.5),
                    _make_delta("global_risk_score",   row.get("global_risk"),   spill * 0.5, 0.45),
                ],
                summary=f"{country} faces regional spillover from {primary} conflict.",
            ))

        # Alliance trigger check
        alliance_rows = _run(conn, """
            MATCH (c:Country {name: $primary})-[:MEMBER_OF]->(a:Alliance)
            RETURN a.name AS alliance
        """, {"primary": primary})
        alliances = [r["alliance"] for r in alliance_rows]

        cascades: list[CascadeEffect] = [
            CascadeEffect(
                mechanism="Direct conflict damage",
                affected=primary,
                severity=_severity(conflict_delta * mag),
                description=f"Infrastructure damage, displacement, and economic contraction in {primary}.",
            ),
            CascadeEffect(
                mechanism="Regional instability",
                affected=f"{len(region_rows)} neighboring countries",
                severity="medium",
                description="Refugee flows, border security costs, and trade disruption spread to neighbors.",
            ),
        ]

        if alliances and mag >= 1.5:
            cascades.append(CascadeEffect(
                mechanism="Alliance activation risk",
                affected=", ".join(alliances),
                severity="high",
                description=(
                    f"{primary} is a member of {', '.join(alliances)}. "
                    f"Escalation above threshold may trigger collective defense obligations."
                ),
            ))

        return ScenarioResult(
            scenario_type=ScenarioType.CONFLICT_ESCALATION,
            actor=actor,
            target=target or primary,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=sorted(affected, key=lambda x: abs(sum(d.delta for d in x.score_deltas)), reverse=True),
            cascade_effects=cascades,
            data_sources=[
                "conflict_risk_score", "HAS_CONFLICT_STATS", "HAS_TRADE_DEPENDENCY_ON",
                "IMPORTS_ENERGY_FROM", "MEMBER_OF", "BELONGS_TO",
            ],
            confidence=0.80,
        )
    finally:
        conn.close()


# =========================================================
# CONFLICT DE-ESCALATION / CEASEFIRE
# =========================================================

def run_conflict_deescalation(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: active conflict reduces or ceasefire is reached.
    Inverse of escalation — scores improve, but slowly.
    """
    target = req.target or req.actor
    mag    = req.magnitude
    year   = req.year

    if not target:
        return _empty_result(req, "conflict_deescalation requires a target country")

    conn = Neo4jConnection()
    try:
        target_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN
                c.conflict_risk_score           AS conflict_risk,
                c.global_risk_score             AS global_risk,
                c.economic_influence_score      AS econ_influence,
                c.overall_vulnerability_score   AS vulnerability
        """, {"name": target})
        tr = target_rows[0] if target_rows else {}

        # De-escalation improves scores but more slowly than escalation damages them
        conflict_delta = -clamp(0.20 * mag)
        risk_delta     = -clamp(0.15 * mag)
        econ_delta     =  clamp(0.10 * mag)
        vuln_delta     = -clamp(0.12 * mag)

        affected = [AffectedCountry(
            country=target,
            impact_type="direct",
            severity="medium",
            score_deltas=[
                _make_delta("conflict_risk_score",         tr.get("conflict_risk"),  conflict_delta),
                _make_delta("global_risk_score",           tr.get("global_risk"),    risk_delta),
                _make_delta("economic_influence_score",    tr.get("econ_influence"), econ_delta),
                _make_delta("overall_vulnerability_score", tr.get("vulnerability"),  vuln_delta),
            ],
            summary=(
                f"Ceasefire or de-escalation in {target} reduces immediate conflict risk; "
                f"recovery is gradual — economic normalization takes 12-36 months."
            ),
        )]

        return ScenarioResult(
            scenario_type=ScenarioType.CONFLICT_DEESCALATION,
            actor=req.actor,
            target=req.target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=affected,
            cascade_effects=[
                CascadeEffect(
                    mechanism="Reconstruction phase",
                    affected=target,
                    severity="medium",
                    description=(
                        f"Post-conflict reconstruction creates economic opportunity "
                        f"but requires sustained international support."
                    ),
                )
            ],
            data_sources=["conflict_risk_score", "global_risk_score"],
            confidence=0.72,
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