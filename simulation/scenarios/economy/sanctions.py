from __future__ import annotations

import logging
from typing import Any

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


def _make_delta(
    score_name: str,
    current: float | None,
    delta: float,
    confidence: float = 0.8,
) -> ScoreDelta:
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


# =========================================================
# SANCTIONS IMPOSITION
# =========================================================

def run_sanctions(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: actor imposes economic sanctions on target.

    Logic:
    - Target loses trade flows from actor (EXPORTS_TO edges)
    - Target's trade_vulnerability rises
    - Target's economic_influence_score falls proportional to
      actor's share of target's total trade
    - Third countries that depend heavily on target may be affected
    - Actor may face retaliation risk (minor vulnerability increase)

    Reads:
        EXPORTS_TO, HAS_TRADE_VOLUME_WITH, HAS_TRADE_AGREEMENT_WITH
        trade_vulnerability_score, economic_influence_score, economic_power_score
    """
    actor  = req.actor
    target = req.target
    mag    = req.magnitude
    year   = req.year

    if not actor or not target:
        return _empty_result(req, "sanctions requires both actor and target")

    conn = Neo4jConnection()
    try:
        # ── 1. How much does actor trade with target? ─────────────────────
        trade_rows = _run(conn, """
            MATCH (a:Country {name: $actor})-[r:EXPORTS_TO]->(t:Country {name: $target})
            WHERE r.year = $year
            RETURN r.value AS export_value, r.dependency AS actor_dep_on_target
            UNION ALL
            MATCH (t:Country {name: $target})-[r:EXPORTS_TO]->(a:Country {name: $actor})
            WHERE r.year = $year
            RETURN r.value AS export_value, r.dependency AS actor_dep_on_target
        """, {"actor": actor, "target": target, "year": year})

        bilateral_value = sum(r.get("export_value") or 0.0 for r in trade_rows)

        # Actor's share of target's total trade volume
        vol_rows = _run(conn, """
            MATCH (t:Country {name: $target})-[r:HAS_TRADE_VOLUME_WITH]-(:Country)
            WHERE r.year = $year
            RETURN sum(r.value) AS total_volume
        """, {"target": target, "year": year})
        total_vol = (vol_rows[0].get("total_volume") or 1.0) if vol_rows else 1.0
        actor_share = clamp(bilateral_value / total_vol) if total_vol > 0 else 0.0

        # ── 2. Target's current scores ────────────────────────────────────
        target_scores = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN
                c.trade_vulnerability_score     AS trade_vuln,
                c.economic_influence_score      AS econ_influence,
                c.economic_power_score          AS econ_power,
                c.energy_vulnerability_score    AS energy_vuln,
                c.global_risk_score             AS global_risk
        """, {"name": target})
        ts = target_scores[0] if target_scores else {}

        # ── 3. Actor's current scores ─────────────────────────────────────
        actor_scores = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN
                c.trade_vulnerability_score  AS trade_vuln,
                c.economic_influence_score   AS econ_influence
        """, {"name": actor})
        as_ = actor_scores[0] if actor_scores else {}

        # ── 4. Compute deltas ─────────────────────────────────────────────
        # Target: trade vulnerability rises proportional to actor's trade share
        target_trade_delta   = clamp(actor_share * 0.3 * mag)
        target_econ_delta    = -clamp(actor_share * 0.25 * mag)
        target_risk_delta    = clamp(actor_share * 0.2 * mag)

        # Actor: minor vulnerability increase (retaliation risk)
        actor_trade_delta    = clamp(actor_share * 0.05 * mag)

        target_affected = AffectedCountry(
            country=target,
            impact_type="direct",
            severity=_severity(actor_share * mag),
            score_deltas=[
                _make_delta("trade_vulnerability_score", ts.get("trade_vuln"), target_trade_delta),
                _make_delta("economic_influence_score",  ts.get("econ_influence"), target_econ_delta),
                _make_delta("global_risk_score",         ts.get("global_risk"), target_risk_delta),
            ],
            exposure_usd=bilateral_value,
            summary=(
                f"{target} loses access to {actor} market; "
                f"bilateral trade value ~${bilateral_value/1e9:.1f}B at risk "
                f"({actor_share*100:.1f}% of total trade)."
            ),
        )

        actor_affected = AffectedCountry(
            country=actor,
            impact_type="direct",
            severity="low",
            score_deltas=[
                _make_delta("trade_vulnerability_score", as_.get("trade_vuln"), actor_trade_delta, 0.6),
            ],
            exposure_usd=bilateral_value * 0.3,
            summary=(
                f"{actor} faces minor retaliation risk and loses "
                f"~${bilateral_value*0.3/1e9:.1f}B in bilateral trade exposure."
            ),
        )

        # ── 5. Third-country cascade — who else depends on target? ────────
        cascade_rows = _run(conn, """
            MATCH (other:Country)-[r:HAS_TRADE_DEPENDENCY_ON]->(t:Country {name: $target})
            WHERE r.year = $year AND r.dependency >= 0.1 AND other.name <> $actor
            RETURN other.name AS country,
                   r.dependency AS dependency,
                   other.trade_vulnerability_score AS trade_vuln,
                   other.economic_influence_score  AS econ_influence
            ORDER BY r.dependency DESC
            LIMIT 8
        """, {"target": target, "year": year, "actor": actor})

        cascade_countries: list[AffectedCountry] = []
        for row in cascade_rows:
            dep = float(row.get("dependency") or 0.0)
            spill = clamp(dep * target_trade_delta * 0.5)
            if spill < 0.01:
                continue
            cascade_countries.append(AffectedCountry(
                country=row["country"],
                impact_type="cascade",
                severity=_severity(spill),
                score_deltas=[
                    _make_delta("trade_vulnerability_score", row.get("trade_vuln"), spill, 0.6),
                ],
                summary=(
                    f"{row['country']} has {dep*100:.0f}% trade dependency on {target}; "
                    f"sanctions disruption spills over."
                ),
            ))

        # ── 6. Cascade effects ────────────────────────────────────────────
        cascades: list[CascadeEffect] = [
            CascadeEffect(
                mechanism="Trade flow disruption",
                affected=target,
                severity=_severity(actor_share * mag),
                description=(
                    f"Bilateral trade of ~${bilateral_value/1e9:.1f}B halts; "
                    f"{target} must find alternative export markets."
                ),
            ),
        ]
        if actor_share > 0.2:
            cascades.append(CascadeEffect(
                mechanism="Currency and reserves pressure",
                affected=target,
                severity="high",
                description=(
                    f"Loss of {actor_share*100:.0f}% of trade could pressure "
                    f"{target}'s currency and foreign reserves."
                ),
            ))
        if cascade_countries:
            cascades.append(CascadeEffect(
                mechanism="Supply chain contagion",
                affected=f"{len(cascade_countries)} third-party economies",
                severity="medium",
                description=(
                    f"Countries with high trade dependency on {target} face "
                    f"secondary disruption as {target} redirects trade flows."
                ),
            ))

        all_affected = [target_affected, actor_affected] + cascade_countries

        return ScenarioResult(
            scenario_type=ScenarioType.SANCTIONS,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",   # filled by engine after LLM narrative
            summary="",
            affected_countries=all_affected,
            cascade_effects=cascades,
            data_sources=[
                "EXPORTS_TO", "HAS_TRADE_VOLUME_WITH", "HAS_TRADE_DEPENDENCY_ON",
                "trade_vulnerability_score", "economic_influence_score",
            ],
            confidence=0.85 if bilateral_value > 0 else 0.5,
            missing_data=_missing([ts.get("trade_vuln"), ts.get("econ_influence")],
                                   ["trade_vulnerability_score", "economic_influence_score"]),
        )
    finally:
        conn.close()


# =========================================================
# SANCTIONS REMOVAL
# =========================================================

def run_sanctions_removal(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: existing sanctions on target are lifted by actor.
    Inverse of sanctions — scores improve for the previously sanctioned country.
    """
    actor  = req.actor
    target = req.target
    mag    = req.magnitude
    year   = req.year

    if not actor or not target:
        return _empty_result(req, "sanctions_removal requires both actor and target")

    conn = Neo4jConnection()
    try:
        # Check if sanctions actually exist
        sanction_rows = _run(conn, """
            MATCH (a:Country {name: $actor})-[r:IMPOSED_SANCTIONS_ON]->(t:Country {name: $target})
            RETURN count(r) AS sanction_count
        """, {"actor": actor, "target": target})
        is_sanctioned = (sanction_rows[0].get("sanction_count") or 0) > 0 if sanction_rows else False

        target_scores = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN
                c.trade_vulnerability_score  AS trade_vuln,
                c.economic_influence_score   AS econ_influence,
                c.global_risk_score          AS global_risk
        """, {"name": target})
        ts = target_scores[0] if target_scores else {}

        # Removal improves scores — inverse of sanctions but slightly smaller
        # because trust/normalization takes time
        improvement = 0.15 * mag

        affected = AffectedCountry(
            country=target,
            impact_type="direct",
            severity="medium",
            score_deltas=[
                _make_delta("trade_vulnerability_score", ts.get("trade_vuln"),   -improvement),
                _make_delta("economic_influence_score",  ts.get("econ_influence"), improvement * 0.7),
                _make_delta("global_risk_score",         ts.get("global_risk"),  -improvement * 0.5),
            ],
            summary=(
                f"{target} regains market access as {actor} lifts sanctions; "
                f"trade normalization expected over 12-24 months."
                + (" (sanctions confirmed in graph)" if is_sanctioned else " (no active sanctions found in graph)")
            ),
        )

        return ScenarioResult(
            scenario_type=ScenarioType.SANCTIONS_REMOVAL,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=[affected],
            cascade_effects=[
                CascadeEffect(
                    mechanism="Trade normalization",
                    affected=target,
                    severity="medium",
                    description=f"{target}'s export channels to {actor} reopen; investment flows likely to follow.",
                )
            ],
            data_sources=["IMPOSED_SANCTIONS_ON", "trade_vulnerability_score"],
            confidence=0.80,
        )
    finally:
        conn.close()


# =========================================================
# SANCTIONS COALITION
# =========================================================

def run_sanctions_coalition(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: multiple actors jointly sanction one target.
    Actor = primary sanctioner, third_parties = coalition members.
    Compounds individual sanctions effects.
    """
    actor        = req.actor
    target       = req.target
    coalition    = [actor] + req.third_parties if actor else req.third_parties
    mag          = req.magnitude
    year         = req.year

    if not target or not coalition:
        return _empty_result(req, "sanctions_coalition requires a target and at least one actor")

    conn = Neo4jConnection()
    try:
        # Sum bilateral trade from all coalition members
        total_bilateral = 0.0
        coalition_shares: dict[str, float] = {}

        for member in coalition:
            if not member:
                continue
            rows = _run(conn, """
                MATCH (a:Country {name: $actor})-[r:EXPORTS_TO]->(t:Country {name: $target})
                WHERE r.year = $year
                RETURN coalesce(r.value, 0) AS val
                UNION ALL
                MATCH (t:Country {name: $target})-[r:EXPORTS_TO]->(a:Country {name: $actor})
                WHERE r.year = $year
                RETURN coalesce(r.value, 0) AS val
            """, {"actor": member, "target": target, "year": year})
            member_bilateral = sum(r.get("val") or 0.0 for r in rows)
            total_bilateral += member_bilateral
            coalition_shares[member] = member_bilateral

        vol_rows = _run(conn, """
            MATCH (t:Country {name: $target})-[r:HAS_TRADE_VOLUME_WITH]-(:Country)
            WHERE r.year = $year
            RETURN sum(r.value) AS total_volume
        """, {"target": target, "year": year})
        total_vol  = (vol_rows[0].get("total_volume") or 1.0) if vol_rows else 1.0
        coalition_share = clamp(total_bilateral / total_vol)

        target_scores = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN
                c.trade_vulnerability_score  AS trade_vuln,
                c.economic_influence_score   AS econ_influence,
                c.global_risk_score          AS global_risk
        """, {"name": target})
        ts = target_scores[0] if target_scores else {}

        # Coalition amplifies impact — more severe than single-party sanctions
        trade_delta = clamp(coalition_share * 0.4 * mag)
        econ_delta  = -clamp(coalition_share * 0.35 * mag)
        risk_delta  = clamp(coalition_share * 0.3 * mag)

        affected = [AffectedCountry(
            country=target,
            impact_type="direct",
            severity=_severity(coalition_share * mag * 1.2),
            score_deltas=[
                _make_delta("trade_vulnerability_score", ts.get("trade_vuln"),   trade_delta),
                _make_delta("economic_influence_score",  ts.get("econ_influence"), econ_delta),
                _make_delta("global_risk_score",         ts.get("global_risk"),  risk_delta),
            ],
            exposure_usd=total_bilateral,
            summary=(
                f"Coalition of {len([c for c in coalition if c])} countries "
                f"blocks {coalition_share*100:.0f}% of {target}'s trade; "
                f"total exposure ~${total_bilateral/1e9:.1f}B."
            ),
        )]

        return ScenarioResult(
            scenario_type=ScenarioType.SANCTIONS_COALITION,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=affected,
            cascade_effects=[
                CascadeEffect(
                    mechanism="Multilateral trade exclusion",
                    affected=target,
                    severity=_severity(coalition_share * mag),
                    description=(
                        f"{target} effectively cut off from "
                        f"{coalition_share*100:.0f}% of global trade partners simultaneously."
                    ),
                )
            ],
            data_sources=["EXPORTS_TO", "HAS_TRADE_VOLUME_WITH", "trade_vulnerability_score"],
            confidence=0.80,
        )
    finally:
        conn.close()


# =========================================================
# HELPERS
# =========================================================

def _severity(score: float) -> str:
    if score >= 0.6:   return "critical"
    if score >= 0.4:   return "high"
    if score >= 0.2:   return "medium"
    return "low"


def _missing(values: list, names: list[str]) -> list[str]:
    return [names[i] for i, v in enumerate(values) if v is None]


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