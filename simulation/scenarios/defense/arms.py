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
# MILITARY INTERVENTION
# =========================================================

def run_military_intervention(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: actor intervenes militarily in target country's conflict.

    Logic:
    - Target's conflict_risk_score drops (intervention stabilizes or escalates)
    - Actor's defense_spending_score rises; military_strength_score improves
    - Actor's economic_influence may suffer (cost of intervention)
    - Alliance members of actor are implicitly committed
    - Regional stability decreases for neighboring countries

    Reads:
        conflict_risk_score, military_strength_score, defense_spending_score,
        MEMBER_OF, BELONGS_TO
    """
    actor  = req.actor
    target = req.target
    mag    = req.magnitude
    year   = req.year
    intervention_type = req.extra_params.get("type", "stabilization")

    if not actor or not target:
        return _empty_result(req, "military_intervention requires both actor (intervening) and target (country)")

    conn = Neo4jConnection()
    try:
        actor_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.military_strength_score AS military,
                   c.defense_spending_score AS defense_spend,
                   c.economic_influence_score AS econ_influence,
                   c.overall_vulnerability_score AS vulnerability
        """, {"name": actor})
        ar = actor_rows[0] if actor_rows else {}

        target_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.conflict_risk_score AS conflict_risk,
                   c.global_risk_score AS global_risk,
                   c.overall_vulnerability_score AS vulnerability,
                   c.economic_influence_score AS econ_influence
        """, {"name": target})
        tr = target_rows[0] if target_rows else {}

        # Actor: gains military projection but costs economic resources
        actor_military_delta  = clamp(0.08 * mag)
        actor_econ_delta      = -clamp(0.06 * mag)
        actor_vuln_delta      = clamp(0.05 * mag)   # blowback risk

        # Target: conflict_risk depends on intervention type
        if intervention_type == "stabilization":
            target_conflict_delta = -clamp(0.25 * mag)
            target_risk_delta     = -clamp(0.15 * mag)
            target_vuln_delta     = -clamp(0.10 * mag)
        else:
            # Enforcement/regime change — may escalate
            target_conflict_delta = clamp(0.15 * mag)
            target_risk_delta     = clamp(0.10 * mag)
            target_vuln_delta     = clamp(0.15 * mag)

        actor_affected = AffectedCountry(
            country=actor,
            impact_type="direct",
            severity=_severity(mag * 0.4),
            score_deltas=[
                _make_delta("military_strength_score",   ar.get("military"),      actor_military_delta),
                _make_delta("economic_influence_score",  ar.get("econ_influence"), actor_econ_delta),
                _make_delta("overall_vulnerability_score", ar.get("vulnerability"), actor_vuln_delta),
            ],
            summary=(
                f"{actor} deploys military forces to {target}; "
                f"projects power but incurs resource costs and blowback risk."
            ),
        )

        target_affected = AffectedCountry(
            country=target,
            impact_type="direct",
            severity=_severity(abs(target_conflict_delta) * mag),
            score_deltas=[
                _make_delta("conflict_risk_score",       tr.get("conflict_risk"),  target_conflict_delta),
                _make_delta("global_risk_score",         tr.get("global_risk"),    target_risk_delta),
                _make_delta("overall_vulnerability_score", tr.get("vulnerability"), target_vuln_delta),
            ],
            summary=(
                f"{actor}'s intervention {'reduces' if intervention_type == 'stabilization' else 'intensifies'} "
                f"conflict dynamics in {target}."
            ),
        )

        # Alliance members of actor face commitment pressure
        alliance_rows = _run(conn, """
            MATCH (a:Country {name: $actor})-[:MEMBER_OF]->(al:Alliance)<-[:MEMBER_OF]-(member:Country)
            WHERE member.name <> $actor
            RETURN member.name AS country,
                   member.military_strength_score AS military,
                   member.overall_vulnerability_score AS vulnerability
            LIMIT 8
        """, {"actor": actor})

        alliance_affected: list[AffectedCountry] = []
        for row in alliance_rows[:4]:
            country = row.get("country")
            if not country:
                continue
            alliance_affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity="low",
                score_deltas=[
                    _make_delta("overall_vulnerability_score", row.get("vulnerability"), clamp(0.03 * mag), 0.5),
                ],
                summary=(
                    f"{country} faces implicit commitment pressure as {actor}'s "
                    f"alliance partner in the intervention."
                ),
            ))

        # Regional neighbors
        region_rows = _run(conn, """
            MATCH (c:Country {name: $target})-[:BELONGS_TO]->(r:Region)<-[:BELONGS_TO]-(neighbor:Country)
            WHERE neighbor.name <> $target AND neighbor.name <> $actor
            RETURN neighbor.name AS country,
                   neighbor.global_risk_score AS global_risk
            LIMIT 5
        """, {"target": target})

        region_affected: list[AffectedCountry] = []
        for row in region_rows:
            country = row.get("country")
            if not country:
                continue
            region_affected.append(AffectedCountry(
                country=country,
                impact_type="regional",
                severity="low",
                score_deltas=[
                    _make_delta("global_risk_score", row.get("global_risk"), clamp(0.05 * mag), 0.45),
                ],
                summary=f"{country} faces regional spillover from military operations near its borders.",
            ))

        cascades = [
            CascadeEffect(
                mechanism=f"Military deployment — {intervention_type}",
                affected=target,
                severity=_severity(mag * 0.5),
                description=(
                    f"{actor} establishes military presence in {target}; "
                    f"{'stabilization forces reduce immediate violence' if intervention_type == 'stabilization' else 'enforcement operations heighten conflict intensity'}."
                ),
            ),
            CascadeEffect(
                mechanism="Alliance solidarity test",
                affected=f"{len(alliance_rows)} alliance members",
                severity="medium",
                description=(
                    f"Alliance partners face pressure to contribute to or support "
                    f"{actor}'s intervention, straining military resources."
                ),
            ),
        ]

        return ScenarioResult(
            scenario_type=ScenarioType.MILITARY_INTERVENTION,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=[actor_affected, target_affected] + alliance_affected + region_affected,
            cascade_effects=cascades,
            data_sources=["conflict_risk_score", "military_strength_score", "MEMBER_OF", "BELONGS_TO"],
            confidence=0.72,
        )
    finally:
        conn.close()


# =========================================================
# ARMS EMBARGO
# =========================================================

def run_arms_embargo(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: arms embargo imposed on target by actor (or coalition).

    Logic:
    - Target's military_strength_score degrades over time (no new weapons)
    - Target's overall_vulnerability_score rises (less capable of defense)
    - Actor's arms_export_score falls slightly (lost market)
    - If target has active conflict, conflict_risk may rise

    Reads:
        military_strength_score, arms_export_score, conflict_risk_score,
        EXPORTS_ARMS, IMPORTS_ARMS
    """
    actor  = req.actor
    target = req.target
    mag    = req.magnitude
    year   = req.year

    if not target:
        return _empty_result(req, "arms_embargo requires a target country")

    conn = Neo4jConnection()
    try:
        target_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.military_strength_score AS military,
                   c.conflict_risk_score AS conflict_risk,
                   c.overall_vulnerability_score AS vulnerability,
                   c.defense_spending_score AS defense_spend
        """, {"name": target})
        tr = target_rows[0] if target_rows else {}

        # Who currently exports arms to target?
        supplier_rows = _run(conn, """
            MATCH (supplier:Country)-[r:EXPORTS_ARMS]->(y:Year)
            WHERE supplier.name = $actor OR $actor IS NULL
            RETURN supplier.name AS country,
                   supplier.arms_export_score AS arms_export,
                   r.normalized_weight AS weight
            ORDER BY r.normalized_weight DESC
            LIMIT 5
        """, {"actor": actor})

        target_military_delta  = -clamp(0.15 * mag)
        target_vuln_delta      = clamp(0.20 * mag)
        current_conflict = float(tr.get("conflict_risk") or 0.0)
        # If already in conflict, embargo makes it more dangerous
        target_conflict_delta  = clamp(0.10 * mag) if current_conflict > 0.3 else 0.0

        target_affected = AffectedCountry(
            country=target,
            impact_type="direct",
            severity=_severity(target_vuln_delta),
            score_deltas=[
                _make_delta("military_strength_score",     tr.get("military"),    target_military_delta),
                _make_delta("overall_vulnerability_score", tr.get("vulnerability"), target_vuln_delta),
                _make_delta("conflict_risk_score",         tr.get("conflict_risk"), target_conflict_delta),
            ],
            summary=(
                f"{target} faces arms embargo; military modernization halts, "
                f"operational readiness degrades over 12-36 months."
            ),
        )

        # Exporting countries lose revenue
        supplier_affected: list[AffectedCountry] = []
        if actor:
            actor_rows = _run(conn, """
                MATCH (c:Country {name: $name})
                RETURN c.arms_export_score AS arms_export,
                       c.economic_influence_score AS econ_influence
            """, {"name": actor})
            ar = actor_rows[0] if actor_rows else {}
            supplier_affected.append(AffectedCountry(
                country=actor,
                impact_type="direct",
                severity="low",
                score_deltas=[
                    _make_delta("arms_export_score", ar.get("arms_export"), -clamp(0.04 * mag), 0.6),
                ],
                summary=f"{actor} loses arms export revenue from {target} embargo.",
            ))

        cascades = [
            CascadeEffect(
                mechanism="Military capability degradation",
                affected=target,
                severity=_severity(target_vuln_delta),
                description=(
                    f"{target}'s armed forces cannot procure replacement parts, "
                    f"ammunition, or new platforms; combat effectiveness falls over time."
                ),
            ),
        ]
        if current_conflict > 0.4:
            cascades.append(CascadeEffect(
                mechanism="Conflict prolongation",
                affected=target,
                severity="high",
                description=(
                    f"With active conflict and an arms embargo, {target} cannot "
                    f"sustain military operations, potentially forcing negotiations or collapse."
                ),
            ))

        return ScenarioResult(
            scenario_type=ScenarioType.ARMS_EMBARGO,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=[target_affected] + supplier_affected,
            cascade_effects=cascades,
            data_sources=["military_strength_score", "conflict_risk_score", "EXPORTS_ARMS"],
            confidence=0.75,
        )
    finally:
        conn.close()


# =========================================================
# DEFENSE SPENDING SURGE
# =========================================================

def run_defense_spending_surge(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: actor significantly increases defense budget.

    Logic:
    - Actor's military_strength_score rises
    - Actor's economic_influence may fall (crowding out civilian investment)
    - Regional neighbors perceive threat; may respond with their own spending
    - Actor's defense_burden_score rises

    Reads:
        military_strength_score, defense_spending_score, economic_influence_score,
        BELONGS_TO, HAS_GDP
    """
    actor = req.actor or req.target
    mag   = req.magnitude
    year  = req.year

    if not actor:
        return _empty_result(req, "defense_spending_surge requires an actor country")

    conn = Neo4jConnection()
    try:
        actor_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.military_strength_score AS military,
                   c.defense_spending_score AS defense_spend,
                   c.economic_influence_score AS econ_influence,
                   c.defense_burden_score AS burden
        """, {"name": actor})
        ar = actor_rows[0] if actor_rows else {}

        # Spending surge effects
        military_delta     = clamp(0.12 * mag)
        defense_spend_delta = clamp(0.20 * mag)
        econ_delta         = -clamp(0.06 * mag)   # opportunity cost
        burden_delta       = clamp(0.15 * mag)

        actor_affected = AffectedCountry(
            country=actor,
            impact_type="direct",
            severity="medium",
            score_deltas=[
                _make_delta("military_strength_score",  ar.get("military"),      military_delta),
                _make_delta("defense_spending_score",   ar.get("defense_spend"), defense_spend_delta),
                _make_delta("economic_influence_score", ar.get("econ_influence"), econ_delta),
                _make_delta("defense_burden_score",     ar.get("burden"),        burden_delta),
            ],
            summary=(
                f"{actor} surges defense budget; military capability improves "
                f"but civilian investment is crowded out."
            ),
        )

        # Regional response — neighbors may feel threatened
        region_rows = _run(conn, """
            MATCH (c:Country {name: $actor})-[:BELONGS_TO]->(r:Region)<-[:BELONGS_TO]-(neighbor:Country)
            WHERE neighbor.name <> $actor
            RETURN neighbor.name AS country,
                   neighbor.military_strength_score AS military,
                   neighbor.defense_spending_score AS defense_spend,
                   neighbor.global_risk_score AS global_risk
            LIMIT 6
        """, {"actor": actor})

        neighbor_affected: list[AffectedCountry] = []
        for row in region_rows:
            country = row.get("country")
            if not country:
                continue
            # Neighbors perceive threat — minor risk increase, may respond
            response_delta = clamp(0.04 * mag)
            neighbor_affected.append(AffectedCountry(
                country=country,
                impact_type="regional",
                severity="low",
                score_deltas=[
                    _make_delta("global_risk_score",      row.get("global_risk"),   response_delta, 0.5),
                    _make_delta("defense_spending_score", row.get("defense_spend"), clamp(0.03 * mag), 0.45),
                ],
                summary=(
                    f"{country} perceives {actor}'s spending surge as a threat signal; "
                    f"regional security competition may intensify."
                ),
            ))

        cascades = [
            CascadeEffect(
                mechanism="Regional arms dynamic",
                affected=f"{len(region_rows)} regional neighbors",
                severity="medium",
                description=(
                    f"{actor}'s spending surge may trigger a security dilemma; "
                    f"neighbors increase own defense budgets in response."
                ),
            ),
            CascadeEffect(
                mechanism="Fiscal pressure",
                affected=actor,
                severity="low",
                description=(
                    f"Higher defense burden diverts resources from education, "
                    f"infrastructure, and health — long-run growth impact."
                ),
            ),
        ]

        return ScenarioResult(
            scenario_type=ScenarioType.DEFENSE_SPENDING_SURGE,
            actor=actor,
            target=req.target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=[actor_affected] + neighbor_affected,
            cascade_effects=cascades,
            data_sources=["military_strength_score", "defense_spending_score", "BELONGS_TO"],
            confidence=0.78,
        )
    finally:
        conn.close()


# =========================================================
# BORDER CONFLICT
# =========================================================

def run_border_conflict(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: territorial dispute or border skirmish escalates between actor and target.

    Logic:
    - Both countries' conflict_risk_score rises
    - Both face economic disruption (trade/investment at border disrupted)
    - Regional neighbors on alert; may choose sides
    - Alliance triggers assessed

    Reads:
        conflict_risk_score, global_risk_score, trade_vulnerability_score,
        MEMBER_OF, HAS_TRADE_VOLUME_WITH, BELONGS_TO
    """
    actor  = req.actor
    target = req.target
    mag    = req.magnitude
    year   = req.year

    if not actor or not target:
        return _empty_result(req, "border_conflict requires both actor and target countries")

    conn = Neo4jConnection()
    try:
        actor_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.conflict_risk_score AS conflict_risk,
                   c.global_risk_score AS global_risk,
                   c.military_strength_score AS military,
                   c.trade_vulnerability_score AS trade_vuln
        """, {"name": actor})
        ar = actor_rows[0] if actor_rows else {}

        target_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.conflict_risk_score AS conflict_risk,
                   c.global_risk_score AS global_risk,
                   c.military_strength_score AS military,
                   c.trade_vulnerability_score AS trade_vuln
        """, {"name": target})
        tr = target_rows[0] if target_rows else {}

        # Bilateral trade disrupted
        trade_rows = _run(conn, """
            MATCH (a:Country {name: $actor})-[r:HAS_TRADE_VOLUME_WITH]-(t:Country {name: $target})
            WHERE r.year = $year
            RETURN r.value AS volume
        """, {"actor": actor, "target": target, "year": year})
        bilateral_volume = float(trade_rows[0].get("volume") or 0.0) if trade_rows else 0.0

        conflict_delta = clamp(0.25 * mag)
        risk_delta     = clamp(0.20 * mag)
        trade_delta    = clamp(0.12 * mag)

        actor_affected = AffectedCountry(
            country=actor,
            impact_type="direct",
            severity=_severity(conflict_delta),
            score_deltas=[
                _make_delta("conflict_risk_score",       ar.get("conflict_risk"),  conflict_delta),
                _make_delta("global_risk_score",         ar.get("global_risk"),    risk_delta),
                _make_delta("trade_vulnerability_score", ar.get("trade_vuln"),     trade_delta),
            ],
            exposure_usd=bilateral_volume,
            summary=(
                f"{actor} engages in border skirmish with {target}; "
                f"conflict_risk rises and bilateral trade is disrupted."
            ),
        )

        target_affected = AffectedCountry(
            country=target,
            impact_type="direct",
            severity=_severity(conflict_delta),
            score_deltas=[
                _make_delta("conflict_risk_score",       tr.get("conflict_risk"),  conflict_delta),
                _make_delta("global_risk_score",         tr.get("global_risk"),    risk_delta),
                _make_delta("trade_vulnerability_score", tr.get("trade_vuln"),     trade_delta),
            ],
            exposure_usd=bilateral_volume,
            summary=(
                f"{target} engages in border skirmish with {actor}; "
                f"military mobilization disrupts civilian economic activity."
            ),
        )

        # Alliance checks
        actor_alliances = _run(conn, """
            MATCH (a:Country {name: $actor})-[:MEMBER_OF]->(al:Alliance)
            RETURN al.name AS alliance
        """, {"actor": actor})
        target_alliances = _run(conn, """
            MATCH (t:Country {name: $target})-[:MEMBER_OF]->(al:Alliance)
            RETURN al.name AS alliance
        """, {"target": target})

        cascades = [
            CascadeEffect(
                mechanism="Military mobilization",
                affected=f"{actor} and {target}",
                severity=_severity(conflict_delta),
                description=(
                    f"Armed forces on both sides mobilize along disputed border; "
                    f"civilian displacement and infrastructure damage likely."
                ),
            ),
            CascadeEffect(
                mechanism="Trade route disruption",
                affected=f"Border regions and trade corridors",
                severity="medium",
                description=(
                    f"Border closure disrupts ~${bilateral_volume/1e9:.1f}B in bilateral trade "
                    f"and regional supply chains dependent on land crossings."
                ),
            ),
        ]

        if actor_alliances and target_alliances:
            cascades.append(CascadeEffect(
                mechanism="Alliance entanglement risk",
                affected=f"Alliance partners of {actor} and {target}",
                severity="high",
                description=(
                    f"Both parties have alliance commitments; "
                    f"escalation risks drawing in external powers."
                ),
            ))

        return ScenarioResult(
            scenario_type=ScenarioType.BORDER_CONFLICT,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=[actor_affected, target_affected],
            cascade_effects=cascades,
            data_sources=[
                "conflict_risk_score", "global_risk_score", "HAS_TRADE_VOLUME_WITH", "MEMBER_OF"
            ],
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