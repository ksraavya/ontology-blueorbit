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
    Two modes, both computed from SIPRI arms-transfer data (TRANSFERS_ARMS_TO,
    TIV delivered over the last 10 years of the register):

    1. Supplier cut-off (actor only), e.g. "What if Russia stops its arms exports?"
       Every recipient of the actor's arms is affected in proportion to its
       DEPENDENCY: the share of its major-arms imports that came from the actor.
    2. Embargo on a country (target, optional actor), e.g. "US arms embargo on
       Saudi Arabia". The target's hit scales with the share of its imports
       supplied by the embargoing side (all suppliers if no actor is named).

    Score deltas = dependency x coefficient x magnitude (coefficients documented
    in-line), so a country that buys 80% of its arms from the supplier moves
    far more than one that buys 5%; the old version applied a flat -0.15.

    Reads: TRANSFERS_ARMS_TO, military_strength_score, overall_vulnerability_score,
           conflict_risk_score, arms_export_score
    """
    actor = req.actor
    target = req.target
    mag = req.magnitude
    K_MIL, K_VULN, K_CONFLICT = 0.30, 0.25, 0.10   # per unit of dependency

    if not actor and not target:
        return _empty_result(req, "arms_embargo needs a supplier (actor) or an embargoed country (target)")

    conn = Neo4jConnection()
    try:
        win = _run(conn, "MATCH ()-[r:TRANSFERS_ARMS_TO]->() RETURN max(r.year) AS hi")
        hi = (win[0].get("hi") if win else None)
        if hi is None:
            return _empty_result(req, "No SIPRI arms-transfer data in the graph (load data/raw/sipri_trade_register.csv)")
        lo = int(hi) - 9
        params = {"lo": lo, "hi": int(hi)}

        affected: list[AffectedCountry] = []
        cascades: list[CascadeEffect] = []

        if actor and not target:
            # ── Mode 1: supplier cut-off ──────────────────────────────────
            rows = _run(conn, """
                MATCH (s {name: $actor})-[r:TRANSFERS_ARMS_TO]->(c:Country)
                WHERE r.year >= $lo AND r.year <= $hi
                WITH c, sum(r.tiv) AS from_actor, collect(r.weapons) AS w
                MATCH (any)-[t:TRANSFERS_ARMS_TO]->(c) WHERE t.year >= $lo AND t.year <= $hi
                WITH c, from_actor, w, sum(t.tiv) AS total
                RETURN c.name AS country, from_actor, total,
                       from_actor / total AS dependency,
                       c.military_strength_score AS military,
                       c.overall_vulnerability_score AS vulnerability,
                       c.conflict_risk_score AS conflict
                ORDER BY from_actor DESC LIMIT 15
            """, {**params, "actor": actor})
            weapons = {r["country"]: r for r in _run(conn, """
                MATCH (s {name: $actor})-[r:TRANSFERS_ARMS_TO]->(c:Country)
                WHERE r.year >= $lo AND r.year <= $hi
                WITH c, r ORDER BY r.tiv DESC
                RETURN c.name AS country, collect(r.weapons)[0] AS top
            """, {**params, "actor": actor})}
            if not rows:
                return _empty_result(req, f"{actor} has no recorded arms deliveries in SIPRI data for {lo}-{hi}")

            world = _run(conn, "MATCH ()-[t:TRANSFERS_ARMS_TO]->() WHERE t.year >= $lo AND t.year <= $hi RETURN sum(t.tiv) AS w", params)
            actor_total = sum(float(r["from_actor"] or 0) for r in rows)
            world_total = float((world[0].get("w") if world else 0) or 0) or 1.0

            for r in rows:
                dep = float(r["dependency"] or 0)
                conflict = float(r.get("conflict") or 0)
                top = (weapons.get(r["country"], {}) or {}).get("top") or []
                affected.append(AffectedCountry(
                    country=r["country"],
                    impact_type="direct" if dep >= 0.3 else "cascade",
                    severity=_severity(dep * mag),
                    score_deltas=[
                        _make_delta("military_strength_score", r.get("military"), -clamp(dep * K_MIL * mag)),
                        _make_delta("overall_vulnerability_score", r.get("vulnerability"), clamp(dep * K_VULN * mag)),
                        _make_delta("conflict_risk_score", r.get("conflict"),
                                    clamp(dep * K_CONFLICT * mag) if conflict > 0.3 else 0.0, 0.6),
                    ],
                    summary=(
                        f"{dep * 100:.0f}% of {r['country']}'s major-arms imports came from {actor} "
                        f"({lo}-{hi}: {float(r['from_actor']):,.0f} of {float(r['total']):,.0f} SIPRI TIV)"
                        + (f"; e.g. {', '.join(top[:2])}." if top else ".")
                    ),
                ))
            share = actor_total / world_total
            ar = _run(conn, "MATCH (c {name: $n}) RETURN c.arms_export_score AS ax, c.economic_influence_score AS ei", {"n": actor})
            ar = ar[0] if ar else {}
            affected.append(AffectedCountry(
                country=actor,
                impact_type="direct",
                severity=_severity(share * 2 * mag),
                score_deltas=[
                    _make_delta("arms_export_score", ar.get("ax"), -clamp(share * mag), 0.7),
                    _make_delta("economic_influence_score", ar.get("ei"), -clamp(share * 0.3 * mag), 0.5),
                ],
                summary=(f"{actor} gives up {share * 100:.1f}% of world major-arms deliveries "
                         f"({actor_total:,.0f} TIV, {lo}-{hi}) and the leverage that comes with them."),
            ))
            dependent = [a for a in affected[:-1] if a.impact_type == "direct"]
            cascades.append(CascadeEffect(
                mechanism="Spare parts and maintenance gap",
                affected=", ".join(a.country for a in dependent[:6]) or "major recipients",
                severity=_severity(max((float(r["dependency"] or 0) for r in rows), default=0) * mag),
                description=(f"{len(dependent)} recipients rely on {actor} for 30%+ of their major arms; "
                             f"fleets of {actor}-made aircraft, tanks and air defence lose spares and upgrades."),
            ))
            cascades.append(CascadeEffect(
                mechanism="Supplier substitution",
                affected="Alternative exporters",
                severity="medium",
                description="Recipients turn to other suppliers; multi-year procurement and retraining lag.",
            ))
            sources = ["TRANSFERS_ARMS_TO (SIPRI)", "military_strength_score", "overall_vulnerability_score", "arms_export_score"]

        else:
            # ── Mode 2: embargo on a target ───────────────────────────────
            sup = _run(conn, """
                MATCH (s)-[r:TRANSFERS_ARMS_TO]->(c:Country {name: $target})
                WHERE r.year >= $lo AND r.year <= $hi
                RETURN s.name AS supplier, sum(r.tiv) AS tiv, s.arms_export_score AS ax
                ORDER BY tiv DESC
            """, {**params, "target": target})
            total = sum(float(r["tiv"] or 0) for r in sup)
            if actor:
                cut = sum(float(r["tiv"] or 0) for r in sup if r["supplier"] == actor)
                dep = cut / total if total else 0.0
                embargoers = [r for r in sup if r["supplier"] == actor]
            else:
                dep = 1.0 if total else 0.0
                embargoers = sup[:6]
            tr = _run(conn, """MATCH (c:Country {name: $n}) RETURN c.military_strength_score AS m,
                               c.overall_vulnerability_score AS v, c.conflict_risk_score AS cr""", {"n": target})
            tr = tr[0] if tr else {}
            conflict = float(tr.get("cr") or 0)
            affected.append(AffectedCountry(
                country=target,
                impact_type="direct",
                severity=_severity(dep * mag),
                score_deltas=[
                    _make_delta("military_strength_score", tr.get("m"), -clamp(dep * K_MIL * mag)),
                    _make_delta("overall_vulnerability_score", tr.get("v"), clamp(dep * K_VULN * mag)),
                    _make_delta("conflict_risk_score", tr.get("cr"), clamp(dep * K_CONFLICT * mag) if conflict > 0.3 else 0.0, 0.6),
                ],
                summary=(f"The embargo cuts off {dep * 100:.0f}% of {target}'s major-arms imports "
                         f"({lo}-{hi}, {total:,.0f} TIV in total)."
                         if total else f"{target} has no recorded major-arms imports in {lo}-{hi}; little direct effect."),
            ))
            for r in embargoers:
                s_share = float(r["tiv"] or 0) / total if total else 0
                affected.append(AffectedCountry(
                    country=r["supplier"],
                    impact_type="direct",
                    severity="low",
                    score_deltas=[_make_delta("arms_export_score", r.get("ax"), -clamp(s_share * 0.1 * mag), 0.6)],
                    summary=f"{r['supplier']} loses a market worth {float(r['tiv'] or 0):,.0f} TIV ({s_share * 100:.0f}% of {target}'s imports).",
                ))
            cascades.append(CascadeEffect(
                mechanism="Military capability degradation",
                affected=target,
                severity=_severity(dep * mag),
                description=f"{target} cannot replace platforms, spares or munitions from embargoing suppliers.",
            ))
            sources = ["TRANSFERS_ARMS_TO (SIPRI)", "military_strength_score", "conflict_risk_score"]

        return ScenarioResult(
            scenario_type=ScenarioType.ARMS_EMBARGO,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=req.year,
            headline="",
            summary="",
            affected_countries=sorted(affected, key=lambda a: sum(abs(d.delta) for d in a.score_deltas), reverse=True),
            cascade_effects=cascades,
            data_sources=sources,
            confidence=0.8,
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