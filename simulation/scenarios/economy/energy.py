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
    if score >= 0.6: return "critical"
    if score >= 0.4: return "high"
    if score >= 0.2: return "medium"
    return "low"


# =========================================================
# ENERGY CUTOFF
# =========================================================

def run_energy_cutoff(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: energy supplier (actor) cuts exports to target (and region).

    Logic:
    - Find all countries importing energy from actor
    - For each, compute dependency on actor
    - Countries with high dependency face critical energy vulnerability rise
    - Target (if specified) is treated as primary affected party
    - GDP exposure estimated from HAS_GDP and energy import value

    Reads:
        IMPORTS_ENERGY_FROM, EXPORTS_ENERGY_TO, energy_vulnerability_score,
        HAS_GDP, economic_power_score
    """
    actor  = req.actor
    target = req.target
    mag    = req.magnitude
    year   = req.year

    if not actor:
        return _empty_result(req, "energy_cutoff requires an actor (the energy supplier)")

    conn = Neo4jConnection()
    try:
        # All importers from this actor
        importer_rows = _run(conn, """
            MATCH (importer:Country)-[r:IMPORTS_ENERGY_FROM]->(supplier:Country {name: $actor})
            WHERE r.year = $year
            RETURN
                importer.name                       AS country,
                r.dependency                        AS dependency,
                r.value                             AS import_value_usd,
                importer.energy_vulnerability_score AS energy_vuln,
                importer.trade_vulnerability_score  AS trade_vuln,
                importer.economic_power_score       AS econ_power,
                importer.global_risk_score          AS global_risk
            ORDER BY r.dependency DESC
        """, {"actor": actor, "year": year})

        if not importer_rows:
            return _empty_result(req, f"No energy import relationships found for supplier {actor} in {year}")

        affected: list[AffectedCountry] = []
        total_exposure = 0.0

        for row in importer_rows:
            country    = row.get("country")
            dep        = float(row.get("dependency") or 0.0)
            imp_val    = float(row.get("import_value_usd") or 0.0)

            if not country:
                continue

            # Only include target if specified, or top dependents
            if target and country != target and dep < 0.15:
                continue

            total_exposure += imp_val

            # Energy vulnerability rises proportional to dependency × magnitude
            energy_delta  = clamp(dep * 0.4 * mag)
            # GDP shock: rough proxy — higher energy dependency → larger GDP hit
            trade_delta   = clamp(dep * 0.2 * mag)
            risk_delta    = clamp(dep * 0.25 * mag)

            is_primary = (target and country == target)

            affected.append(AffectedCountry(
                country=country,
                impact_type="direct" if is_primary else "cascade",
                severity=_severity(dep * mag),
                score_deltas=[
                    _make_delta("energy_vulnerability_score", row.get("energy_vuln"), energy_delta),
                    _make_delta("trade_vulnerability_score",  row.get("trade_vuln"),  trade_delta),
                    _make_delta("global_risk_score",          row.get("global_risk"), risk_delta),
                ],
                exposure_usd=imp_val,
                summary=(
                    f"{country} imports {dep*100:.0f}% of energy from {actor}; "
                    f"~${imp_val/1e9:.1f}B energy supply at risk."
                ),
            ))

        # Actor's own scores — loses energy export leverage
        actor_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN
                c.economic_influence_score   AS econ_influence,
                c.strategic_influence_score  AS strat_influence
        """, {"name": actor})
        ar = actor_rows[0] if actor_rows else {}

        affected.append(AffectedCountry(
            country=actor,
            impact_type="direct",
            severity="medium",
            score_deltas=[
                _make_delta("economic_influence_score", ar.get("econ_influence"), -0.05 * mag, 0.6),
            ],
            summary=(
                f"{actor} loses energy export revenue; "
                f"long-term leverage over importers diminishes."
            ),
        ))

        # Alternative supplier coverage
        alt_rows = _run(conn, """
            MATCH (supplier:Country)-[r:EXPORTS_ENERGY_TO]->(dummy)
            WHERE supplier.name <> $actor AND r.year = $year
            RETURN supplier.name AS supplier, sum(r.value) AS total_export
            ORDER BY total_export DESC
            LIMIT 5
        """, {"actor": actor, "year": year})

        alt_coverage = ", ".join(r["supplier"] for r in alt_rows[:3]) if alt_rows else "limited"

        cascades: list[CascadeEffect] = [
            CascadeEffect(
                mechanism="Energy supply gap",
                affected=f"{len([a for a in affected if a.impact_type == 'direct'])} direct importers",
                severity=_severity(mag * 0.6),
                description=(
                    f"{actor} supplies energy to {len(importer_rows)} countries; "
                    f"total exposure ~${total_exposure/1e9:.1f}B. "
                    f"Alternative suppliers: {alt_coverage}."
                ),
            ),
            CascadeEffect(
                mechanism="Industrial output contraction",
                affected="Energy-intensive industries in affected countries",
                severity="high",
                description=(
                    "Manufacturing, heating, and transport costs spike; "
                    "industrial output contracts in high-dependency importers."
                ),
            ),
        ]

        if mag >= 1.5:
            cascades.append(CascadeEffect(
                mechanism="Political instability",
                affected="Governments in high-dependency import countries",
                severity="high",
                description=(
                    "Severe energy shortfall creates domestic political pressure "
                    "on governments unable to secure alternative supply."
                ),
            ))

        return ScenarioResult(
            scenario_type=ScenarioType.ENERGY_CUTOFF,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=sorted(affected, key=lambda x: abs(sum(d.delta for d in x.score_deltas)), reverse=True),
            cascade_effects=cascades,
            data_sources=[
                "IMPORTS_ENERGY_FROM", "EXPORTS_ENERGY_TO",
                "energy_vulnerability_score", "economic_power_score",
            ],
            confidence=0.85 if importer_rows else 0.4,
        )
    finally:
        conn.close()


# =========================================================
# ENERGY PRICE SHOCK
# =========================================================

def run_energy_price_shock(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: global energy price spike or crash.
    magnitude > 1.0 = price spike, magnitude < 1.0 treated as 0.5 = price crash.

    Logic:
    - Countries with high energy_vulnerability_score are most exposed to spikes
    - Energy exporters benefit from spikes (economic_influence rises)
    - All countries face trade_vulnerability increase in a spike
    """
    mag  = req.magnitude
    year = req.year
    is_spike = mag >= 1.0

    conn = Neo4jConnection()
    try:
        # Major energy exporters
        exporter_rows = _run(conn, """
            MATCH (c:Country)-[r:EXPORTS_ENERGY_TO]->(:Country)
            WHERE r.year = $year
            WITH c, sum(r.value) AS total_exports
            WHERE total_exports > 0
            RETURN
                c.name                          AS country,
                total_exports                   AS export_value,
                c.economic_influence_score      AS econ_influence,
                c.economic_power_score          AS econ_power
            ORDER BY total_exports DESC
            LIMIT 10
        """, {"year": year})

        # High energy importers
        importer_rows = _run(conn, """
            MATCH (c:Country)
            WHERE c.energy_vulnerability_score IS NOT NULL
              AND c.energy_vulnerability_score > 0.3
            RETURN
                c.name                          AS country,
                c.energy_vulnerability_score    AS energy_vuln,
                c.trade_vulnerability_score     AS trade_vuln,
                c.global_risk_score             AS global_risk
            ORDER BY c.energy_vulnerability_score DESC
            LIMIT 15
        """)

        affected: list[AffectedCountry] = []

        # Exporters gain/lose from price change
        for row in exporter_rows:
            delta = clamp(0.08 * mag) if is_spike else -0.05
            affected.append(AffectedCountry(
                country=row["country"],
                impact_type="direct",
                severity="medium" if is_spike else "low",
                score_deltas=[
                    _make_delta("economic_influence_score", row.get("econ_influence"),
                                delta if is_spike else -delta, 0.7),
                ],
                exposure_usd=row.get("export_value"),
                summary=(
                    f"{row['country']} {'benefits from' if is_spike else 'hurt by'} "
                    f"price {'spike' if is_spike else 'crash'}; "
                    f"export revenues {'rise' if is_spike else 'fall'}."
                ),
            ))

        # Importers suffer from spike, benefit from crash
        for row in importer_rows:
            vuln = float(row.get("energy_vuln") or 0.0)
            energy_delta = clamp(vuln * 0.2 * mag) if is_spike else -clamp(vuln * 0.1)
            risk_delta   = clamp(vuln * 0.15 * mag) if is_spike else 0.0
            affected.append(AffectedCountry(
                country=row["country"],
                impact_type="cascade",
                severity=_severity(vuln * mag * 0.5),
                score_deltas=[
                    _make_delta("energy_vulnerability_score", row.get("energy_vuln"), energy_delta),
                    _make_delta("global_risk_score",          row.get("global_risk"), risk_delta),
                ],
                summary=(
                    f"{row['country']} (energy_vulnerability={vuln:.2f}) faces "
                    f"{'higher import costs' if is_spike else 'lower import costs'}."
                ),
            ))

        cascades = [
            CascadeEffect(
                mechanism=f"Energy price {'spike' if is_spike else 'crash'}",
                affected="All energy-dependent economies",
                severity=_severity(mag * 0.5),
                description=(
                    f"{'Rising' if is_spike else 'Falling'} energy prices "
                    f"{'strain import budgets and fuel inflation' if is_spike else 'improve trade balances but hurt exporters'}."
                ),
            )
        ]

        return ScenarioResult(
            scenario_type=ScenarioType.ENERGY_PRICE_SHOCK,
            actor=req.actor,
            target=req.target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=affected,
            cascade_effects=cascades,
            data_sources=["EXPORTS_ENERGY_TO", "energy_vulnerability_score"],
            confidence=0.75,
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

# =========================================================
# ENERGY DIVERSIFICATION
# =========================================================
 
def run_energy_diversification(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: a country shifts away from a dominant energy supplier.
    Actor = diversifying country. Target = supplier being reduced.
 
    Logic:
    - Actor's energy_vulnerability_score decreases (less concentrated dependency)
    - Target (dominant supplier) loses revenue and geopolitical leverage
    - New suppliers (third_parties) gain market share
    - Transition period creates short-term vulnerability (infrastructure investment)
 
    Reads:
        IMPORTS_ENERGY_FROM, energy_vulnerability_score,
        economic_influence_score, EXPORTS_ENERGY_TO
    """
    actor  = req.actor
    target = req.target   # supplier being reduced
    mag    = req.magnitude
    year   = req.year
    new_suppliers = req.third_parties
 
    if not actor:
        return ScenarioResult(
            scenario_type=ScenarioType.ENERGY_DIVERSIFICATION,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="Insufficient data to run simulation",
            summary="energy_diversification requires an actor country",
            confidence=0.0,
            missing_data=["energy_diversification requires an actor country"],
        )
 
    conn = Neo4jConnection()
    try:
        actor_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.energy_vulnerability_score AS energy_vuln,
                   c.trade_vulnerability_score AS trade_vuln,
                   c.economic_influence_score AS econ_influence
        """, {"name": actor})
        ar = actor_rows[0] if actor_rows else {}
 
        # Current dependency on target supplier
        if target:
            dep_rows = _run(conn, """
                MATCH (a:Country {name: $actor})-[r:IMPORTS_ENERGY_FROM]->(s:Country {name: $target})
                WHERE r.year = $year
                RETURN r.dependency AS dependency, r.value AS import_value
            """, {"actor": actor, "target": target, "year": year})
            current_dep = float(dep_rows[0].get("dependency") or 0.0) if dep_rows else 0.3
            import_value = float(dep_rows[0].get("import_value") or 0.0) if dep_rows else 0.0
        else:
            # Find the most dominant supplier
            dominant_rows = _run(conn, """
                MATCH (a:Country {name: $actor})-[r:IMPORTS_ENERGY_FROM]->(s:Country)
                WHERE r.year = $year
                RETURN s.name AS supplier, r.dependency AS dependency, r.value AS import_value
                ORDER BY r.dependency DESC LIMIT 1
            """, {"actor": actor, "year": year})
            if dominant_rows:
                target = dominant_rows[0].get("supplier")
                current_dep  = float(dominant_rows[0].get("dependency") or 0.0)
                import_value = float(dominant_rows[0].get("import_value") or 0.0)
            else:
                current_dep  = 0.3
                import_value = 0.0
 
        # Actor benefits from diversification
        # Energy vulnerability falls proportional to the dependency being reduced
        energy_vuln_delta = -clamp(current_dep * 0.30 * mag)
        # Short-term cost: new infrastructure
        trade_vuln_delta  = clamp(0.05 * mag)
 
        actor_affected = AffectedCountry(
            country=actor,
            impact_type="direct",
            severity="medium",
            score_deltas=[
                _make_delta("energy_vulnerability_score", ar.get("energy_vuln"), energy_vuln_delta),
                _make_delta("trade_vulnerability_score",  ar.get("trade_vuln"),  trade_vuln_delta),
            ],
            summary=(
                f"{actor} reduces energy dependence on {target or 'dominant supplier'} "
                f"from {current_dep*100:.0f}% to lower levels; "
                f"vulnerability falls but transition requires investment."
            ),
        )
 
        affected: list[AffectedCountry] = [actor_affected]
 
        # Dominant supplier (target) loses leverage and revenue
        if target:
            supplier_rows = _run(conn, """
                MATCH (c:Country {name: $name})
                RETURN c.economic_influence_score AS econ_influence,
                       c.economic_power_score AS econ_power
            """, {"name": target})
            sr = supplier_rows[0] if supplier_rows else {}
 
            revenue_loss_delta = -clamp(current_dep * 0.20 * mag)
            affected.append(AffectedCountry(
                country=target,
                impact_type="direct",
                severity=_severity(abs(revenue_loss_delta)),
                score_deltas=[
                    _make_delta("economic_influence_score", sr.get("econ_influence"), revenue_loss_delta, 0.65),
                    _make_delta("economic_power_score",     sr.get("econ_power"),     revenue_loss_delta * 0.5, 0.60),
                ],
                exposure_usd=import_value * current_dep,
                summary=(
                    f"{target} loses energy market share as {actor} diversifies; "
                    f"~${import_value*current_dep/1e9:.1f}B in annual revenue at risk."
                ),
            ))
 
        # New suppliers benefit
        for new_supplier in (new_suppliers or [])[:3]:
            if not new_supplier:
                continue
            new_sup_rows = _run(conn, """
                MATCH (c:Country {name: $name})
                RETURN c.economic_influence_score AS econ_influence
            """, {"name": new_supplier})
            ns = new_sup_rows[0] if new_sup_rows else {}
            gain = clamp(current_dep * 0.10 * mag)
            affected.append(AffectedCountry(
                country=new_supplier,
                impact_type="cascade",
                severity="low",
                score_deltas=[
                    _make_delta("economic_influence_score", ns.get("econ_influence"), gain, 0.55),
                ],
                summary=f"{new_supplier} gains energy export market share as {actor} diversifies.",
            ))
 
        cascades: list[CascadeEffect] = [
            CascadeEffect(
                mechanism="Dependency reduction",
                affected=actor,
                severity="medium",
                description=(
                    f"{actor}'s reduced reliance on {target or 'single supplier'} "
                    f"diminishes that country's geopolitical leverage in bilateral negotiations."
                ),
            ),
            CascadeEffect(
                mechanism="Infrastructure investment",
                affected=actor,
                severity="low",
                description=(
                    "New terminals, pipelines, and grid connections required; "
                    "transition period of 3-7 years before full diversification benefit realized."
                ),
            ),
        ]
 
        return ScenarioResult(
            scenario_type=ScenarioType.ENERGY_DIVERSIFICATION,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=affected,
            cascade_effects=cascades,
            data_sources=["IMPORTS_ENERGY_FROM", "energy_vulnerability_score", "economic_influence_score"],
            confidence=0.78 if import_value > 0 else 0.55,
        )
    finally:
        conn.close()