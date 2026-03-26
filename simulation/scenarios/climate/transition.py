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
# ENERGY TRANSITION
# =========================================================

def run_energy_transition(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: major economy rapidly shifts energy mix (e.g. accelerated renewable transition).
    Actor = transitioning country.

    Logic:
    - Actor's energy_vulnerability_score decreases (less dependent on fossil fuel imports)
    - Actor's climate_vulnerability decreases (lower emissions)
    - Fossil fuel exporters that supply actor face demand loss (revenue impact)
    - If actor is a major fossil fuel importer: energy exporters face material revenue loss
    - Renewable technology exporters gain (if specified in third_parties)

    extra_params:
        from_source: "coal" | "gas" | "oil" | "fossil" (default: "fossil")
        to_source: "renewables" | "nuclear" | "mixed" (default: "renewables")

    Reads:
        energy_vulnerability_score, IMPORTS_ENERGY_FROM, EXPORTS_ENERGY_TO,
        climate_vulnerability_score, economic_influence_score
    """
    actor       = req.actor or req.target
    mag         = req.magnitude
    year        = req.year
    from_source = req.extra_params.get("from_source", "fossil")
    to_source   = req.extra_params.get("to_source", "renewables")

    if not actor:
        return _empty_result(req, "energy_transition requires an actor country")

    conn = Neo4jConnection()
    try:
        actor_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.energy_vulnerability_score AS energy_vuln,
                   c.climate_vulnerability_score AS climate_vuln,
                   c.economic_influence_score AS econ_influence,
                   c.economic_power_score AS econ_power,
                   c.trade_vulnerability_score AS trade_vuln
        """, {"name": actor})
        ar = actor_rows[0] if actor_rows else {}

        # How much does actor import from fossil fuel suppliers?
        energy_import_rows = _run(conn, """
            MATCH (c:Country {name: $actor})-[r:IMPORTS_ENERGY_FROM]->(supplier:Country)
            WHERE r.year = $year
            RETURN supplier.name AS supplier,
                   r.dependency AS dependency,
                   r.value AS import_value,
                   supplier.economic_influence_score AS supplier_influence
            ORDER BY r.dependency DESC
            LIMIT 8
        """, {"actor": actor, "year": year})

        total_energy_import = sum(float(r.get("import_value") or 0.0) for r in energy_import_rows)

        # Actor benefits: reduced energy vulnerability, improved climate scores
        energy_vuln_delta  = -clamp(0.20 * mag)
        climate_vuln_delta = -clamp(0.15 * mag)
        # Short-term cost: transition investment may reduce other economic metrics
        econ_power_delta   = -clamp(0.04 * mag)  # transition investment cost (temporary)

        actor_affected = AffectedCountry(
            country=actor,
            impact_type="direct",
            severity="medium",
            score_deltas=[
                _make_delta("energy_vulnerability_score",  ar.get("energy_vuln"),   energy_vuln_delta),
                _make_delta("climate_vulnerability_score", ar.get("climate_vuln"),  climate_vuln_delta),
                _make_delta("economic_power_score",        ar.get("econ_power"),    econ_power_delta),
            ],
            exposure_usd=total_energy_import,
            summary=(
                f"{actor} accelerates transition from {from_source} to {to_source}; "
                f"energy import dependency falls, reducing geopolitical exposure "
                f"to fossil fuel supply disruptions."
            ),
        )

        affected: list[AffectedCountry] = [actor_affected]

        # Energy exporters (current suppliers) face demand loss
        for row in energy_import_rows[:6]:
            supplier = row.get("supplier")
            if not supplier:
                continue
            dep          = float(row.get("dependency") or 0.0)
            import_val   = float(row.get("import_value") or 0.0)
            # Revenue loss proportional to actor's dependency on this supplier
            revenue_loss_delta = -clamp(dep * 0.15 * mag)

            # Get supplier's scores
            supplier_rows = _run(conn, """
                MATCH (c:Country {name: $name})
                RETURN c.economic_influence_score AS econ_influence,
                       c.economic_power_score AS econ_power
            """, {"name": supplier})
            sr = supplier_rows[0] if supplier_rows else {}

            affected.append(AffectedCountry(
                country=supplier,
                impact_type="cascade",
                severity=_severity(abs(revenue_loss_delta)),
                score_deltas=[
                    _make_delta("economic_influence_score", sr.get("econ_influence"), revenue_loss_delta, 0.65),
                    _make_delta("economic_power_score",     sr.get("econ_power"),     revenue_loss_delta * 0.5, 0.60),
                ],
                exposure_usd=import_val * dep,
                summary=(
                    f"{supplier} loses {dep*100:.0f}% of energy demand from {actor}; "
                    f"~${import_val*dep/1e9:.1f}B in annual export revenue at risk."
                ),
            ))

        # Countries already highly energy-vulnerable may benefit (lower global prices)
        beneficiary_rows = _run(conn, """
            MATCH (c:Country)
            WHERE c.energy_vulnerability_score >= 0.5
              AND c.name <> $actor
            RETURN c.name AS country,
                   c.energy_vulnerability_score AS energy_vuln
            ORDER BY c.energy_vulnerability_score DESC
            LIMIT 5
        """, {"actor": actor})

        for row in beneficiary_rows[:3]:
            country = row.get("country")
            if not country:
                continue
            small_benefit = clamp(0.03 * mag)
            affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity="low",
                score_deltas=[
                    _make_delta("energy_vulnerability_score", row.get("energy_vuln"), -small_benefit, 0.45),
                ],
                summary=(
                    f"{country} may benefit marginally from lower fossil fuel demand "
                    f"as {actor}'s transition reduces global price pressure."
                ),
            ))

        cascades = [
            CascadeEffect(
                mechanism=f"{from_source.title()} demand reduction",
                affected=f"Fossil fuel exporters supplying {actor}",
                severity=_severity(0.15 * mag),
                description=(
                    f"{actor}'s accelerated transition reduces demand for {from_source} imports; "
                    f"exporters must find alternative buyers or diversify revenue."
                ),
            ),
            CascadeEffect(
                mechanism="Technology spillover",
                affected="Global clean energy markets",
                severity="medium",
                description=(
                    f"Rapid {actor} transition accelerates renewable technology deployment; "
                    f"cost curves fall, benefiting other potential adopters."
                ),
            ),
            CascadeEffect(
                mechanism="Stranded asset risk",
                affected="Fossil fuel infrastructure owners",
                severity="medium" if mag >= 1.3 else "low",
                description=(
                    f"Accelerated transition signals long-run decline in fossil fuel value; "
                    f"stranded asset risk rises for producers and infrastructure owners."
                ),
            ),
        ]

        return ScenarioResult(
            scenario_type=ScenarioType.ENERGY_TRANSITION,
            actor=actor,
            target=req.target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=affected,
            cascade_effects=cascades,
            data_sources=[
                "energy_vulnerability_score", "IMPORTS_ENERGY_FROM",
                "climate_vulnerability_score", "economic_influence_score",
            ],
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