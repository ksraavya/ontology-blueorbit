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
# CLIMATE DISASTER
# =========================================================

def run_climate_disaster(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: major natural disaster (flood, drought, earthquake, cyclone) hits a country.

    Logic:
    - Target's economic_power_score falls (GDP shock from damage)
    - Target's climate_vulnerability_score rises
    - Target's overall_vulnerability_score spikes
    - Supply chain disruption propagates to trade partners
    - If disaster hits a major energy exporter: energy price shock cascades
    - Reconstruction demand creates economic opportunity (delayed effect)

    extra_params:
        disaster_type: "earthquake" | "flood" | "drought" | "cyclone" | "wildfire"
        damage_usd: estimated damage in USD (optional, for magnitude calibration)

    Reads:
        climate_risk_score, climate_vulnerability_score, economic_power_score,
        supply_chain_risk_score, DISRUPTS_SUPPLY_CHAIN, EXPORTS_ENERGY_TO,
        HAS_TRADE_DEPENDENCY_ON
    """
    target       = req.target or req.actor
    mag          = req.magnitude
    year         = req.year
    disaster_type = req.extra_params.get("disaster_type", "natural disaster")
    damage_usd   = req.extra_params.get("damage_usd")

    if not target:
        return _empty_result(req, "climate_disaster requires a target country")

    conn = Neo4jConnection()
    try:
        target_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.climate_risk_score AS climate_risk,
                   c.climate_vulnerability_score AS climate_vuln,
                   c.economic_power_score AS econ_power,
                   c.global_risk_score AS global_risk,
                   c.overall_vulnerability_score AS vulnerability,
                   c.supply_chain_risk_score AS supply_chain_risk,
                   c.disaster_frequency_score AS disaster_freq
        """, {"name": target})
        tr = target_rows[0] if target_rows else {}

        # High existing climate_risk means country is more exposed
        existing_climate_risk = float(tr.get("climate_risk") or 0.3)
        baseline_exposure = clamp(existing_climate_risk * 0.5 + 0.3)

        # Disaster-type specific modifiers
        disaster_lower = str(disaster_type).lower()
        if "earthquake" in disaster_lower:
            econ_mult = 1.4
            supply_mult = 1.5
            disaster_label = "earthquake"
        elif "flood" in disaster_lower:
            econ_mult = 1.2
            supply_mult = 1.3
            disaster_label = "flood"
        elif "drought" in disaster_lower:
            econ_mult = 1.1
            supply_mult = 1.0
            disaster_label = "drought"
        elif "cyclone" in disaster_lower or "hurricane" in disaster_lower or "typhoon" in disaster_lower:
            econ_mult = 1.3
            supply_mult = 1.4
            disaster_label = "cyclone"
        elif "wildfire" in disaster_lower:
            econ_mult = 1.0
            supply_mult = 1.1
            disaster_label = "wildfire"
        else:
            econ_mult = 1.0
            supply_mult = 1.0
            disaster_label = str(disaster_type)

        econ_power_delta  = -clamp(0.15 * mag * econ_mult * baseline_exposure)
        climate_delta     = clamp(0.20 * mag)
        risk_delta        = clamp(0.25 * mag * baseline_exposure)
        vuln_delta        = clamp(0.22 * mag * baseline_exposure)
        supply_delta      = clamp(0.15 * mag * supply_mult)

        target_affected = AffectedCountry(
            country=target,
            impact_type="direct",
            severity=_severity(abs(econ_power_delta) + risk_delta),
            score_deltas=[
                _make_delta("economic_power_score",      tr.get("econ_power"),    econ_power_delta),
                _make_delta("climate_vulnerability_score", tr.get("climate_vuln"), climate_delta),
                _make_delta("global_risk_score",         tr.get("global_risk"),   risk_delta),
                _make_delta("overall_vulnerability_score", tr.get("vulnerability"), vuln_delta),
            ],
            exposure_usd=float(damage_usd) if damage_usd else None,
            summary=(
                f"{target} hit by major {disaster_label}; "
                f"GDP contraction estimated at {abs(econ_power_delta)*100:.1f} points, "
                f"infrastructure damage disrupts supply chains."
            ),
        )

        # Supply chain partners
        supply_rows = _run(conn, """
            MATCH (t:Country {name: $target})-[r:DISRUPTS_SUPPLY_CHAIN]->(other:Country)
            RETURN other.name AS country,
                   r.value AS disruption_score,
                   other.trade_vulnerability_score AS trade_vuln
            ORDER BY r.value DESC
            LIMIT 8
        """, {"target": target})

        supply_affected: list[AffectedCountry] = []
        for row in supply_rows:
            country = row.get("country")
            if not country:
                continue
            base_disruption = float(row.get("disruption_score") or 0.0)
            spill = clamp(base_disruption * mag * 0.5)
            if spill < 0.02:
                continue
            supply_affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity=_severity(spill),
                score_deltas=[
                    _make_delta("trade_vulnerability_score", row.get("trade_vuln"), spill, 0.65),
                ],
                summary=(
                    f"{country}'s supply chains linked to {target} face disruption "
                    f"following the {disaster_label} (disruption score: {base_disruption:.2f})."
                ),
            ))

        # Energy cascade if target is an energy exporter
        energy_rows = _run(conn, """
            MATCH (t:Country {name: $target})-[r:EXPORTS_ENERGY_TO]->(importer:Country)
            WHERE r.year = $year
            RETURN importer.name AS country,
                   r.normalized_weight AS weight,
                   importer.energy_vulnerability_score AS energy_vuln
            ORDER BY r.normalized_weight DESC
            LIMIT 5
        """, {"target": target, "year": year})

        energy_affected: list[AffectedCountry] = []
        for row in energy_rows:
            country = row.get("country")
            if not country:
                continue
            weight = float(row.get("weight") or 0.0)
            spill  = clamp(weight * 0.20 * mag)
            if spill < 0.02:
                continue
            energy_affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity=_severity(spill),
                score_deltas=[
                    _make_delta("energy_vulnerability_score", row.get("energy_vuln"), spill, 0.60),
                ],
                summary=(
                    f"{country} imports energy from {target}; disaster disrupts "
                    f"energy production and export capacity."
                ),
            ))

        cascades = [
            CascadeEffect(
                mechanism=f"{disaster_label.title()} impact",
                affected=target,
                severity=_severity(risk_delta),
                description=(
                    f"Physical damage to infrastructure, housing, and industrial capacity "
                    f"in {target} requires 2-10 year recovery timeline."
                ),
            ),
            CascadeEffect(
                mechanism="Supply chain disruption",
                affected=f"{len(supply_rows)} trade partners",
                severity="medium" if supply_rows else "low",
                description=(
                    f"Key export infrastructure in {target} damaged; "
                    f"trading partners must find alternative suppliers."
                ),
            ),
        ]
        if energy_rows:
            cascades.append(CascadeEffect(
                mechanism="Energy supply disruption",
                affected=f"{len(energy_rows)} energy importers",
                severity="high",
                description=(
                    f"{target}'s energy export infrastructure damaged; "
                    f"importing countries face supply shortfalls and price spikes."
                ),
            ))
        if mag >= 1.5:
            cascades.append(CascadeEffect(
                mechanism="International humanitarian response",
                affected="International donors and aid organizations",
                severity="medium",
                description=(
                    f"Scale of disaster triggers international relief mobilization; "
                    f"aid flows provide partial offset to economic damage."
                ),
            ))

        all_affected = [target_affected] + supply_affected + energy_affected

        return ScenarioResult(
            scenario_type=ScenarioType.CLIMATE_DISASTER,
            actor=req.actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=sorted(all_affected, key=lambda x: abs(sum(d.delta for d in x.score_deltas)), reverse=True),
            cascade_effects=cascades,
            data_sources=[
                "climate_risk_score", "economic_power_score",
                "DISRUPTS_SUPPLY_CHAIN", "EXPORTS_ENERGY_TO",
            ],
            confidence=0.80,
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