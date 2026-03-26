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
# FOOD SUPPLY SHOCK
# =========================================================

def run_food_supply_shock(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: major disruption to agricultural exports or food supply.
    Actor = country whose food exports are disrupted.
    Target = primary importing country affected.

    Logic:
    - Actor loses agricultural export revenue
    - Target faces food security risk (global_risk and overall_vulnerability rise)
    - Countries heavily importing food from actor cascade
    - Food-insecure countries (high existing vulnerability) most impacted

    Reads:
        EXPORTS_TO, HAS_TRADE_DEPENDENCY_ON, overall_vulnerability_score,
        global_risk_score, climate_vulnerability_score
    """
    actor  = req.actor
    target = req.target
    mag    = req.magnitude
    year   = req.year

    conn = Neo4jConnection()
    try:
        # Find countries dependent on actor's food exports
        dependent_rows = _run(conn, """
            MATCH (importer:Country)-[r:HAS_TRADE_DEPENDENCY_ON]->(exporter:Country)
            WHERE ($actor IS NULL OR exporter.name = $actor)
              AND r.year = $year AND r.dependency >= 0.08
            RETURN importer.name AS country,
                   exporter.name AS source,
                   r.dependency AS dependency,
                   importer.overall_vulnerability_score AS vulnerability,
                   importer.global_risk_score AS global_risk,
                   importer.climate_vulnerability_score AS climate_vuln
            ORDER BY r.dependency DESC
            LIMIT 12
        """, {"actor": actor, "year": year})

        affected: list[AffectedCountry] = []
        seen: set[str] = set()

        # Primary target
        if target:
            target_rows = _run(conn, """
                MATCH (c:Country {name: $name})
                RETURN c.overall_vulnerability_score AS vulnerability,
                       c.global_risk_score AS global_risk,
                       c.climate_vulnerability_score AS climate_vuln
            """, {"name": target})
            tr = target_rows[0] if target_rows else {}

            target_affected = AffectedCountry(
                country=target,
                impact_type="direct",
                severity=_severity(0.25 * mag),
                score_deltas=[
                    _make_delta("overall_vulnerability_score", tr.get("vulnerability"), clamp(0.25 * mag)),
                    _make_delta("global_risk_score",           tr.get("global_risk"),   clamp(0.20 * mag)),
                ],
                summary=(
                    f"{target} faces primary food supply disruption; "
                    f"import substitution requires urgent procurement from alternative sources."
                ),
            )
            affected.append(target_affected)
            seen.add(target)

        # Cascade to other importers
        for row in dependent_rows:
            country = row.get("country")
            if not country or country in seen:
                continue
            if target and country == target:
                continue
            seen.add(country)
            dep  = float(row.get("dependency") or 0.0)
            spill = clamp(dep * 0.20 * mag)
            vuln  = float(row.get("vulnerability") or 0.0)
            severity_mult = 1.3 if vuln > 0.5 else 1.0  # more vulnerable countries hit harder

            affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity=_severity(spill * severity_mult),
                score_deltas=[
                    _make_delta("overall_vulnerability_score", row.get("vulnerability"), clamp(spill * severity_mult), 0.65),
                    _make_delta("global_risk_score",           row.get("global_risk"),   clamp(spill * 0.6), 0.60),
                ],
                summary=(
                    f"{country} has {dep*100:.0f}% food trade dependency on "
                    f"{row.get('source')}; supply shock raises food security risk."
                ),
            ))

        # Actor loses export revenue
        if actor:
            actor_rows = _run(conn, """
                MATCH (c:Country {name: $name})
                RETURN c.economic_influence_score AS econ_influence,
                       c.trade_vulnerability_score AS trade_vuln
            """, {"name": actor})
            ar = actor_rows[0] if actor_rows else {}
            if actor not in seen:
                seen.add(actor)
                affected.insert(0, AffectedCountry(
                    country=actor,
                    impact_type="direct",
                    severity="medium",
                    score_deltas=[
                        _make_delta("economic_influence_score", ar.get("econ_influence"), -clamp(0.08 * mag), 0.6),
                        _make_delta("trade_vulnerability_score", ar.get("trade_vuln"),    clamp(0.05 * mag), 0.6),
                    ],
                    summary=f"{actor} loses agricultural export revenue; trade position weakens.",
                ))

        cascades = [
            CascadeEffect(
                mechanism="Food price spike",
                affected="All food-importing countries",
                severity=_severity(0.20 * mag),
                description=(
                    f"Disruption of {actor or 'major producer'}'s food exports "
                    f"drives up global grain/food prices, hitting low-income importers hardest."
                ),
            ),
            CascadeEffect(
                mechanism="Social stability pressure",
                affected="High-vulnerability importers",
                severity="high" if mag >= 1.3 else "medium",
                description=(
                    "Rising food prices in already-vulnerable countries create "
                    "domestic political pressure and potential unrest."
                ),
            ),
        ]

        return ScenarioResult(
            scenario_type=ScenarioType.FOOD_SUPPLY_SHOCK,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=affected,
            cascade_effects=cascades,
            data_sources=["HAS_TRADE_DEPENDENCY_ON", "overall_vulnerability_score", "climate_vulnerability_score"],
            confidence=0.78,
        )
    finally:
        conn.close()


# =========================================================
# SUPPLY CHAIN COLLAPSE
# =========================================================

def run_supply_chain_collapse(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: key shipping route, port, or logistics hub is disrupted.
    Target = the country or chokepoint affected.

    Logic:
    - All countries with supply chain dependency on target face disruption
    - Trade costs rise globally for goods transiting the affected route
    - Countries with alternative routing face mild gains

    Reads:
        DISRUPTS_SUPPLY_CHAIN, supply_chain_risk_score, HAS_TRADE_VOLUME_WITH,
        trade_vulnerability_score, economic_power_score
    """
    target = req.target or req.actor
    mag    = req.magnitude
    year   = req.year

    if not target:
        return _empty_result(req, "supply_chain_collapse requires a target (the disrupted hub/country)")

    conn = Neo4jConnection()
    try:
        # Target's own scores
        target_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.economic_power_score AS econ_power,
                   c.supply_chain_risk_score AS supply_chain_risk,
                   c.trade_vulnerability_score AS trade_vuln,
                   c.global_risk_score AS global_risk
        """, {"name": target})
        tr = target_rows[0] if target_rows else {}

        target_econ_delta     = -clamp(0.12 * mag)
        target_risk_delta     = clamp(0.15 * mag)
        target_supply_delta   = clamp(0.20 * mag)

        target_affected = AffectedCountry(
            country=target,
            impact_type="direct",
            severity=_severity(target_supply_delta),
            score_deltas=[
                _make_delta("economic_power_score",      tr.get("econ_power"),      target_econ_delta),
                _make_delta("supply_chain_risk_score",   tr.get("supply_chain_risk"), target_supply_delta),
                _make_delta("global_risk_score",         tr.get("global_risk"),     target_risk_delta),
            ],
            summary=(
                f"{target}'s logistics infrastructure disrupted; "
                f"port congestion, shipping delays cascade through global supply chains."
            ),
        )

        # Countries dependent on target for supply chain
        dependent_rows = _run(conn, """
            MATCH (other:Country)-[r:DISRUPTS_SUPPLY_CHAIN]->(t:Country {name: $target})
            RETURN other.name AS country,
                   r.value AS disruption_value,
                   other.trade_vulnerability_score AS trade_vuln,
                   other.economic_power_score AS econ_power
            ORDER BY r.value DESC
            LIMIT 10
        """, {"target": target})

        # Also get major trade partners
        trade_rows = _run(conn, """
            MATCH (other:Country)-[r:HAS_TRADE_VOLUME_WITH]-(t:Country {name: $target})
            WHERE r.year = $year AND r.normalized_weight >= 0.1
            RETURN other.name AS country,
                   r.normalized_weight AS weight,
                   other.trade_vulnerability_score AS trade_vuln
            ORDER BY r.normalized_weight DESC
            LIMIT 8
        """, {"target": target, "year": year})

        cascade_countries: list[AffectedCountry] = []
        seen: set[str] = {target}

        for row in (dependent_rows + trade_rows):
            country = row.get("country")
            if not country or country in seen:
                continue
            seen.add(country)
            base = float(row.get("disruption_value") or row.get("weight") or 0.0)
            spill = clamp(base * 0.25 * mag)
            if spill < 0.015:
                continue
            cascade_countries.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity=_severity(spill),
                score_deltas=[
                    _make_delta("trade_vulnerability_score", row.get("trade_vuln"), spill, 0.65),
                ],
                summary=(
                    f"{country} faces higher shipping costs and delivery delays "
                    f"as {target}'s logistics hub disruption ripples outward."
                ),
            ))

        return ScenarioResult(
            scenario_type=ScenarioType.SUPPLY_CHAIN_COLLAPSE,
            actor=req.actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=[target_affected] + cascade_countries,
            cascade_effects=[
                CascadeEffect(
                    mechanism="Logistics hub disruption",
                    affected=f"{target} and {len(cascade_countries)} trade partners",
                    severity=_severity(target_supply_delta),
                    description=(
                        f"Disruption at {target}'s key logistics nodes forces "
                        f"rerouting of cargo through alternative (more expensive) routes."
                    ),
                ),
                CascadeEffect(
                    mechanism="Freight cost spike",
                    affected="Global shipping markets",
                    severity="medium",
                    description=(
                        "Container freight rates spike for routes transiting the disrupted hub; "
                        "inflationary pressure on goods prices globally."
                    ),
                ),
            ],
            data_sources=["DISRUPTS_SUPPLY_CHAIN", "HAS_TRADE_VOLUME_WITH", "supply_chain_risk_score"],
            confidence=0.75,
        )
    finally:
        conn.close()


# =========================================================
# RESOURCE SCARCITY
# =========================================================

def run_resource_scarcity(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: critical resource (water, food, minerals) becomes scarce in target.

    Logic:
    - Target's climate_vulnerability and resource_stress rise
    - Economic output constrained (energy, agriculture, industry)
    - Countries that depend on target's resource exports face shortfalls

    Reads:
        resource_stress_score, climate_vulnerability_score, economic_power_score,
        DEPENDS_ON_RESOURCE, HAS_TRADE_DEPENDENCY_ON
    """
    target   = req.target or req.actor
    mag      = req.magnitude
    year     = req.year
    resource = req.extra_params.get("resource", "critical resource")

    if not target:
        return _empty_result(req, "resource_scarcity requires a target country")

    conn = Neo4jConnection()
    try:
        target_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.resource_stress_score AS resource_stress,
                   c.climate_vulnerability_score AS climate_vuln,
                   c.economic_power_score AS econ_power,
                   c.overall_vulnerability_score AS vulnerability,
                   c.global_risk_score AS global_risk
        """, {"name": target})
        tr = target_rows[0] if target_rows else {}

        resource_delta   = clamp(0.25 * mag)
        climate_delta    = clamp(0.15 * mag)
        econ_delta       = -clamp(0.12 * mag)
        risk_delta       = clamp(0.18 * mag)

        target_affected = AffectedCountry(
            country=target,
            impact_type="direct",
            severity=_severity(resource_delta),
            score_deltas=[
                _make_delta("resource_stress_score",       tr.get("resource_stress"), resource_delta),
                _make_delta("climate_vulnerability_score", tr.get("climate_vuln"),    climate_delta),
                _make_delta("economic_power_score",        tr.get("econ_power"),      econ_delta),
                _make_delta("global_risk_score",           tr.get("global_risk"),     risk_delta),
            ],
            summary=(
                f"{target} faces critical scarcity of {resource}; "
                f"economic output constrained, social stability at risk."
            ),
        )

        # Countries dependent on target's resource exports
        resource_rows = _run(conn, """
            MATCH (other:Country)-[r:DEPENDS_ON_RESOURCE]->(t:Country {name: $target})
            RETURN other.name AS country,
                   r.value AS dependency_score,
                   other.resource_stress_score AS resource_stress
            ORDER BY r.value DESC
            LIMIT 6
        """, {"target": target})

        cascade_countries: list[AffectedCountry] = []
        for row in resource_rows:
            country = row.get("country")
            if not country:
                continue
            dep_score = float(row.get("dependency_score") or 0.0)
            spill = clamp(dep_score * 0.3 * mag)
            cascade_countries.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity=_severity(spill),
                score_deltas=[
                    _make_delta("resource_stress_score", row.get("resource_stress"), spill, 0.60),
                ],
                summary=f"{country} depends on {target}'s {resource} exports; scarcity creates upstream supply stress.",
            ))

        return ScenarioResult(
            scenario_type=ScenarioType.RESOURCE_SCARCITY,
            actor=req.actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=[target_affected] + cascade_countries,
            cascade_effects=[
                CascadeEffect(
                    mechanism=f"{resource.title()} scarcity",
                    affected=target,
                    severity=_severity(resource_delta),
                    description=(
                        f"Declining availability of {resource} in {target} "
                        f"constrains agriculture, industry, and civilian welfare."
                    ),
                ),
                CascadeEffect(
                    mechanism="Resource competition",
                    affected="Regional neighbors",
                    severity="medium",
                    description=(
                        f"Scarcity of {resource} may drive transboundary competition "
                        f"or migration as populations seek adequate supply."
                    ),
                ),
            ],
            data_sources=["resource_stress_score", "DEPENDS_ON_RESOURCE", "climate_vulnerability_score"],
            confidence=0.72,
        )
    finally:
        conn.close()


# =========================================================
# CLIMATE MIGRATION
# =========================================================

def run_climate_migration(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: large-scale population displacement due to climate.
    Actor = origin country (sending migrants).
    Target = destination country or region.

    Logic:
    - Origin's climate_vulnerability and overall_vulnerability rises
    - Destination faces social and fiscal pressure
    - Regional instability rises for transit countries

    Reads:
        climate_vulnerability_score, overall_vulnerability_score, global_risk_score,
        BELONGS_TO
    """
    actor  = req.actor   # origin (source of migration)
    target = req.target  # destination
    mag    = req.magnitude
    year   = req.year

    if not actor:
        return _empty_result(req, "climate_migration requires an actor (origin country)")

    conn = Neo4jConnection()
    try:
        actor_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.climate_vulnerability_score AS climate_vuln,
                   c.overall_vulnerability_score AS vulnerability,
                   c.global_risk_score AS global_risk,
                   c.economic_power_score AS econ_power
        """, {"name": actor})
        ar = actor_rows[0] if actor_rows else {}

        # Origin: climate vulnerability spikes, economic base shrinks
        climate_delta = clamp(0.20 * mag)
        vuln_delta    = clamp(0.25 * mag)
        econ_delta    = -clamp(0.10 * mag)
        risk_delta    = clamp(0.15 * mag)

        actor_affected = AffectedCountry(
            country=actor,
            impact_type="direct",
            severity=_severity(climate_delta + vuln_delta),
            score_deltas=[
                _make_delta("climate_vulnerability_score", ar.get("climate_vuln"), climate_delta),
                _make_delta("overall_vulnerability_score", ar.get("vulnerability"), vuln_delta),
                _make_delta("economic_power_score",        ar.get("econ_power"),   econ_delta),
                _make_delta("global_risk_score",           ar.get("global_risk"),  risk_delta),
            ],
            summary=(
                f"{actor} experiences mass climate displacement; "
                f"agricultural zones and coastal areas becoming uninhabitable."
            ),
        )

        affected: list[AffectedCountry] = [actor_affected]

        # Destination country pressure
        if target:
            target_rows = _run(conn, """
                MATCH (c:Country {name: $name})
                RETURN c.overall_vulnerability_score AS vulnerability,
                       c.global_risk_score AS global_risk,
                       c.economic_power_score AS econ_power
            """, {"name": target})
            tr = target_rows[0] if target_rows else {}

            dest_vuln_delta = clamp(0.08 * mag)
            dest_risk_delta = clamp(0.06 * mag)
            dest_econ_delta = -clamp(0.04 * mag)

            affected.append(AffectedCountry(
                country=target,
                impact_type="direct",
                severity="medium",
                score_deltas=[
                    _make_delta("overall_vulnerability_score", tr.get("vulnerability"), dest_vuln_delta),
                    _make_delta("global_risk_score",           tr.get("global_risk"),   dest_risk_delta),
                    _make_delta("economic_power_score",        tr.get("econ_power"),    dest_econ_delta),
                ],
                summary=(
                    f"{target} receives large influx of climate migrants from {actor}; "
                    f"social services, housing, and labor markets under pressure."
                ),
            ))

        # Regional transit countries
        region_rows = _run(conn, """
            MATCH (c:Country {name: $actor})-[:BELONGS_TO]->(r:Region)<-[:BELONGS_TO]-(neighbor:Country)
            WHERE neighbor.name <> $actor AND ($target IS NULL OR neighbor.name <> $target)
            RETURN neighbor.name AS country,
                   neighbor.global_risk_score AS global_risk,
                   neighbor.overall_vulnerability_score AS vulnerability
            LIMIT 4
        """, {"actor": actor, "target": target})

        for row in region_rows:
            country = row.get("country")
            if not country:
                continue
            spill = clamp(0.05 * mag)
            affected.append(AffectedCountry(
                country=country,
                impact_type="regional",
                severity="low",
                score_deltas=[
                    _make_delta("global_risk_score",           row.get("global_risk"),  spill, 0.5),
                    _make_delta("overall_vulnerability_score", row.get("vulnerability"), spill * 0.5, 0.45),
                ],
                summary=f"{country} serves as transit route for climate migrants from {actor}.",
            ))

        return ScenarioResult(
            scenario_type=ScenarioType.CLIMATE_MIGRATION,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=affected,
            cascade_effects=[
                CascadeEffect(
                    mechanism="Mass displacement",
                    affected=f"{actor} and neighbors",
                    severity=_severity(climate_delta + vuln_delta),
                    description=(
                        f"Climate-driven displacement from {actor} creates a "
                        f"multi-country humanitarian challenge requiring regional coordination."
                    ),
                ),
                CascadeEffect(
                    mechanism="Receiving country pressure",
                    affected=target or "destination countries",
                    severity="medium",
                    description=(
                        "Large migrant populations strain public services and "
                        "may generate domestic political tension in receiving countries."
                    ),
                ),
            ],
            data_sources=["climate_vulnerability_score", "overall_vulnerability_score", "BELONGS_TO"],
            confidence=0.70,
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