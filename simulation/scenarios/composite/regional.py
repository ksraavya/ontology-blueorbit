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
# REGIONAL DESTABILIZATION
# =========================================================

def run_regional_destabilization(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: entire region enters prolonged instability.
    Target = region name or a representative country in the region.

    Logic:
    - All countries in the region face rising conflict_risk and global_risk
    - Economic activity contracts across the region
    - External powers with interests in the region face increased engagement costs
    - Supply chains through the region disrupted

    Reads:
        BELONGS_TO, conflict_risk_score, global_risk_score,
        overall_vulnerability_score, economic_power_score, IS_INFLUENTIAL_TO
    """
    target = req.target or req.actor   # region name or lead country
    mag    = req.magnitude
    year   = req.year
    region_name = req.extra_params.get("region", target)

    if not target:
        return _empty_result(req, "regional_destabilization requires a target (region or country)")

    conn = Neo4jConnection()
    try:
        # Try to find region members directly by region name
        region_member_rows = _run(conn, """
            MATCH (c:Country)-[:BELONGS_TO]->(r:Region {name: $region})
            RETURN c.name AS country,
                   c.conflict_risk_score AS conflict_risk,
                   c.global_risk_score AS global_risk,
                   c.overall_vulnerability_score AS vulnerability,
                   c.economic_power_score AS econ_power,
                   c.trade_vulnerability_score AS trade_vuln
            ORDER BY c.global_risk_score DESC
        """, {"region": region_name})

        # If no region found by name, find neighbors via the target country
        if not region_member_rows and target:
            region_member_rows = _run(conn, """
                MATCH (lead:Country {name: $target})-[:BELONGS_TO]->(r:Region)<-[:BELONGS_TO]-(c:Country)
                RETURN c.name AS country,
                       c.conflict_risk_score AS conflict_risk,
                       c.global_risk_score AS global_risk,
                       c.overall_vulnerability_score AS vulnerability,
                       c.economic_power_score AS econ_power,
                       c.trade_vulnerability_score AS trade_vuln,
                       r.name AS region_name
                ORDER BY c.global_risk_score DESC
            """, {"target": target})
            if region_member_rows:
                region_name = region_member_rows[0].get("region_name") or region_name

        if not region_member_rows:
            return _empty_result(req, f"No region found for target '{target}'")

        affected: list[AffectedCountry] = []
        total_econ_exposure = 0.0

        for row in region_member_rows:
            country = row.get("country")
            if not country:
                continue

            econ_power    = float(row.get("econ_power") or 0.0)
            current_risk  = float(row.get("conflict_risk") or 0.0)
            total_econ_exposure += econ_power

            # Higher baseline risk = worse outcome during destabilization
            risk_multiplier = 1.0 + current_risk * 0.5
            conflict_delta = clamp(0.20 * mag * risk_multiplier)
            risk_delta     = clamp(0.18 * mag * risk_multiplier)
            econ_delta     = -clamp(0.12 * mag)
            vuln_delta     = clamp(0.15 * mag)

            affected.append(AffectedCountry(
                country=country,
                impact_type="direct",
                severity=_severity((conflict_delta + risk_delta) * 0.5),
                score_deltas=[
                    _make_delta("conflict_risk_score",       row.get("conflict_risk"),  conflict_delta),
                    _make_delta("global_risk_score",         row.get("global_risk"),    risk_delta),
                    _make_delta("economic_power_score",      row.get("econ_power"),     econ_delta),
                    _make_delta("overall_vulnerability_score", row.get("vulnerability"), vuln_delta),
                ],
                summary=(
                    f"{country} caught in regional destabilization; "
                    f"conflict risk and economic disruption escalate simultaneously."
                ),
            ))

        # External powers with influence in the region
        external_rows = _run(conn, """
            MATCH (external:Country)-[r:IS_INFLUENTIAL_TO]->(c:Country)
            WHERE c.name IN $region_countries AND external.name <> $target
            WITH external, count(c) AS influence_count, avg(r.normalized_weight) AS avg_weight
            WHERE influence_count >= 2
            RETURN external.name AS country,
                   influence_count,
                   avg_weight,
                   external.strategic_influence_score AS strat_influence
            ORDER BY avg_weight DESC
            LIMIT 5
        """, {"region_countries": [r.get("country") for r in region_member_rows if r.get("country")], "target": target})

        for row in external_rows:
            country = row.get("country")
            if not country:
                continue
            engagement_cost = clamp(float(row.get("avg_weight") or 0.0) * 0.08 * mag)
            affected.append(AffectedCountry(
                country=country,
                impact_type="cascade",
                severity="low",
                score_deltas=[
                    _make_delta("strategic_influence_score", row.get("strat_influence"),
                                -clamp(0.04 * mag), 0.55),
                ],
                summary=(
                    f"{country} has strategic interests in {region_name}; "
                    f"regional destabilization raises engagement costs and risks."
                ),
            ))

        n_countries = len(region_member_rows)

        cascades = [
            CascadeEffect(
                mechanism="Regional conflict cascade",
                affected=f"All {n_countries} countries in {region_name}",
                severity=_severity(0.20 * mag),
                description=(
                    f"Prolonged instability in {region_name} creates refugee flows, "
                    f"border disputes, and competing proxy interests."
                ),
            ),
            CascadeEffect(
                mechanism="Economic disruption",
                affected=f"{region_name} trade and investment",
                severity="high",
                description=(
                    f"Investors exit the region; trade routes disrupted; "
                    f"aid flows replace FDI as primary external capital source."
                ),
            ),
            CascadeEffect(
                mechanism="External power competition",
                affected=f"Regional powers and global actors",
                severity="medium",
                description=(
                    f"Destabilized {region_name} becomes arena for competing external interests; "
                    f"proxy conflict risk rises."
                ),
            ),
        ]

        return ScenarioResult(
            scenario_type=ScenarioType.REGIONAL_DESTABILIZATION,
            actor=req.actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=sorted(affected, key=lambda x: abs(sum(d.delta for d in x.score_deltas)), reverse=True),
            cascade_effects=cascades,
            global_risk_delta=clamp(0.10 * mag),
            data_sources=[
                "BELONGS_TO", "conflict_risk_score", "global_risk_score",
                "IS_INFLUENTIAL_TO", "overall_vulnerability_score",
            ],
            confidence=0.72,
        )
    finally:
        conn.close()


# =========================================================
# GLOBAL PANDEMIC
# =========================================================

def run_global_pandemic(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: health crisis with major economic and geopolitical impact.
    Target = country of origin (if specified).

    Logic:
    - All countries face GDP contraction and trade disruption (varying by openness)
    - Highly trade-integrated countries affected more
    - Healthcare-vulnerable countries (high overall_vulnerability) face worse outcomes
    - Supply chains collapse; medical equipment becomes strategic good
    - Geopolitical competition over vaccines/treatments

    Reads:
        overall_vulnerability_score, economic_power_score, trade_vulnerability_score,
        trade_integration_score, global_risk_score, HAS_TRADE_VOLUME_WITH
    """
    target  = req.target   # origin country
    mag     = req.magnitude
    year    = req.year
    pathogen = req.extra_params.get("pathogen", "novel pathogen")

    conn = Neo4jConnection()
    try:
        # Get all countries — pandemic is global
        all_country_rows = _run(conn, """
            MATCH (c:Country)
            WHERE c.economic_power_score IS NOT NULL
            RETURN c.name AS country,
                   c.economic_power_score AS econ_power,
                   c.overall_vulnerability_score AS vulnerability,
                   c.trade_vulnerability_score AS trade_vuln,
                   c.global_risk_score AS global_risk,
                   c.trade_integration_score AS trade_integration
            ORDER BY c.economic_power_score DESC
        """)

        # Major economies and most vulnerable
        major_economies = [r for r in all_country_rows
                           if float(r.get("econ_power") or 0.0) > 0.3][:15]
        vulnerable_countries = sorted(
            all_country_rows,
            key=lambda r: float(r.get("vulnerability") or 0.0),
            reverse=True
        )[:10]

        # Combine unique countries
        seen: set[str] = set()
        priority_rows: list[dict] = []
        for row in major_economies + vulnerable_countries:
            country = row.get("country")
            if country and country not in seen:
                seen.add(country)
                priority_rows.append(row)

        affected: list[AffectedCountry] = []

        # Origin country — source of outbreak
        if target:
            origin_rows = _run(conn, """
                MATCH (c:Country {name: $name})
                RETURN c.economic_power_score AS econ_power,
                       c.overall_vulnerability_score AS vulnerability,
                       c.global_risk_score AS global_risk,
                       c.trade_vulnerability_score AS trade_vuln
            """, {"name": target})
            tr = origin_rows[0] if origin_rows else {}

            origin_econ_delta  = -clamp(0.20 * mag)
            origin_risk_delta  = clamp(0.30 * mag)
            origin_vuln_delta  = clamp(0.25 * mag)

            affected.append(AffectedCountry(
                country=target,
                impact_type="direct",
                severity="critical",
                score_deltas=[
                    _make_delta("economic_power_score",      tr.get("econ_power"),    origin_econ_delta),
                    _make_delta("global_risk_score",         tr.get("global_risk"),   origin_risk_delta),
                    _make_delta("overall_vulnerability_score", tr.get("vulnerability"), origin_vuln_delta),
                ],
                summary=(
                    f"{target} identified as outbreak origin; faces international "
                    f"scrutiny, travel bans, and severe economic disruption."
                ),
            ))
            seen.add(target)

        # All major economies and vulnerable countries
        for row in priority_rows:
            country = row.get("country")
            if not country or country in seen:
                continue
            seen.add(country)

            econ_power    = float(row.get("econ_power") or 0.0)
            vulnerability = float(row.get("vulnerability") or 0.0)
            trade_integ   = float(row.get("trade_integration") or 0.0)

            # More trade-integrated = more exposed during lockdowns
            # More vulnerable = worse health outcomes
            econ_delta  = -clamp((0.10 + trade_integ * 0.10) * mag)
            risk_delta  = clamp((0.12 + vulnerability * 0.10) * mag)
            trade_delta = clamp(0.15 * mag * trade_integ)

            affected.append(AffectedCountry(
                country=country,
                impact_type="direct" if econ_power > 0.3 else "cascade",
                severity=_severity(abs(econ_delta) + risk_delta),
                score_deltas=[
                    _make_delta("economic_power_score",      row.get("econ_power"),    econ_delta),
                    _make_delta("global_risk_score",         row.get("global_risk"),   risk_delta),
                    _make_delta("trade_vulnerability_score", row.get("trade_vuln"),    trade_delta),
                ],
                summary=(
                    f"{country}: GDP contraction ~{abs(econ_delta)*100:.1f} pts; "
                    f"trade disruption from lockdowns and border closures."
                ),
            ))

        cascades = [
            CascadeEffect(
                mechanism="Global supply chain shutdown",
                affected="All trade-integrated economies",
                severity="critical",
                description=(
                    f"Pandemic lockdowns halt global manufacturing and shipping; "
                    f"just-in-time supply chains fail across all sectors."
                ),
            ),
            CascadeEffect(
                mechanism="Emergency fiscal spending",
                affected="All governments",
                severity="high",
                description=(
                    "Mass emergency spending inflates debt levels; "
                    "fiscal space for future crises narrows across all countries."
                ),
            ),
            CascadeEffect(
                mechanism="Geopolitical vaccine competition",
                affected="Developing economies and global powers",
                severity="high",
                description=(
                    "Medical countermeasure development becomes strategic competition; "
                    "vaccine diplomacy reshapes geopolitical alignments."
                ),
            ),
            CascadeEffect(
                mechanism="Trade and travel collapse",
                affected="Airlines, tourism, services sectors globally",
                severity="critical",
                description=(
                    "International travel bans and border closures persist 12-24 months; "
                    "tourism and services sectors face existential disruption."
                ),
            ),
        ]

        return ScenarioResult(
            scenario_type=ScenarioType.GLOBAL_PANDEMIC,
            actor=req.actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=sorted(affected, key=lambda x: abs(sum(d.delta for d in x.score_deltas)), reverse=True)[:20],
            cascade_effects=cascades,
            global_risk_delta=clamp(0.20 * mag),
            data_sources=[
                "economic_power_score", "overall_vulnerability_score",
                "trade_integration_score", "global_risk_score",
            ],
            confidence=0.68,
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