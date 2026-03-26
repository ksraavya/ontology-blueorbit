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
# REGIME CHANGE
# =========================================================

def run_regime_change(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: significant change in government type or leadership in target country.

    Logic:
    - Target's political_stability_score changes based on new regime type
    - If democratization: scores gradually improve; if autocratization: mixed
    - global_risk_score spikes during transition regardless of direction
    - Diplomatic relations with democratic/autocratic blocs shift
    - Trade may be disrupted during transition period

    extra_params:
        direction: "democratization" | "autocratization" | "coup" | "revolution"
        new_alignment: optional new geopolitical alignment

    Reads:
        political_stability_score, global_risk_score, geopolitical_influence_score,
        diplomatic_centrality_score, bloc_alignment_score
    """
    target    = req.target or req.actor
    mag       = req.magnitude
    year      = req.year
    direction = req.extra_params.get("direction", "transition")
    new_align = req.extra_params.get("new_alignment")

    if not target:
        return _empty_result(req, "regime_change requires a target country")

    conn = Neo4jConnection()
    try:
        target_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.political_stability_score AS political_stability,
                   c.global_risk_score AS global_risk,
                   c.overall_vulnerability_score AS vulnerability,
                   c.diplomatic_centrality_score AS centrality,
                   c.geopolitical_influence_score AS geo_influence,
                   c.bloc_alignment_score AS bloc_align,
                   c.economic_influence_score AS econ_influence
        """, {"name": target})
        tr = target_rows[0] if target_rows else {}

        # Transition period: global_risk always spikes short-term
        risk_delta    = clamp(0.25 * mag)   # transition turbulence
        vuln_delta    = clamp(0.20 * mag)

        if direction == "democratization":
            stability_delta  = clamp(0.15 * mag)    # long-run improvement
            geo_delta        = clamp(0.08 * mag)
            risk_delta       = clamp(0.20 * mag)    # slightly lower instability
            description_note = "transition to more democratic governance"
        elif direction == "autocratization":
            stability_delta  = -clamp(0.10 * mag)   # reduced popular legitimacy
            geo_delta        = -clamp(0.05 * mag)
            risk_delta       = clamp(0.22 * mag)
            description_note = "shift toward authoritarian governance"
        elif direction == "coup":
            stability_delta  = -clamp(0.25 * mag)
            geo_delta        = -clamp(0.15 * mag)
            risk_delta       = clamp(0.35 * mag)
            vuln_delta       = clamp(0.30 * mag)
            description_note = "military coup and forced government change"
        elif direction == "revolution":
            stability_delta  = -clamp(0.15 * mag)   # uncertain but may stabilize
            geo_delta        = clamp(0.05 * mag)     # could go either way
            risk_delta       = clamp(0.30 * mag)
            vuln_delta       = clamp(0.25 * mag)
            description_note = "popular revolution and leadership change"
        else:
            stability_delta  = 0.0
            geo_delta        = 0.0
            description_note = "political transition"

        deltas = [
            _make_delta("political_stability_score",   tr.get("political_stability"), stability_delta),
            _make_delta("global_risk_score",           tr.get("global_risk"),         risk_delta),
            _make_delta("overall_vulnerability_score", tr.get("vulnerability"),       vuln_delta),
            _make_delta("geopolitical_influence_score", tr.get("geo_influence"),      geo_delta),
        ]
        if new_align:
            deltas.append(
                _make_delta("bloc_alignment_score", tr.get("bloc_align"), clamp(0.10 * mag) if new_align else 0.0, 0.6)
            )

        target_affected = AffectedCountry(
            country=target,
            impact_type="direct",
            severity=_severity(risk_delta),
            score_deltas=deltas,
            summary=(
                f"{target} undergoes {description_note}; "
                f"short-term instability{'and leadership vacuum' if direction == 'coup' else ''} "
                f"creates uncertainty for investors and allies."
                + (f" New alignment toward {new_align}." if new_align else "")
            ),
        )

        # Neighboring countries face refugee flows and instability
        region_rows = _run(conn, """
            MATCH (c:Country {name: $target})-[:BELONGS_TO]->(r:Region)<-[:BELONGS_TO]-(neighbor:Country)
            WHERE neighbor.name <> $target
            RETURN neighbor.name AS country,
                   neighbor.global_risk_score AS global_risk,
                   neighbor.overall_vulnerability_score AS vulnerability
            LIMIT 5
        """, {"target": target})

        neighbor_affected: list[AffectedCountry] = []
        for row in region_rows:
            country = row.get("country")
            if not country:
                continue
            spill = clamp(0.06 * mag)
            neighbor_affected.append(AffectedCountry(
                country=country,
                impact_type="regional",
                severity="low",
                score_deltas=[
                    _make_delta("global_risk_score", row.get("global_risk"), spill, 0.5),
                ],
                summary=(
                    f"{country} faces refugee flows and border security pressure "
                    f"from political upheaval in {target}."
                ),
            ))

        cascades = [
            CascadeEffect(
                mechanism=description_note.title(),
                affected=target,
                severity=_severity(risk_delta),
                description=(
                    f"Political transition in {target} disrupts institutional continuity; "
                    f"international agreements and commitments under review."
                ),
            ),
            CascadeEffect(
                mechanism="Investment uncertainty",
                affected=f"{target} and foreign investors",
                severity="high" if direction in ("coup", "revolution") else "medium",
                description=(
                    f"Foreign direct investment pauses pending clarity on new "
                    f"government's policy orientation and contract enforcement."
                ),
            ),
        ]

        if direction in ("coup", "revolution"):
            cascades.append(CascadeEffect(
                mechanism="Humanitarian pressure",
                affected=f"{target} and neighboring countries",
                severity="high",
                description=(
                    "Internal displacement and potential refugee flows create "
                    "humanitarian burden for neighboring countries."
                ),
            ))

        return ScenarioResult(
            scenario_type=ScenarioType.REGIME_CHANGE,
            actor=req.actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=[target_affected] + neighbor_affected,
            cascade_effects=cascades,
            data_sources=["political_stability_score", "global_risk_score", "BELONGS_TO"],
            confidence=0.68,
        )
    finally:
        conn.close()


# =========================================================
# REUNIFICATION
# =========================================================

def run_reunification(req: ScenarioRequest) -> ScenarioResult:
    """
    Scenario: two separated territories begin formal reunification process.
    Actor = initiating territory. Target = the other territory or the combined state.

    Logic:
    - Combined economic power rises (GDP pooling)
    - diplomatic_centrality improves as unified state
    - Transition creates short-term vulnerability (institutional integration)
    - Regional powers with interests in both may gain or lose
    - Alliance memberships may need renegotiation

    Reads:
        economic_power_score, diplomatic_centrality_score, strategic_influence_score,
        HAS_GDP, MEMBER_OF
    """
    actor  = req.actor
    target = req.target
    mag    = req.magnitude
    year   = req.year

    if not actor or not target:
        return _empty_result(req, "reunification requires both actor and target (the two territories)")

    conn = Neo4jConnection()
    try:
        actor_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.economic_power_score AS econ_power,
                   c.diplomatic_centrality_score AS centrality,
                   c.strategic_influence_score AS strat_influence,
                   c.overall_vulnerability_score AS vulnerability,
                   c.military_strength_score AS military
        """, {"name": actor})
        ar = actor_rows[0] if actor_rows else {}

        target_rows = _run(conn, """
            MATCH (c:Country {name: $name})
            RETURN c.economic_power_score AS econ_power,
                   c.diplomatic_centrality_score AS centrality,
                   c.strategic_influence_score AS strat_influence,
                   c.overall_vulnerability_score AS vulnerability
        """, {"name": target})
        tr = target_rows[0] if target_rows else {}

        # GDP pooling effect
        actor_gdp_rows = _run(conn, """
            MATCH (c:Country {name: $name})-[r:HAS_GDP]->(:Metric)
            WHERE r.year = $year
            RETURN r.value AS gdp
        """, {"name": actor, "year": year})
        target_gdp_rows = _run(conn, """
            MATCH (c:Country {name: $name})-[r:HAS_GDP]->(:Metric)
            WHERE r.year = $year
            RETURN r.value AS gdp
        """, {"name": target, "year": year})

        actor_gdp  = float(actor_gdp_rows[0].get("gdp") or 0.0) if actor_gdp_rows else 0.0
        target_gdp = float(target_gdp_rows[0].get("gdp") or 0.0) if target_gdp_rows else 0.0
        combined_gdp = actor_gdp + target_gdp

        econ_power_delta = clamp(0.20 * mag)
        centrality_delta = clamp(0.15 * mag)
        strat_delta      = clamp(0.12 * mag)
        vuln_delta       = clamp(0.10 * mag)   # transition vulnerability

        actor_affected = AffectedCountry(
            country=actor,
            impact_type="direct",
            severity="high",
            score_deltas=[
                _make_delta("economic_power_score",      ar.get("econ_power"),    econ_power_delta),
                _make_delta("diplomatic_centrality_score", ar.get("centrality"),  centrality_delta),
                _make_delta("strategic_influence_score", ar.get("strat_influence"), strat_delta),
                _make_delta("overall_vulnerability_score", ar.get("vulnerability"), vuln_delta),
            ],
            exposure_usd=combined_gdp,
            summary=(
                f"{actor} and {target} begin reunification; "
                f"combined economy ~${combined_gdp/1e12:.1f}T, "
                f"diplomatic weight increases significantly."
            ),
        )

        target_affected = AffectedCountry(
            country=target,
            impact_type="direct",
            severity="high",
            score_deltas=[
                _make_delta("economic_power_score",      tr.get("econ_power"),    econ_power_delta * 0.8),
                _make_delta("diplomatic_centrality_score", tr.get("centrality"),  centrality_delta * 0.8),
                _make_delta("overall_vulnerability_score", tr.get("vulnerability"), vuln_delta),
            ],
            summary=(
                f"{target} enters reunification process with {actor}; "
                f"institutional integration and governance harmonization required."
            ),
        )

        # Regional powers with interests in the divided state
        region_rows = _run(conn, """
            MATCH (c:Country)-[:BELONGS_TO]->(r:Region)<-[:BELONGS_TO]-(involved:Country)
            WHERE involved.name IN [$actor, $target]
              AND c.name <> $actor AND c.name <> $target
            RETURN DISTINCT c.name AS country,
                   c.strategic_influence_score AS strat_influence,
                   c.global_risk_score AS global_risk
            ORDER BY c.strategic_influence_score DESC
            LIMIT 5
        """, {"actor": actor, "target": target})

        region_affected: list[AffectedCountry] = []
        for row in region_rows[:3]:
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
                summary=(
                    f"{country} recalibrates its regional strategy as "
                    f"{actor}–{target} reunification shifts regional balance."
                ),
            ))

        cascades = [
            CascadeEffect(
                mechanism="Economic integration",
                affected=f"{actor} and {target}",
                severity="medium",
                description=(
                    f"Currency, legal, and economic systems must be harmonized; "
                    f"transition costs are significant but long-run GDP synergies emerge."
                ),
            ),
            CascadeEffect(
                mechanism="Alliance renegotiation",
                affected="Alliance partners",
                severity="medium",
                description=(
                    f"Existing alliance memberships must be renegotiated for the unified state; "
                    f"competing alliance obligations from both territories create complexity."
                ),
            ),
            CascadeEffect(
                mechanism="Regional balance shift",
                affected=f"Regional neighbors",
                severity="medium",
                description=(
                    f"A unified {actor}–{target} entity represents a meaningfully "
                    f"larger geopolitical actor, reshaping regional power dynamics."
                ),
            ),
        ]

        return ScenarioResult(
            scenario_type=ScenarioType.REUNIFICATION,
            actor=actor,
            target=target,
            raw_query=req.raw_query,
            year=year,
            headline="",
            summary="",
            affected_countries=[actor_affected, target_affected] + region_affected,
            cascade_effects=cascades,
            data_sources=["economic_power_score", "strategic_influence_score", "HAS_GDP", "MEMBER_OF"],
            confidence=0.65,
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