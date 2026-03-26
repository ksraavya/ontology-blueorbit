from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# =========================================================
# SCENARIO TYPE ENUM
# =========================================================

class ScenarioType(str, Enum):
    # Economic
    SANCTIONS              = "sanctions"
    SANCTIONS_REMOVAL      = "sanctions_removal"
    SANCTIONS_COALITION    = "sanctions_coalition"
    TRADE_WAR              = "trade_war"
    TRADE_AGREEMENT        = "trade_agreement"
    TRADE_AGREEMENT_COLLAPSE = "trade_agreement_collapse"
    ENERGY_CUTOFF          = "energy_cutoff"
    ENERGY_DIVERSIFICATION = "energy_diversification"
    ENERGY_PRICE_SHOCK     = "energy_price_shock"
    DEBT_CRISIS            = "debt_crisis"
    EXPORT_BAN             = "export_ban"
    GDP_SHOCK              = "gdp_shock"

    # Defense
    CONFLICT_ESCALATION    = "conflict_escalation"
    CONFLICT_DEESCALATION  = "conflict_deescalation"
    MILITARY_INTERVENTION  = "military_intervention"
    NUCLEAR_THREAT         = "nuclear_threat"
    CYBER_ATTACK           = "cyber_attack"
    ARMS_EMBARGO           = "arms_embargo"
    DEFENSE_SPENDING_SURGE = "defense_spending_surge"
    BORDER_CONFLICT        = "border_conflict"

    # Geopolitical
    ALLIANCE_EXIT          = "alliance_exit"
    ALLIANCE_FORMATION     = "alliance_formation"
    ALLIANCE_EXPANSION     = "alliance_expansion"
    DIPLOMATIC_BREAKDOWN   = "diplomatic_breakdown"
    DIPLOMATIC_NORMALIZATION = "diplomatic_normalization"
    BLOC_REALIGNMENT       = "bloc_realignment"
    INTERNATIONAL_ISOLATION = "international_isolation"
    REGIME_CHANGE          = "regime_change"
    REUNIFICATION          = "reunification"

    # Climate
    CLIMATE_DISASTER       = "climate_disaster"
    RESOURCE_SCARCITY      = "resource_scarcity"
    FOOD_SUPPLY_SHOCK      = "food_supply_shock"
    SUPPLY_CHAIN_COLLAPSE  = "supply_chain_collapse"
    CLIMATE_MIGRATION      = "climate_migration"
    ENERGY_TRANSITION      = "energy_transition"

    # Composite
    STATE_FRAGILITY        = "state_fragility"
    POWER_VACUUM           = "power_vacuum"
    HEGEMONY_SHIFT         = "hegemony_shift"
    REGIONAL_DESTABILIZATION = "regional_destabilization"
    GLOBAL_PANDEMIC        = "global_pandemic"

    # Fallback
    UNKNOWN                = "unknown"


# =========================================================
# SCORE DELTA
# =========================================================

class ScoreDelta(BaseModel):
    """
    Projected change to a single score property on a Country node.
    current + delta = projected.  All values clamped to [-1.0, 1.0].
    """
    score_name: str          = Field(description="Neo4j property name, e.g. trade_vulnerability_score")
    current:    float        = Field(description="Current value from graph (0-1)")
    delta:      float        = Field(description="Projected change (-1 to +1)")
    projected:  float        = Field(description="current + delta, clamped to [0,1]")
    direction:  str          = Field(description="increase | decrease | unchanged")
    confidence: float        = Field(default=0.8, ge=0.0, le=1.0)


# =========================================================
# AFFECTED COUNTRY
# =========================================================

class AffectedCountry(BaseModel):
    """
    A country impacted by the scenario, with its score deltas and
    a human-readable summary of how it is affected.
    """
    country:        str                  = Field(description="Country name")
    impact_type:    str                  = Field(
        description="direct | cascade | regional | global"
    )
    severity:       str                  = Field(
        description="critical | high | medium | low"
    )
    score_deltas:   list[ScoreDelta]     = Field(default_factory=list)
    exposure_usd:   float | None         = Field(
        default=None,
        description="Estimated economic exposure in USD where applicable"
    )
    summary:        str                  = Field(
        description="One sentence describing how this country is affected"
    )


# =========================================================
# SCENARIO REQUEST
# =========================================================

class ScenarioRequest(BaseModel):
    """
    Parsed output from the LLM intent classifier.
    Engine receives natural language, LLM produces this.
    """
    scenario_type:  ScenarioType         = Field(description="Classified scenario type")
    actor:          str | None           = Field(
        default=None,
        description="Primary actor country/entity initiating the action"
    )
    target:         str | None           = Field(
        default=None,
        description="Primary target country/entity"
    )
    third_parties:  list[str]            = Field(
        default_factory=list,
        description="Additional countries explicitly mentioned"
    )
    magnitude:      float                = Field(
        default=1.0, ge=0.1, le=2.0,
        description="Intensity multiplier: 0.5=partial, 1.0=full, 1.5=severe, 2.0=extreme"
    )
    year:           int                  = Field(
        default=2024, ge=2020, le=2040,
        description="Reference year for graph reads"
    )
    raw_query:      str                  = Field(description="Original user query")
    extra_params:   dict[str, Any]       = Field(
        default_factory=dict,
        description="Scenario-specific parameters extracted by LLM"
    )


# =========================================================
# CASCADE EFFECT
# =========================================================

class CascadeEffect(BaseModel):
    """
    A second-order or third-order effect triggered by the scenario.
    E.g. energy cutoff → supply chain disruption → GDP contraction.
    """
    mechanism:   str   = Field(description="What causes this cascade")
    affected:    str   = Field(description="Who / what is affected")
    severity:    str   = Field(description="critical | high | medium | low")
    description: str   = Field(description="One sentence explanation")


# =========================================================
# SCENARIO RESULT
# =========================================================

class ScenarioResult(BaseModel):
    """
    Full output of a simulation run.
    Score deltas are projections — never written to the graph.
    """
    scenario_type:       ScenarioType          = Field()
    actor:               str | None            = Field(default=None)
    target:              str | None            = Field(default=None)
    raw_query:           str                   = Field()
    year:                int                   = Field()

    # Narrative
    headline:            str                   = Field(
        description="One-line summary of what happens"
    )
    summary:             str                   = Field(
        description="2-3 paragraph analytical narrative"
    )

    # Quantitative
    affected_countries:  list[AffectedCountry] = Field(default_factory=list)
    cascade_effects:     list[CascadeEffect]   = Field(default_factory=list)
    global_risk_delta:   float | None          = Field(
        default=None,
        description="Projected change to average global_risk_score"
    )

    # Metadata
    data_sources:        list[str]             = Field(
        default_factory=list,
        description="Neo4j relationship types and properties read during simulation"
    )
    confidence:          float                 = Field(
        default=0.75, ge=0.0, le=1.0,
        description="Overall simulation confidence based on data coverage"
    )
    missing_data:        list[str]             = Field(
        default_factory=list,
        description="Score properties that were null during simulation"
    )
    computation_time_ms: float | None          = Field(default=None)


# =========================================================
# API REQUEST / RESPONSE WRAPPERS
# =========================================================

class SimulateRequest(BaseModel):
    """Raw API request from the user."""
    query: str = Field(
        description="Natural language scenario query",
        examples=["What if US sanctions China?",
                  "What if Russia cuts gas to Europe?",
                  "What if Turkey leaves NATO?"]
    )
    year:      int   = Field(default=2024, ge=2020, le=2040)
    magnitude: float = Field(
        default=1.0, ge=0.1, le=2.0,
        description="Override magnitude if user specifies partial/full/severe"
    )


class SimulateResponse(BaseModel):
    """Full API response."""
    request:  SimulateRequest
    parsed:   ScenarioRequest
    result:   ScenarioResult