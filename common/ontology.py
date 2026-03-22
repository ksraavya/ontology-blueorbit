from __future__ import annotations
from typing import FrozenSet, Dict, Set, List


"""
Global Ontology for the Intelligence Engine

Defines:
1. All relationships (RAW + DERIVED)
2. Domain grouping
3. Relation metadata (behavior + effects)
4. Validation utilities

Design Principles:
- One relation = one meaning
- Metadata is LIGHT (no formulas)
- Intelligence layer uses metadata to decide behavior
"""


# =========================================================
# DEFENSE (RAW)
# =========================================================

SPENDS_ON_DEFENSE = "SPENDS_ON_DEFENSE"
EXPORTS_WEAPON_TO = "EXPORTS_WEAPON_TO"
IMPORTS_WEAPON_FROM = "IMPORTS_WEAPON_FROM"
INVOLVED_IN = "INVOLVED_IN"
PARTICIPATED_IN_EXERCISE = "PARTICIPATED_IN_EXERCISE"
SIGNED_DEFENSE_DEAL = "SIGNED_DEFENSE_DEAL"
HAS_MILITARY_ALLIANCE_WITH = "HAS_MILITARY_ALLIANCE_WITH"

DEFENSE_RELATIONS: FrozenSet[str] = frozenset({
    SPENDS_ON_DEFENSE,
    EXPORTS_WEAPON_TO,
    IMPORTS_WEAPON_FROM,
    INVOLVED_IN,
    PARTICIPATED_IN_EXERCISE,
    SIGNED_DEFENSE_DEAL,
    HAS_MILITARY_ALLIANCE_WITH,
})


# =========================================================
# ECONOMY / TRADE / ENERGY (RAW)
# =========================================================

EXPORTS_TO = "EXPORTS_TO"
IMPORTS_FROM = "IMPORTS_FROM"
HAS_GDP = "HAS_GDP"
HAS_INFLATION = "HAS_INFLATION"
HAS_TRADE_BALANCE = "HAS_TRADE_BALANCE"
HAS_TRADE_VOLUME_WITH = "HAS_TRADE_VOLUME_WITH"

EXPORTS_ENERGY_TO = "EXPORTS_ENERGY_TO"
IMPORTS_ENERGY_FROM = "IMPORTS_ENERGY_FROM"

ECONOMY_RELATIONS: FrozenSet[str] = frozenset({
    EXPORTS_TO,
    IMPORTS_FROM,
    HAS_GDP,
    HAS_INFLATION,
    HAS_TRADE_BALANCE,
    HAS_TRADE_VOLUME_WITH,
    EXPORTS_ENERGY_TO,
    IMPORTS_ENERGY_FROM,
})


# =========================================================
# CLIMATE (RAW)
# =========================================================

EXPERIENCED = "EXPERIENCED"
AFFECTED_BY = "AFFECTED_BY"
CAUSED_DAMAGE = "CAUSED_DAMAGE"
RESULTED_IN_FATALITIES = "RESULTED_IN_FATALITIES"
EMITS = "EMITS"
HAS_RESOURCE_STRESS = "HAS_RESOURCE_STRESS"
DEPENDS_ON_RESOURCE = "DEPENDS_ON_RESOURCE"

CLIMATE_RELATIONS: FrozenSet[str] = frozenset({
    EXPERIENCED,
    AFFECTED_BY,
    CAUSED_DAMAGE,
    RESULTED_IN_FATALITIES,
    EMITS,
    HAS_RESOURCE_STRESS,
    DEPENDS_ON_RESOURCE,
})


# =========================================================
# GEOPOLITICS (RAW)
# =========================================================

HAS_POLITICAL_SYSTEM = "HAS_POLITICAL_SYSTEM"
DIPLOMATIC_INTERACTION = "DIPLOMATIC_INTERACTION"
HAS_DIPLOMATIC_TIES_WITH = "HAS_DIPLOMATIC_TIES_WITH"
MEMBER_OF = "MEMBER_OF"
OPPOSES = "OPPOSES"

GEOPOLITICS_RELATIONS: FrozenSet[str] = frozenset({
    HAS_POLITICAL_SYSTEM,
    DIPLOMATIC_INTERACTION,
    HAS_DIPLOMATIC_TIES_WITH,
    MEMBER_OF,
    OPPOSES,
})


# =========================================================
# CROSS-DOMAIN (RAW)
# =========================================================

FUNDS_DEFENSE = "FUNDS_DEFENSE"
DEPENDS_ON_FOR_DEFENSE_SUPPLY = "DEPENDS_ON_FOR_DEFENSE_SUPPLY"

AFFECTS_ECONOMY = "AFFECTS_ECONOMY"
DISRUPTS_SUPPLY_CHAIN = "DISRUPTS_SUPPLY_CHAIN"

INCREASES_CONFLICT_RISK = "INCREASES_CONFLICT_RISK"

HAS_TRADE_AGREEMENT_WITH = "HAS_TRADE_AGREEMENT_WITH"
IMPOSED_SANCTIONS_ON = "IMPOSED_SANCTIONS_ON"

STRATEGIC_PARTNER_OF = "STRATEGIC_PARTNER_OF"
HAS_SECURITY_COOPERATION_WITH = "HAS_SECURITY_COOPERATION_WITH"

CROSS_DOMAIN_RELATIONS: FrozenSet[str] = frozenset({
    FUNDS_DEFENSE,
    DEPENDS_ON_FOR_DEFENSE_SUPPLY,
    AFFECTS_ECONOMY,
    DISRUPTS_SUPPLY_CHAIN,
    INCREASES_CONFLICT_RISK,
    HAS_TRADE_AGREEMENT_WITH,
    IMPOSED_SANCTIONS_ON,
    STRATEGIC_PARTNER_OF,
    HAS_SECURITY_COOPERATION_WITH,
})


# =========================================================
# DERIVED RELATIONS
# =========================================================

HAS_TRADE_DEPENDENCY_ON = "HAS_TRADE_DEPENDENCY_ON"
DEPENDS_ON_ENERGY_FROM = "DEPENDS_ON_ENERGY_FROM"

ALIGNED_WITH = "ALIGNED_WITH"
PART_OF_BLOC = "PART_OF_BLOC"

HAS_HIGH_DEPENDENCY_ON = "HAS_HIGH_DEPENDENCY_ON"
IS_MAJOR_EXPORT_PARTNER_OF = "IS_MAJOR_EXPORT_PARTNER_OF"
IS_HIGH_RISK_FOR = "IS_HIGH_RISK_FOR"
IS_INFLUENTIAL_TO = "IS_INFLUENTIAL_TO"
BELONGS_TO_CLUSTER = "BELONGS_TO_CLUSTER"

DERIVED_RELATIONS: FrozenSet[str] = frozenset({
    HAS_TRADE_DEPENDENCY_ON,
    DEPENDS_ON_ENERGY_FROM,
    ALIGNED_WITH,
    PART_OF_BLOC,
    HAS_HIGH_DEPENDENCY_ON,
    IS_MAJOR_EXPORT_PARTNER_OF,
    IS_HIGH_RISK_FOR,
    IS_INFLUENTIAL_TO,
    BELONGS_TO_CLUSTER,
})


# =========================================================
# AGGREGATIONS
# =========================================================

RAW_RELATIONS: FrozenSet[str] = frozenset(
    set().union(
        DEFENSE_RELATIONS,
        ECONOMY_RELATIONS,
        CLIMATE_RELATIONS,
        GEOPOLITICS_RELATIONS,
        CROSS_DOMAIN_RELATIONS,
    )
)

ALL_RELATIONSHIPS: FrozenSet[str] = frozenset(
    RAW_RELATIONS.union(DERIVED_RELATIONS)
)


# =========================================================
# 🔥 RELATION METADATA (CORE INTELLIGENCE HOOK)
# =========================================================

RELATION_METADATA: Dict[str, Dict] = {

    # ---------- FLOW ----------
    EXPORTS_TO: {"type": "flow", "affects": ["trade", "dependency", "influence"]},
    IMPORTS_FROM: {"type": "flow", "affects": ["trade", "dependency"]},
    EXPORTS_WEAPON_TO: {"type": "flow", "affects": ["defense", "influence"]},
    IMPORTS_WEAPON_FROM: {"type": "flow", "affects": ["defense", "dependency"]},
    EXPORTS_ENERGY_TO: {"type": "flow", "affects": ["energy", "influence"]},
    IMPORTS_ENERGY_FROM: {"type": "flow", "affects": ["energy", "dependency"]},
    DEPENDS_ON_RESOURCE: {"type": "flow", "affects": ["resource", "dependency"]},

    # ---------- INTERACTIONS ----------
    DIPLOMATIC_INTERACTION: {"type": "interaction", "affects": ["alignment"]},
    PARTICIPATED_IN_EXERCISE: {"type": "interaction", "affects": ["defense_cooperation"]},
    SIGNED_DEFENSE_DEAL: {"type": "interaction", "affects": ["defense_cooperation"]},

    # ---------- RELATIONSHIPS ----------
    HAS_MILITARY_ALLIANCE_WITH: {"type": "relationship", "affects": ["alignment"]},
    HAS_DIPLOMATIC_TIES_WITH: {"type": "relationship", "affects": ["alignment"]},
    STRATEGIC_PARTNER_OF: {"type": "relationship", "affects": ["alignment", "influence"]},
    HAS_SECURITY_COOPERATION_WITH: {"type": "relationship", "affects": ["defense"]},
    HAS_TRADE_AGREEMENT_WITH: {"type": "relationship", "affects": ["trade", "alignment"]},
    IMPOSED_SANCTIONS_ON: {"type": "risk", "affects": ["trade", "economic_risk", "alignment"]},

    # ---------- STATE ----------
    HAS_GDP: {"type": "state", "affects": ["economic_power"]},
    HAS_INFLATION: {"type": "state", "affects": ["economic_stability"]},
    HAS_TRADE_BALANCE: {"type": "state", "affects": ["economic_health"]},
    SPENDS_ON_DEFENSE: {"type": "state", "affects": ["military_strength"]},
    EMITS: {"type": "state", "affects": ["climate_pressure"]},
    HAS_RESOURCE_STRESS: {"type": "state", "affects": ["climate_vulnerability"]},

    # ---------- EVENTS ----------
    EXPERIENCED: {"type": "event", "affects": ["risk"]},
    AFFECTED_BY: {"type": "event", "affects": ["risk"]},
    CAUSED_DAMAGE: {"type": "event", "affects": ["risk"]},
    RESULTED_IN_FATALITIES: {"type": "event", "affects": ["risk"]},

    # ---------- RISK ----------
    DISRUPTS_SUPPLY_CHAIN: {"type": "risk", "affects": ["economic_risk"]},
    INCREASES_CONFLICT_RISK: {"type": "risk", "affects": ["global_risk"]},
    AFFECTS_ECONOMY: {"type": "risk", "affects": ["economic_risk"]},
}


# =========================================================
# HELPERS
# =========================================================

def is_valid_relationship(rel: str) -> bool:
    return rel in ALL_RELATIONSHIPS


def get_relation_type(rel: str) -> str:
    return RELATION_METADATA.get(rel, {}).get("type", "unknown")


def get_relation_effects(rel: str) -> List[str]:
    return RELATION_METADATA.get(rel, {}).get("affects", [])


# =========================================================
# VALIDATION
# =========================================================

def _validate_ontology() -> None:
    overlap: Set[str] = RAW_RELATIONS.intersection(DERIVED_RELATIONS)
    if overlap:
        raise ValueError(f"Overlap in RAW and DERIVED relations: {overlap}")

_validate_ontology()