# ===== common/ontology.py =====

from __future__ import annotations

from typing import FrozenSet, Set, Dict


"""
Global Ontology for the Intelligence Engine

This file defines ALL relationship types used across the system.

Key Concepts:
- RAW relationships → directly from ingested data
- DERIVED relationships → computed/analytical relationships

Rules:
- All relationship names are UPPERCASE
- One name per concept (no synonyms)
- Direction must remain consistent across modules
"""


# ==============================
# DEFENSE (RAW)
# ==============================

SPENDS_ON_DEFENSE = "SPENDS_ON_DEFENSE"
EXPORTS_ARMS = "EXPORTS_ARMS"
HAS_CONFLICT_STATS = "HAS_CONFLICT_STATS"
EXPORTS_WEAPON_TO = "EXPORTS_WEAPON_TO"
IMPORTS_WEAPON_FROM = "IMPORTS_WEAPON_FROM"
INVOLVED_IN = "INVOLVED_IN"
PARTICIPATED_IN_EXERCISE = "PARTICIPATED_IN_EXERCISE"
SIGNED_DEFENSE_DEAL = "SIGNED_DEFENSE_DEAL"
HAS_MILITARY_ALLIANCE_WITH = "HAS_MILITARY_ALLIANCE_WITH"

DEFENSE_RELATIONS: FrozenSet[str] = frozenset({
    SPENDS_ON_DEFENSE,
    EXPORTS_ARMS,
    HAS_CONFLICT_STATS,
    EXPORTS_WEAPON_TO,
    IMPORTS_WEAPON_FROM,
    INVOLVED_IN,
    PARTICIPATED_IN_EXERCISE,
    SIGNED_DEFENSE_DEAL,
    HAS_MILITARY_ALLIANCE_WITH,
})


# ==============================
# ECONOMY / TRADE / ENERGY (RAW)
# ==============================

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


# ==============================
# CLIMATE (RAW)
# ==============================

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


# ==============================
# GEOPOLITICS (RAW)
# ==============================

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


# ==============================
# CROSS-DOMAIN (RAW)
# ==============================

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


# ==============================
# DERIVED RELATIONSHIPS
# ==============================

# Economy-derived
HAS_TRADE_DEPENDENCY_ON = "HAS_TRADE_DEPENDENCY_ON"
DEPENDS_ON_ENERGY_FROM = "DEPENDS_ON_ENERGY_FROM"

# Geopolitics-derived
ALIGNED_WITH = "ALIGNED_WITH"
PART_OF_BLOC = "PART_OF_BLOC"

# Meta / analytics
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


# ==============================
# RAW RELATION AGGREGATION
# ==============================

RAW_RELATIONS: FrozenSet[str] = frozenset(
    set().union(
        DEFENSE_RELATIONS,
        ECONOMY_RELATIONS,
        CLIMATE_RELATIONS,
        GEOPOLITICS_RELATIONS,
        CROSS_DOMAIN_RELATIONS,
    )
)


# ==============================
# META RELATIONS (subset of derived)
# ==============================

META_RELATIONS: FrozenSet[str] = frozenset({
    HAS_HIGH_DEPENDENCY_ON,
    IS_MAJOR_EXPORT_PARTNER_OF,
    IS_HIGH_RISK_FOR,
    IS_INFLUENTIAL_TO,
    BELONGS_TO_CLUSTER,
})


# ==============================
# GLOBAL ACCESS STRUCTURES
# ==============================

ALL_RELATIONSHIPS: FrozenSet[str] = frozenset(
    set().union(RAW_RELATIONS, DERIVED_RELATIONS)
)

RELATIONS_BY_DOMAIN: Dict[str, FrozenSet[str]] = {
    "defense": DEFENSE_RELATIONS,
    "economy": ECONOMY_RELATIONS,
    "climate": CLIMATE_RELATIONS,
    "geopolitics": GEOPOLITICS_RELATIONS,
    "cross_domain": CROSS_DOMAIN_RELATIONS,
    "meta": META_RELATIONS,
}


# ==============================
# VALIDATION
# ==============================

def _validate_ontology() -> None:
    """Ensure ontology consistency and prevent silent errors."""

    # RAW and DERIVED must not overlap
    overlap: Set[str] = set(RAW_RELATIONS).intersection(DERIVED_RELATIONS)
    if overlap:
        raise ValueError(f"Duplicate relationships found in RAW and DERIVED: {sorted(overlap)}")

    # ALL must be exact union
    if ALL_RELATIONSHIPS != RAW_RELATIONS.union(DERIVED_RELATIONS):
        raise ValueError("Mismatch in ALL_RELATIONSHIPS definition")


_validate_ontology()


# ==============================
# UTILITY
# ==============================

def is_valid_relationship(rel: str) -> bool:
    """Check if a relationship is part of the ontology."""
    return rel in ALL_RELATIONSHIPS