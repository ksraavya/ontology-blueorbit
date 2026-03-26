from __future__ import annotations

from typing import Callable

from simulation.models import ScenarioRequest, ScenarioResult, ScenarioType

# ── Lazy imports so missing scenario files don't crash startup ────────────────

def _load_handlers() -> dict[ScenarioType, Callable[[ScenarioRequest], ScenarioResult]]:
    handlers: dict[ScenarioType, Callable] = {}

    # Economic — sanctions
    try:
        from simulation.scenarios.economy.sanctions import (
            run_sanctions,
            run_sanctions_removal,
            run_sanctions_coalition,
        )
        handlers[ScenarioType.SANCTIONS]           = run_sanctions
        handlers[ScenarioType.SANCTIONS_REMOVAL]   = run_sanctions_removal
        handlers[ScenarioType.SANCTIONS_COALITION] = run_sanctions_coalition
    except ImportError:
        pass

    # Economic — energy
    try:
        from simulation.scenarios.economy.energy import (
            run_energy_cutoff,
            run_energy_price_shock,
        )
        handlers[ScenarioType.ENERGY_CUTOFF]      = run_energy_cutoff
        handlers[ScenarioType.ENERGY_PRICE_SHOCK] = run_energy_price_shock
    except ImportError:
        pass

    # Economic — trade
    try:
        from simulation.scenarios.economy.trade_war import (
            run_trade_war,
            run_trade_agreement,
            run_trade_agreement_collapse,
        )
        handlers[ScenarioType.TRADE_WAR]                  = run_trade_war
        handlers[ScenarioType.TRADE_AGREEMENT]            = run_trade_agreement
        handlers[ScenarioType.TRADE_AGREEMENT_COLLAPSE]   = run_trade_agreement_collapse
    except ImportError:
        pass

    # Economic — GDP / debt / export ban
    try:
        from simulation.scenarios.economy.gdp_shock import (
            run_gdp_shock,
            run_debt_crisis,
            run_export_ban,
        )
        handlers[ScenarioType.GDP_SHOCK]    = run_gdp_shock
        handlers[ScenarioType.DEBT_CRISIS]  = run_debt_crisis
        handlers[ScenarioType.EXPORT_BAN]   = run_export_ban
    except ImportError:
        pass

    # Economic — energy diversification
    try:
        from simulation.scenarios.economy.energy import run_energy_diversification
        handlers[ScenarioType.ENERGY_DIVERSIFICATION] = run_energy_diversification
    except ImportError:
        pass

    # Defense — conflict
    try:
        from simulation.scenarios.defense.conflict import (
            run_conflict_escalation,
            run_conflict_deescalation,
        )
        handlers[ScenarioType.CONFLICT_ESCALATION]   = run_conflict_escalation
        handlers[ScenarioType.CONFLICT_DEESCALATION] = run_conflict_deescalation
    except ImportError:
        pass

    # Defense — arms / spending
    try:
        from simulation.scenarios.defense.arms import (
            run_military_intervention,
            run_arms_embargo,
            run_defense_spending_surge,
            run_border_conflict,
        )
        handlers[ScenarioType.MILITARY_INTERVENTION]   = run_military_intervention
        handlers[ScenarioType.ARMS_EMBARGO]            = run_arms_embargo
        handlers[ScenarioType.DEFENSE_SPENDING_SURGE]  = run_defense_spending_surge
        handlers[ScenarioType.BORDER_CONFLICT]         = run_border_conflict
    except ImportError:
        pass

    # Defense — nuclear / cyber
    try:
        from simulation.scenarios.defense.nuclear import run_nuclear_threat
        handlers[ScenarioType.NUCLEAR_THREAT] = run_nuclear_threat
    except ImportError:
        pass

    try:
        from simulation.scenarios.defense.cyber import run_cyber_attack
        handlers[ScenarioType.CYBER_ATTACK] = run_cyber_attack
    except ImportError:
        pass

    # Geopolitical — alliance
    try:
        from simulation.scenarios.geopolitics.alliance import (
            run_alliance_exit,
            run_alliance_formation,
            run_alliance_expansion,
        )
        handlers[ScenarioType.ALLIANCE_EXIT]      = run_alliance_exit
        handlers[ScenarioType.ALLIANCE_FORMATION] = run_alliance_formation
        handlers[ScenarioType.ALLIANCE_EXPANSION] = run_alliance_expansion
    except ImportError:
        pass

    # Geopolitical — diplomacy
    try:
        from simulation.scenarios.geopolitics.diplomacy import (
            run_diplomatic_breakdown,
            run_diplomatic_normalization,
            run_bloc_realignment,
            run_international_isolation,
        )
        handlers[ScenarioType.DIPLOMATIC_BREAKDOWN]       = run_diplomatic_breakdown
        handlers[ScenarioType.DIPLOMATIC_NORMALIZATION]   = run_diplomatic_normalization
        handlers[ScenarioType.BLOC_REALIGNMENT]           = run_bloc_realignment
        handlers[ScenarioType.INTERNATIONAL_ISOLATION]    = run_international_isolation
    except ImportError:
        pass

    # Geopolitical — governance
    try:
        from simulation.scenarios.geopolitics.governance import (
            run_regime_change,
            run_reunification,
        )
        handlers[ScenarioType.REGIME_CHANGE]  = run_regime_change
        handlers[ScenarioType.REUNIFICATION]  = run_reunification
    except ImportError:
        pass

    # Climate
    try:
        from simulation.scenarios.climate.disaster import run_climate_disaster
        handlers[ScenarioType.CLIMATE_DISASTER] = run_climate_disaster
    except ImportError:
        pass

    try:
        from simulation.scenarios.climate.supply_chain import (
            run_food_supply_shock,
            run_supply_chain_collapse,
            run_resource_scarcity,
            run_climate_migration,
        )
        handlers[ScenarioType.FOOD_SUPPLY_SHOCK]      = run_food_supply_shock
        handlers[ScenarioType.SUPPLY_CHAIN_COLLAPSE]  = run_supply_chain_collapse
        handlers[ScenarioType.RESOURCE_SCARCITY]      = run_resource_scarcity
        handlers[ScenarioType.CLIMATE_MIGRATION]      = run_climate_migration
    except ImportError:
        pass

    try:
        from simulation.scenarios.climate.transition import run_energy_transition
        handlers[ScenarioType.ENERGY_TRANSITION] = run_energy_transition
    except ImportError:
        pass

    # Composite
    try:
        from simulation.scenarios.composite.power_shift import (
            run_state_fragility,
            run_power_vacuum,
            run_hegemony_shift,
        )
        handlers[ScenarioType.STATE_FRAGILITY]  = run_state_fragility
        handlers[ScenarioType.POWER_VACUUM]     = run_power_vacuum
        handlers[ScenarioType.HEGEMONY_SHIFT]   = run_hegemony_shift
    except ImportError:
        pass

    try:
        from simulation.scenarios.composite.regional import (
            run_regional_destabilization,
            run_global_pandemic,
        )
        handlers[ScenarioType.REGIONAL_DESTABILIZATION] = run_regional_destabilization
        handlers[ScenarioType.GLOBAL_PANDEMIC]          = run_global_pandemic
    except ImportError:
        pass

    return handlers


# Singleton — loaded once at startup
_REGISTRY: dict[ScenarioType, Callable] | None = None


def get_handler(scenario_type: ScenarioType) -> Callable[[ScenarioRequest], ScenarioResult] | None:
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = _load_handlers()
    return _REGISTRY.get(scenario_type)


def list_available() -> list[str]:
    """Return all scenario types that have a registered handler."""
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = _load_handlers()
    return sorted(k.value for k in _REGISTRY.keys())