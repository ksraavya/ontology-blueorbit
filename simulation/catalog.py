from __future__ import annotations

"""
Scenario catalogue: what each scenario type needs, for the structured UI.

`actor` / `target` say what each slot means for that scenario
(None = slot not used). Mirrors the `_empty_result(... requires ...)`
checks in simulation/scenarios/*.
"""

from typing import Any, Optional

from simulation.models import ScenarioType as T

# type: (domain, label, actor meaning, target meaning, target kind, example)
_C: dict[T, tuple[str, str, Optional[str], Optional[str], str, str]] = {
    T.SANCTIONS:              ("economy", "Sanctions", "Sanctioning country", "Sanctioned country", "country", "What if the US sanctions China?"),
    T.SANCTIONS_REMOVAL:      ("economy", "Sanctions lifted", "Country lifting sanctions", "Previously sanctioned country", "country", "What if the US lifts sanctions on Iran?"),
    T.SANCTIONS_COALITION:    ("economy", "Coalition sanctions", "Lead sanctioning country", "Sanctioned country", "country", "What if the US, UK and Japan jointly sanction Russia?"),
    T.TRADE_WAR:              ("economy", "Trade war", "Country raising tariffs", "Trade-war opponent", "country", "What if the US and China enter a trade war?"),
    T.TRADE_AGREEMENT:        ("economy", "Trade agreement", "Joining country", "Bloc or partner (optional)", "bloc", "What if India joins RCEP?"),
    T.TRADE_AGREEMENT_COLLAPSE: ("economy", "Trade deal collapse", "Country A", "Country B / bloc", "country", "What if the US–Mexico trade deal collapses?"),
    T.ENERGY_CUTOFF:          ("economy", "Energy cutoff", "Energy supplier", "Buyer (optional)", "country", "What if Russia cuts gas to Germany?"),
    T.ENERGY_DIVERSIFICATION: ("economy", "Energy diversification", "Diversifying country", "Current main supplier (optional)", "country", "What if Germany diversifies away from Russian gas?"),
    T.ENERGY_PRICE_SHOCK:     ("economy", "Energy price shock", None, None, "none", "What if oil prices spike by 50%?"),
    T.DEBT_CRISIS:            ("economy", "Debt crisis", None, "Country in crisis", "country", "What if Pakistan defaults on its debt?"),
    T.EXPORT_BAN:             ("economy", "Export ban", "Banning country", "Affected importer", "country", "What if China bans rare earth exports to the US?"),
    T.GDP_SHOCK:              ("economy", "GDP shock", None, "Shocked economy", "country", "What if China enters a severe recession?"),
    T.CONFLICT_ESCALATION:    ("defense", "Conflict escalation", "Aggressor (optional)", "Conflict country", "country", "What if Russia escalates the war in Ukraine?"),
    T.CONFLICT_DEESCALATION:  ("defense", "Ceasefire / de-escalation", "Party (optional)", "Conflict country", "country", "What if there is a ceasefire in Ukraine?"),
    T.MILITARY_INTERVENTION:  ("defense", "Military intervention", "Intervening country", "Country intervened in", "country", "What if Turkey intervenes militarily in Syria?"),
    T.NUCLEAR_THREAT:         ("defense", "Nuclear threat", "Threatening country", "Threatened country (optional)", "country", "What if North Korea conducts a nuclear test?"),
    T.CYBER_ATTACK:           ("defense", "Cyber attack", "Attacker (optional)", "Victim country", "country", "What if Russia launches a cyber attack on the US grid?"),
    T.ARMS_EMBARGO:           ("defense", "Arms embargo / supplier cut-off", "Supplier cutting exports (optional)", "Embargoed country (optional)", "country", "What if Russia stops its arms exports?"),
    T.DEFENSE_SPENDING_SURGE: ("defense", "Defense spending surge", "Country increasing spending", None, "none", "What if Japan doubles its defense budget?"),
    T.BORDER_CONFLICT:        ("defense", "Border conflict", "Country A", "Country B", "country", "What if India and China have a border clash?"),
    T.ALLIANCE_EXIT:          ("geopolitics", "Leave alliance", "Leaving country", "Alliance", "alliance", "What if Turkey leaves NATO?"),
    T.ALLIANCE_FORMATION:     ("geopolitics", "New alliance", "Founding country", "Partner (optional)", "country", "What if India and Japan form a defense pact?"),
    T.ALLIANCE_EXPANSION:     ("geopolitics", "Join alliance", "Joining country", "Alliance", "alliance", "What if Ukraine joins NATO?"),
    T.DIPLOMATIC_BREAKDOWN:   ("geopolitics", "Diplomatic breakdown", "Country A", "Country B", "country", "What if India and Pakistan cut diplomatic ties?"),
    T.DIPLOMATIC_NORMALIZATION: ("geopolitics", "Normalisation", "Country A", "Country B", "country", "What if Saudi Arabia normalises relations with Israel?"),
    T.BLOC_REALIGNMENT:       ("geopolitics", "Bloc realignment", "Realigning country", "New partner (optional)", "country", "What if Saudi Arabia realigns with China?"),
    T.INTERNATIONAL_ISOLATION: ("geopolitics", "International isolation", None, "Isolated country", "country", "What if Iran is expelled from international bodies?"),
    T.REGIME_CHANGE:          ("geopolitics", "Regime change", None, "Country", "country", "What if there is a coup in Pakistan?"),
    T.REUNIFICATION:          ("geopolitics", "Reunification", "Territory A", "Territory B", "country", "What if North and South Korea reunify?"),
    T.CLIMATE_DISASTER:       ("climate", "Climate disaster", None, "Hit country", "country", "What if a major earthquake hits Japan?"),
    T.RESOURCE_SCARCITY:      ("climate", "Resource scarcity", None, "Affected country", "country", "What if Egypt faces a severe water shortage?"),
    T.FOOD_SUPPLY_SHOCK:      ("climate", "Food supply shock", "Exporter (optional)", "Affected country (optional)", "country", "What if Ukraine's grain exports stop?"),
    T.SUPPLY_CHAIN_COLLAPSE:  ("climate", "Supply chain collapse", None, "Disrupted hub / country", "country", "What if the Strait of Hormuz closes near Iran?"),
    T.CLIMATE_MIGRATION:      ("climate", "Climate migration", "Origin country", None, "none", "What if climate change displaces millions from Bangladesh?"),
    T.ENERGY_TRANSITION:      ("climate", "Energy transition", "Transitioning country", None, "none", "What if China rapidly phases out coal?"),
    T.STATE_FRAGILITY:        ("composite", "State fragility", None, None, "none", "Which countries are most fragile right now?"),
    T.POWER_VACUUM:           ("composite", "Power vacuum", "Withdrawing power", None, "none", "What if the US withdraws from global engagement?"),
    T.HEGEMONY_SHIFT:         ("composite", "Hegemony shift", "Rising power", "Incumbent power", "country", "What if China overtakes the US as the dominant power?"),
    T.REGIONAL_DESTABILIZATION: ("composite", "Regional destabilisation", None, "Region or lead country", "region", "What if the Middle East destabilises?"),
    T.GLOBAL_PANDEMIC:        ("composite", "Global pandemic", "Origin country (optional)", None, "none", "What if a new global pandemic starts?"),
}


def scenario_catalog(available: list[str]) -> list[dict[str, Any]]:
    out = []
    for t, (domain, label, actor, target, target_kind, example) in _C.items():
        out.append({
            "type": t.value,
            "domain": domain,
            "label": label,
            "actor": actor,
            "target": target,
            "target_kind": target_kind,
            "example": example,
            "available": t.value in available,
        })
    return out
