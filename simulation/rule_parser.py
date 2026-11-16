from __future__ import annotations

"""
Rule-based intent parser: the no-LLM fallback.

Turns "What if Russia cuts gas to Germany?" into the same ScenarioRequest
the LLM would produce, using ordered keyword rules + entity scanning.
It is deliberately simple and predictable: the first matching rule wins,
countries are assigned to actor/target by position and by what the
scenario handler needs. It will not understand every phrasing, but it
covers the common ones, so an exhausted API quota no longer means
"no simulation at all".
"""

import re
from typing import Any, Optional

from common.country_resolver import FoundEntity, find_entities
from simulation.models import ScenarioRequest, ScenarioType as T

_ALLIANCE_WORDS = r"(nato|brics|sco|quad|asean|csto|african union|arab league|alliance)"
_BLOC_WORDS = r"(rcep|cptpp|usmca|mercosur|european union|\beu\b|gcc)"

# Ordered: first match wins. More specific phrasings come before generic ones
# ("lift sanctions" before "sanction", "trade war" before "war").
RULES: list[tuple[T, str]] = [
    (T.SANCTIONS_REMOVAL, r"\b(lift|lifts|lifted|lifting|remove[sd]?|eases?|eased|drops?|ends?)\b.{0,30}\bsanction"),
    (T.SANCTIONS_COALITION, r"\b(coalition|jointly|together|allies|g7)\b.{0,50}\bsanction|\bsanction.{0,50}\b(coalition|jointly)"),
    (T.SANCTIONS, r"\bsanction"),
    (T.ARMS_EMBARGO, r"arms embargo|weapons? (ban|embargo)|stops? (selling|supplying|sending) (arms|weapons)|(stops?|halts?|cuts?|ends?|bans?)\b.{0,25}\b(arms|weapons?)\b.{0,20}\bexports?"),
    (T.EXPORT_BAN, r"export ban|bans? (the )?exports?|export controls?|bans? .{0,25}(chips?|semiconductors?|rare earths?|grain|wheat|lithium|fertili[sz]er)"),
    (T.TRADE_AGREEMENT_COLLAPSE, r"(trade (deal|agreement|pact)|\bfta\b).{0,30}(collapse|ends?|breaks?|terminat|scrap)|(collapse|end|scrap|terminat)\w*.{0,20}(trade deal|trade agreement|\bfta\b)|(leaves?|exits?|quits?) " + _BLOC_WORDS),
    (T.TRADE_AGREEMENT, r"(joins?|signs?|enters?) .{0,25}(" + _BLOC_WORDS + r"|trade (deal|agreement|pact)|\bfta\b|free trade)|free trade agreement"),
    (T.TRADE_WAR, r"trade war|tariff"),
    (T.ENERGY_CUTOFF, r"(cuts?|cut off|halts?|stops?|shuts? off|turns? off|blocks?|suspends?)\b.{0,25}\b(gas|oil|energy|lng|pipeline|crude)|(gas|oil|energy) (cut ?off|embargo)"),
    (T.ENERGY_DIVERSIFICATION, r"diversif|(reduces?|ends?|weans?).{0,25}(dependence|reliance) on"),
    (T.ENERGY_PRICE_SHOCK, r"(oil|gas|energy) price|price (spike|shock|crash)|oil (spike|shock|crash)"),
    (T.DEBT_CRISIS, r"\bdefaults?\b|debt crisis|currency (collapse|crisis)|bail ?out|\bimf\b"),
    (T.GDP_SHOCK, r"recession|\bgdp\b|economic (collapse|crash|contraction|boom|slowdown)|depression|economy (collapses|crashes|booms)"),
    (T.NUCLEAR_THREAT, r"nuclear|atomic|\bnukes?\b"),
    (T.CYBER_ATTACK, r"cyber|\bhack"),
    (T.MILITARY_INTERVENTION, r"interven|sends? troops|deploys? (troops|forces)|military (operation|intervention)"),
    (T.BORDER_CONFLICT, r"border (clash|conflict|skirmish|dispute|war)|territorial dispute"),
    (T.CONFLICT_DEESCALATION, r"ceasefire|cease-fire|peace (deal|treaty|agreement|talks)|de-?escalat|truce|armistice|ends? the war"),
    (T.SUPPLY_CHAIN_COLLAPSE, r"supply chain|\bstrait\b|\bcanal\b|shipping (route|lane)|port (closure|shut)|chokepoint"),
    (T.ALLIANCE_EXIT, r"(leaves?|exits?|quits?|withdraws? from|pulls? out of) .{0,15}" + _ALLIANCE_WORDS),
    (T.ALLIANCE_EXPANSION, r"(joins?|admitted to|enters?|becomes? a member of) .{0,15}" + _ALLIANCE_WORDS),
    (T.CONFLICT_ESCALATION, r"invade|invasion|\bwar\b|attack|escalat|conflict|air ?strikes?|bomb|blockade"),
    (T.DEFENSE_SPENDING_SURGE, r"(defen[cs]e|military) (spending|budget)|rearm|doubles? .{0,15}(military|defen[cs]e)"),
    (T.ALLIANCE_FORMATION, r"(forms?|creates?|new) .{0,20}alliance|military pact|defen[cs]e pact"),
    (T.DIPLOMATIC_NORMALIZATION, r"normali[sz]|restores? (diplomatic )?(ties|relations)|rapprochement|reconcil|peace with"),
    (T.DIPLOMATIC_BREAKDOWN, r"(cuts?|severs?|breaks?|downgrades?) .{0,20}(ties|relations)|expels? .{0,15}(diplomats|ambassador)|recalls? .{0,15}ambassador"),
    (T.BLOC_REALIGNMENT, r"realign|switch(es)? sides|pivots? (to|towards)|aligns? (itself )?(with|towards)"),
    (T.INTERNATIONAL_ISOLATION, r"isolat|expelled from|suspended from|kicked out"),
    (T.REUNIFICATION, r"reunif|\bunif(y|ies|ication)\b"),
    (T.REGIME_CHANGE, r"regime change|\bcoup\b|revolution|government (falls|collapses|is overthrown)|toppled|overthrow"),
    (T.CLIMATE_DISASTER, r"earthquake|flood|hurricane|cyclone|typhoon|tsunami|drought|wildfire|heat ?wave|disaster|volcan"),
    (T.FOOD_SUPPLY_SHOCK, r"food (supply|shortage|crisis|price)|famine|grain (shortage|crisis)|crop failure"),
    (T.RESOURCE_SCARCITY, r"(water|resource) (shortage|scarcity|crisis)|scarcity|shortage of"),
    (T.CLIMATE_MIGRATION, r"migration|refugees?|displace"),
    (T.ENERGY_TRANSITION, r"energy transition|renewable|net[ -]zero|green energy|phases? out (coal|fossil)|decarboni"),
    (T.GLOBAL_PANDEMIC, r"pandemic|epidemic|virus|outbreak|covid"),
    (T.POWER_VACUUM, r"power vacuum|isolationis|retreats? from|disengage|withdraws? from (the world|global)"),
    (T.HEGEMONY_SHIFT, r"hegemon|superpower|overtakes?|world order|dominant power"),
    (T.REGIONAL_DESTABILIZATION, r"destabili|regional (crisis|instability|war)|unrest across"),
    (T.STATE_FRAGILITY, r"fragil|most at risk|most vulnerable|failed state|collapse risk|unstable|instability"),
]
_COMPILED = [(t, re.compile(p)) for t, p in RULES]

# Which slot a lone country should fill, per scenario (mirrors each handler's
# `_empty_result(req, "... requires ...")` check).
_NEEDS_TARGET = {
    T.CLIMATE_DISASTER, T.SUPPLY_CHAIN_COLLAPSE, T.RESOURCE_SCARCITY,
    T.REGIONAL_DESTABILIZATION, T.ARMS_EMBARGO, T.CONFLICT_ESCALATION,
    T.CONFLICT_DEESCALATION, T.CYBER_ATTACK, T.GDP_SHOCK, T.DEBT_CRISIS,
    T.INTERNATIONAL_ISOLATION, T.REGIME_CHANGE, T.FOOD_SUPPLY_SHOCK,
}
_NEEDS_ACTOR = {
    T.CLIMATE_MIGRATION, T.ENERGY_TRANSITION, T.DEFENSE_SPENDING_SURGE,
    T.NUCLEAR_THREAT, T.ENERGY_CUTOFF, T.TRADE_AGREEMENT, T.ALLIANCE_FORMATION,
    T.BLOC_REALIGNMENT, T.ALLIANCE_EXIT, T.ALLIANCE_EXPANSION, T.POWER_VACUUM,
    T.ENERGY_DIVERSIFICATION, T.TRADE_AGREEMENT_COLLAPSE,
}
_ALLIANCE_TARGET = {T.ALLIANCE_EXIT, T.ALLIANCE_EXPANSION}

_MAGNITUDE_WORDS: list[tuple[float, str]] = [
    (2.0, r"\b(extreme|catastrophic|all-out|total war|devastating)\b"),
    (1.5, r"\b(severe|major|significant|massive|large-scale|heavy)\b"),
    (0.5, r"\b(partial|partially|limited|minor|small|slight|targeted)\b"),
    (1.0, r"\b(full|fully|complete|completely|total)\b"),
]


def _classify(q: str) -> Optional[T]:
    for scenario, pat in _COMPILED:
        if pat.search(q):
            return scenario
    return None


def _magnitude(q: str, default: float) -> float:
    for value, pat in _MAGNITUDE_WORDS:
        if re.search(pat, q):
            return value
    return default


def _year(q: str, default: int) -> int:
    m = re.search(r"\b(20[2-4]\d)\b", q)
    return int(m.group(1)) if m else default


def _extras(scenario: T, q: str, entities: list[FoundEntity]) -> dict[str, Any]:
    extra: dict[str, Any] = {}
    commodity = re.search(r"\b(gas|oil|lng|crude|coal|chips?|semiconductors?|rare earths?|grain|wheat|lithium|fertili[sz]ers?)\b", q)
    if scenario in (T.ENERGY_CUTOFF, T.EXPORT_BAN) and commodity:
        extra["commodity"] = commodity.group(1)
    if scenario == T.CLIMATE_DISASTER:
        d = re.search(r"(earthquake|flood|hurricane|cyclone|typhoon|tsunami|drought|wildfire|heat ?wave|volcan\w*)", q)
        if d:
            extra["disaster_type"] = d.group(1)
    if scenario == T.GDP_SHOCK and re.search(r"\b(boom|growth|grows|surge)\b", q):
        extra["direction"] = "growth"
    if scenario == T.NUCLEAR_THREAT:
        extra["threat_type"] = "nuclear_test" if re.search(r"\btest", q) else "doctrine_change"
    if scenario == T.CYBER_ATTACK:
        s = re.search(r"\b(grid|power|energy|financial|banks?|telecom|military|infrastructure)\b", q)
        if s:
            extra["sector"] = s.group(1)
    blocs = [e for e in entities if e.kind == "alliance"]
    if scenario in (T.TRADE_AGREEMENT, T.TRADE_AGREEMENT_COLLAPSE) and blocs:
        extra["agreement_name"] = blocs[0].name
    regions = [e for e in entities if e.kind == "region"]
    if scenario == T.REGIONAL_DESTABILIZATION and regions:
        extra["region"] = regions[0].name
    if scenario in (T.ALLIANCE_EXIT, T.BLOC_REALIGNMENT):
        m = re.search(r"(realign|aligns?|pivots?)\w* (itself )?(with|towards|to) ", q)
        if m:
            after = [e for e in entities if e.kind == "country" and e.start > m.start()]
            if after:
                extra["realignment_target"] = after[0].name
    return extra


def _assign_roles(scenario: T, q: str, entities: list[FoundEntity]) -> tuple[Optional[str], Optional[str], list[str]]:
    countries = [e for e in entities if e.kind == "country"]
    alliances = [e for e in entities if e.kind == "alliance"]
    regions = [e for e in entities if e.kind == "region"]

    # Passive voice: "China is sanctioned by the US" -> actor = entity after "by".
    by = re.search(r"\bby\b", q)
    if by and len(countries) >= 2:
        after = [c for c in countries if c.start > by.start()]
        before = [c for c in countries if c.start < by.start()]
        if after and before:
            countries = [after[0], before[0]] + [c for c in countries if c not in (after[0], before[0])]

    names = [c.name for c in countries]

    if scenario in _ALLIANCE_TARGET:
        actor = names[0] if names else None
        target = alliances[0].name if alliances else None
        return actor, target, names[1:]

    if scenario == T.SANCTIONS_COALITION:
        # Target follows "on"/"against"; otherwise it is the last country named.
        on = re.search(r"\b(on|against)\b", q)
        tgt = None
        if on:
            after = [c for c in countries if c.start > on.start()]
            tgt = after[0].name if after else None
        tgt = tgt or (names[-1] if names else None)
        actors = [n for n in names if n != tgt] + [a.name for a in alliances]
        return (actors[0] if actors else None), tgt, actors[1:]

    if scenario == T.ARMS_EMBARGO and re.search(r"(stops?|halts?|cuts?|ends?|bans?)\b.{0,25}\b(arms|weapons?)\b.{0,20}\bexports?", q) and names:
        # "Russia stops its arms exports" = supplier cut-off: the country is the actor
        return names[0], (names[1] if len(names) > 1 else None), names[2:]

    if scenario in (T.TRADE_AGREEMENT, T.TRADE_AGREEMENT_COLLAPSE):
        actor = names[0] if names else None
        target = alliances[0].name if alliances else (names[1] if len(names) > 1 else None)
        return actor, target, names[1:] if alliances else names[2:]

    if scenario == T.REGIONAL_DESTABILIZATION:
        target = regions[0].name if regions else (names[0] if names else None)
        return None, target, names if regions else names[1:]

    if len(names) >= 2:
        return names[0], names[1], names[2:]
    if len(names) == 1:
        if scenario in _NEEDS_ACTOR:
            return names[0], None, []
        return None, names[0], []
    return None, None, []


def parse_rules(raw_query: str, year: int = 2024, magnitude: float = 1.0) -> ScenarioRequest:
    """Parse without any LLM. Returns UNKNOWN when no rule matches."""
    q = raw_query.casefold()
    scenario = _classify(q) or T.UNKNOWN
    entities = find_entities(raw_query)
    actor, target, third = _assign_roles(scenario, q, entities) if scenario != T.UNKNOWN else (None, None, [])
    return ScenarioRequest(
        scenario_type=scenario,
        actor=actor,
        target=target,
        third_parties=third,
        magnitude=_magnitude(q, magnitude),
        year=_year(q, year),
        raw_query=raw_query,
        extra_params=_extras(scenario, q, entities),
        parsed_by="rules",
    )


def parse_rules_multi(raw_query: str, year: int = 2024, magnitude: float = 1.0) -> list[ScenarioRequest]:
    """
    Compound questions without an LLM: split on "and" / ";" / "also" and keep
    each part that is a scenario on its own. Falls back to one parse.
    """
    parts = [p.strip() for p in re.split(r"\band\b|;|\balso\b|\bwhile\b", raw_query) if p.strip()]
    if len(parts) > 1:
        reqs = [parse_rules(p, year=year, magnitude=magnitude) for p in parts]
        good = [q for q in reqs if q.scenario_type != T.UNKNOWN and (q.actor or q.target or q.scenario_type in (T.STATE_FRAGILITY, T.GLOBAL_PANDEMIC, T.ENERGY_PRICE_SHOCK))]
        if len(good) > 1:
            for q in good:
                q.raw_query = raw_query
            return good[:3]
    return [parse_rules(raw_query, year=year, magnitude=magnitude)]
