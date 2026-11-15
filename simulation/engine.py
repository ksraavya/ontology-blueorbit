from __future__ import annotations

"""
Simulation engine.

    query ──► parse (LLM chain ─► rule parser) ──► resolve names against graph
          ──► resolve year against available data ──► deterministic handler
          ──► narrative (LLM chain ─► template)

Only parsing and narrative touch an LLM; every number comes from a handler
reading Neo4j. Each LLM step has a non-LLM fallback (graceful degradation),
so the engine still produces a full result with no API key at all.
"""

import logging
import threading
import time
from typing import Optional

from common import llm
from common.country_resolver import resolve_entity
from simulation.models import (
    ScenarioRequest,
    ScenarioResult,
    ScenarioType,
    SimulateRequest,
    SimulateResponse,
)
from simulation.prompts import (
    INTENT_SYSTEM_PROMPT,
    NARRATIVE_SYSTEM_PROMPT,
    build_narrative_user_prompt,
)
from simulation.registry import get_handler
from simulation.rule_parser import parse_rules, parse_rules_multi

logger = logging.getLogger(__name__)


# =========================================================
# INTENT PARSING
# =========================================================

def _request_from_dict(parsed: dict, raw_query: str, year: int, magnitude: float, provider: str) -> ScenarioRequest:
    try:
        scenario = ScenarioType(str(parsed.get("scenario_type", "unknown")).strip().lower())
    except ValueError:
        scenario = ScenarioType.UNKNOWN
    try:
        mag = float(parsed.get("magnitude", magnitude))
    except (TypeError, ValueError):
        mag = magnitude
    try:
        yr = int(parsed.get("year", year))
    except (TypeError, ValueError):
        yr = year
    third = parsed.get("third_parties") or []
    extra = parsed.get("extra_params") or {}
    return ScenarioRequest(
        scenario_type=scenario,
        actor=parsed.get("actor") or None,
        target=parsed.get("target") or None,
        third_parties=[t for t in third if isinstance(t, str)] if isinstance(third, list) else [],
        magnitude=min(2.0, max(0.1, mag)),
        year=min(2040, max(2020, yr)),
        raw_query=raw_query,
        extra_params=extra if isinstance(extra, dict) else {},
        parsed_by=provider,
    )


def _parse_with_llm(raw_query: str, year: int, magnitude: float) -> Optional[list[ScenarioRequest]]:
    try:
        res = llm.complete_json(INTENT_SYSTEM_PROMPT, raw_query, max_tokens=700)
    except llm.LLMUnavailable as exc:
        logger.info("LLM intent parse unavailable, using rules: %s", exc)
        return None
    data = res.data
    items = data.get("scenarios") if isinstance(data.get("scenarios"), list) else [data]
    reqs = [_request_from_dict(d, raw_query, year, magnitude, res.provider) for d in items[:3] if isinstance(d, dict)]
    return reqs or None


def parse_intents(raw_query: str, year: int = 2024, magnitude: float = 1.0) -> list[ScenarioRequest]:
    """
    LLM first (may return several scenarios for a compound question);
    rule parser if the LLM is unavailable or understood nothing.
    """
    parsed = _parse_with_llm(raw_query, year, magnitude)
    known = [p for p in (parsed or []) if p.scenario_type != ScenarioType.UNKNOWN]
    if not known:
        rules = parse_rules_multi(raw_query, year=year, magnitude=magnitude)
        known = [r for r in rules if r.scenario_type != ScenarioType.UNKNOWN] or (parsed or rules)[:1]
    return [_resolve_names(p) for p in known]


def parse_intent(raw_query: str, year: int = 2024, magnitude: float = 1.0) -> ScenarioRequest:
    """First scenario only (kept for /simulate/parse-only and older callers)."""
    return parse_intents(raw_query, year, magnitude)[0]


def _resolve_names(req: ScenarioRequest) -> ScenarioRequest:
    """Map 'Russia' -> 'Russian Federation' etc. to the spelling stored in Neo4j."""
    req.actor = resolve_entity(req.actor) if req.actor else None
    req.target = resolve_entity(req.target) if req.target else None
    req.third_parties = [resolve_entity(t) or t for t in req.third_parties]
    if isinstance(req.extra_params.get("realignment_target"), str):
        req.extra_params["realignment_target"] = resolve_entity(req.extra_params["realignment_target"])
    return req


def _structured_request(api_request: SimulateRequest) -> ScenarioRequest:
    req = ScenarioRequest(
        scenario_type=api_request.scenario_type or ScenarioType.UNKNOWN,
        actor=api_request.actor or None,
        target=api_request.target or None,
        third_parties=api_request.third_parties,
        magnitude=api_request.magnitude,
        year=api_request.year,
        raw_query=api_request.query or _describe(api_request),
        extra_params=api_request.extra_params,
        parsed_by="structured",
    )
    return _resolve_names(req)


def _describe(r: SimulateRequest) -> str:
    stype = (r.scenario_type.value if r.scenario_type else "scenario").replace("_", " ")
    parts = [p for p in (r.actor, r.target) if p]
    return f"{stype}: {' → '.join(parts)}" if parts else stype


# =========================================================
# YEAR RESOLUTION
# =========================================================
# Trade data (BACI) is published 1-2 years late. Handlers filter edges with
# `r.year = $year`; asking for a year with no edges silently yields all-zero
# deltas. So snap the requested year to the nearest year that has data.

_years_lock = threading.Lock()
_years_cache: tuple[float, list[int]] = (0.0, [])
_YEARS_TTL_S = 600


def available_trade_years() -> list[int]:
    global _years_cache
    loaded_at, years = _years_cache
    if years and time.time() - loaded_at < _YEARS_TTL_S:
        return years
    with _years_lock:
        try:
            from common.db import Neo4jConnection
            rows = Neo4jConnection().run_query(
                "MATCH ()-[r:HAS_TRADE_VOLUME_WITH]->() WHERE r.year IS NOT NULL "
                "RETURN DISTINCT toInteger(r.year) AS year ORDER BY year"
            )
            years = [int(r["year"]) for r in rows if r.get("year") is not None]
        except Exception as exc:
            logger.warning("Could not read available trade years: %s", exc)
            years = []
        _years_cache = (time.time(), years)
        return years


def _resolve_year(req: ScenarioRequest) -> Optional[str]:
    years = available_trade_years()
    if not years or req.year in years:
        return None
    earlier = [y for y in years if y <= req.year]
    chosen = max(earlier) if earlier else min(years)
    chosen = min(2040, max(2020, chosen))
    if chosen == req.year:
        return None
    note = f"No trade data for {req.year}; used {chosen}, the nearest year in the graph."
    req.year = chosen
    return note


# =========================================================
# NARRATIVE
# =========================================================

def generate_narrative(result: ScenarioResult) -> tuple[str, str, str]:
    user_prompt = build_narrative_user_prompt(
        scenario_type=result.scenario_type.value,
        actor=result.actor,
        target=result.target,
        raw_query=result.raw_query,
        affected_countries=[c.model_dump() for c in result.affected_countries],
        cascade_effects=[c.model_dump() for c in result.cascade_effects],
        year=result.year,
    )
    try:
        res = llm.complete_json(NARRATIVE_SYSTEM_PROMPT, user_prompt, max_tokens=700)
        headline = res.data.get("headline") or _default_headline(result)
        summary = res.data.get("summary") or _default_summary(result)
        return str(headline), str(summary), res.provider
    except llm.LLMUnavailable as exc:
        logger.info("LLM narrative unavailable, using template: %s", exc)
        return _default_headline(result), _default_summary(result), "template"


def _default_headline(result: ScenarioResult) -> str:
    actor = result.actor or ""
    target = result.target or ""
    stype = result.scenario_type.value.replace("_", " ")
    if actor and target:
        return f"{stype.title()}: {actor} → {target}"
    if target or actor:
        return f"{stype.title()} — {target or actor}"
    return f"{stype.title()} scenario"


def _default_summary(result: ScenarioResult) -> str:
    """Template brief built only from computed numbers (no LLM)."""
    n = len(result.affected_countries)
    if n == 0:
        reason = "; ".join(result.missing_data) or "no graph data matched this scenario"
        return f"No measurable impact could be computed ({reason})."

    biggest: list[tuple[float, str, str, float]] = []
    for c in result.affected_countries:
        for d in c.score_deltas:
            biggest.append((abs(d.delta), c.country, d.score_name, d.delta))
    biggest.sort(reverse=True)

    if not biggest or biggest[0][0] < 1e-4:
        return (
            f"The graph has no {result.year} data linking these countries for this scenario, "
            f"so every projected change is zero. Check that the relevant pipeline has been loaded "
            f"(see /health) or try another year."
        )

    direct = [c.country for c in result.affected_countries if c.impact_type == "direct"]
    cascade = [c.country for c in result.affected_countries if c.impact_type != "direct"]
    lines = [
        f"{n} countries affected ({len(direct)} directly, {len(cascade)} through cascades), "
        f"based on {result.year} graph data. Confidence {result.confidence:.0%}."
    ]
    if biggest:
        moves = ", ".join(
            f"{country} {name.replace('_score', '').replace('_', ' ')} {delta:+.3f}"
            for _, country, name, delta in biggest[:3]
        )
        lines.append(f"Largest projected moves: {moves}.")
    exposure = sum(c.exposure_usd or 0 for c in result.affected_countries)
    if exposure > 0:
        lines.append(f"Estimated economic exposure ≈ ${exposure / 1e9:,.1f}B.")
    if result.cascade_effects:
        lines.append("Key mechanism: " + result.cascade_effects[0].description)
    return " ".join(lines)


# =========================================================
# UNKNOWN HANDLER
# =========================================================

def _handle_unknown(req: ScenarioRequest, reason: str) -> ScenarioResult:
    return ScenarioResult(
        scenario_type=req.scenario_type,
        actor=req.actor,
        target=req.target,
        raw_query=req.raw_query,
        year=req.year,
        headline="Scenario not understood" if req.scenario_type == ScenarioType.UNKNOWN
                 else f"Scenario '{req.scenario_type.value}' could not run",
        summary=reason,
        confidence=0.0,
        missing_data=[reason],
    )


# =========================================================
# COMPOUND SCENARIOS
# =========================================================

_SEV_RANK = {"low": 0, "medium": 1, "high": 2, "critical": 3}


def _merge_results(results: list[ScenarioResult]) -> ScenarioResult:
    """
    Combine several scenario results into one: per country, deltas on the same
    score are summed (projected value recomputed from the current value and
    clamped to 0..1); severity is the worst of the parts; exposure is summed.
    Each part is also kept in `parts` so the UI can show them separately.
    """
    from common.intelligence.normalization import clamp
    from simulation.models import AffectedCountry, ScoreDelta

    by_country: dict[str, dict] = {}
    for res in results:
        for ac in res.affected_countries:
            slot = by_country.setdefault(ac.country, {"ac": ac, "deltas": {}, "summaries": [], "exposure": 0.0, "sev": ac.severity, "direct": False})
            for d in ac.score_deltas:
                cur = slot["deltas"].get(d.score_name)
                if cur is None:
                    slot["deltas"][d.score_name] = [d.current, d.delta, d.confidence]
                else:
                    cur[1] += d.delta
                    cur[2] = min(cur[2], d.confidence)
            slot["summaries"].append(ac.summary)
            slot["exposure"] += ac.exposure_usd or 0.0
            if _SEV_RANK.get(ac.severity, 0) > _SEV_RANK.get(slot["sev"], 0):
                slot["sev"] = ac.severity
            slot["direct"] = slot["direct"] or ac.impact_type == "direct"

    merged: list[AffectedCountry] = []
    for country, slot in by_country.items():
        deltas = []
        for name, (current, delta, conf) in slot["deltas"].items():
            delta = max(-1.0, min(1.0, delta))
            deltas.append(ScoreDelta(
                score_name=name, current=round(current, 4), delta=round(delta, 4),
                projected=round(clamp(current + delta), 4),
                direction="increase" if delta > 0.001 else "decrease" if delta < -0.001 else "unchanged",
                confidence=conf,
            ))
        merged.append(AffectedCountry(
            country=country,
            impact_type="direct" if slot["direct"] else "cascade",
            severity=slot["sev"],
            score_deltas=deltas,
            exposure_usd=slot["exposure"] or None,
            summary=" ".join(dict.fromkeys(slot["summaries"])),
        ))
    # rank by the SIZE of the changes: opposite-sign deltas must not cancel out
    merged.sort(key=lambda a: sum(abs(d.delta) for d in a.score_deltas), reverse=True)

    first = results[0]
    parts = [{
        "scenario_type": r.scenario_type.value, "actor": r.actor, "target": r.target,
        "headline": r.headline, "countries": len(r.affected_countries), "confidence": r.confidence,
        "missing_data": r.missing_data,
    } for r in results]
    return ScenarioResult(
        scenario_type=first.scenario_type,
        actor=first.actor,
        target=first.target,
        raw_query=first.raw_query,
        year=first.year,
        headline="",
        summary="",
        affected_countries=merged,
        cascade_effects=[c for r in results for c in r.cascade_effects],
        data_sources=list(dict.fromkeys(src for r in results for src in r.data_sources)),
        confidence=min(r.confidence for r in results),
        missing_data=list(dict.fromkeys(m for r in results for m in r.missing_data)),
        parts=parts,
    )


def _run_one(parsed: ScenarioRequest) -> ScenarioResult:
    handler = get_handler(parsed.scenario_type)
    if parsed.scenario_type == ScenarioType.UNKNOWN:
        return _handle_unknown(
            parsed,
            "Could not classify the question. Try naming the action and the countries, "
            "e.g. 'What if the US sanctions China?', or use structured mode.",
        )
    if handler is None:
        return _handle_unknown(parsed, f"No handler registered for {parsed.scenario_type.value}.")
    try:
        return handler(parsed)
    except Exception as exc:
        logger.error("Handler failed: %s", exc, exc_info=True)
        return _handle_unknown(parsed, f"Handler error: {exc}")


# =========================================================
# MAIN ENTRY POINT
# =========================================================

def run_simulation(api_request: SimulateRequest) -> SimulateResponse:
    t0 = time.perf_counter()

    if api_request.scenario_type:
        parsed_list = [_structured_request(api_request)]
    else:
        parsed_list = parse_intents(api_request.query, api_request.year, api_request.magnitude)

    notes: list[str] = []
    for p in parsed_list:
        year_note = _resolve_year(p)
        if year_note and year_note not in notes:
            notes.append(year_note)

    results = [_run_one(p) for p in parsed_list]
    for r in results:
        # Parts that failed for lack of data still explain themselves in the merged view.
        if not r.affected_countries and r.summary:
            notes.append(f"{r.scenario_type.value.replace('_', ' ')}: {r.summary}")
    if len(results) == 1:
        result = results[0]
    else:
        working = [r for r in results if r.affected_countries] or results[:1]
        result = _merge_results(working)
        if len(working) > 1:
            result.headline = ""   # let the narrative describe the combined picture
            result.summary = ""
    parsed = parsed_list[0]

    has_signal = any(
        abs(d.delta) >= 1e-4 for c in result.affected_countries for d in c.score_deltas
    )
    if (not result.headline or not result.summary) and not has_signal:
        # Nothing real to describe: an LLM given all-zero numbers invents a story.
        result.headline = result.headline or _default_headline(result)
        result.summary = result.summary or _default_summary(result)
        result.narrative_by = "template"
    elif not result.headline or not result.summary:
        headline, summary, by = generate_narrative(result)
        result.headline = result.headline or headline
        result.summary = result.summary or summary
        result.narrative_by = by
    elif result.narrative_by is None:
        result.narrative_by = "handler"

    result.notes = notes + result.notes
    result.computation_time_ms = round((time.perf_counter() - t0) * 1000, 1)

    return SimulateResponse(request=api_request, parsed=parsed, result=result, parsed_parts=parsed_list)
