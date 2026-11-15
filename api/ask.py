from __future__ import annotations

"""
POST /graph/ask  {"question": "..."}

Natural-language questions answered from the graph, safely.

The LLM never writes Cypher. It picks ONE of a few parameterised,
read-only query templates (a "tool") and its arguments; the backend
validates the arguments against allowlists, runs the query, and the LLM
then summarises ONLY the returned rows. Without an LLM key a small rule
fallback still handles the common shapes.

    question -> LLM picks {tool, args} -> validate -> Cypher template -> rows
             -> LLM writes answer from rows -> {answer, rows, columns, query}

Why: the previous router chose among ~15 coarse REST endpoints, so
"which country imports the most arms from the USA" got the exporters
ranking and the model invented an answer over unrelated rows.
"""

import json
import re
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from common import llm
from common.country_resolver import resolve_country
from common.db import Neo4jConnection

router = APIRouter(prefix="/graph", tags=["graph ask"])

# ── Metrics a ranking can use ────────────────────────────────────────────────
# Node properties (scores + IMF/World Bank indicators) ...
NODE_METRICS: dict[str, str] = {
    "global_risk_score": "Global risk score (0-1)",
    "overall_vulnerability_score": "Overall vulnerability (0-1)",
    "strategic_influence_score": "Strategic influence (0-1)",
    "conflict_risk_score": "Conflict risk (0-1)",
    "trade_vulnerability_score": "Trade vulnerability (0-1)",
    "energy_vulnerability_score": "Energy vulnerability (0-1)",
    "economic_influence_score": "Economic influence (0-1)",
    "military_strength_score": "Military strength (0-1)",
    "diplomatic_centrality_score": "Diplomatic centrality (0-1)",
    "climate_risk_score": "Climate risk (0-1)",
    "gdp_usd_bn": "GDP (billion USD, IMF)",
    "gdp_per_capita_usd": "GDP per capita (USD, IMF)",
    "gdp_growth_pct": "Real GDP growth (%, IMF)",
    "inflation_pct": "Inflation (%, IMF)",
    "gov_debt_pct_gdp": "Government debt (% of GDP, IMF)",
    "unemployment_pct": "Unemployment (%, IMF)",
    "population_m": "Population (millions, IMF)",
    "current_account_pct_gdp": "Current account (% of GDP, IMF)",
    "trade_pct_gdp": "Trade (% of GDP, World Bank)",
    "fdi_inflow_pct_gdp": "FDI inflows (% of GDP, World Bank)",
    "reserves_months_imports": "Reserves (months of imports, World Bank)",
    "military_exp_pct_gdp": "Military spending (% of GDP, World Bank/SIPRI)",
    "armed_forces_personnel": "Armed forces personnel (World Bank)",
    "arms_imports_tiv": "Arms imports (SIPRI TIV, World Bank)",
    "arms_exports_tiv": "Arms exports (SIPRI TIV, World Bank)",
    "political_stability": "Political stability (WGI, -2.5..2.5)",
    "rule_of_law": "Rule of law (WGI)",
    "control_of_corruption": "Control of corruption (WGI)",
    "gov_effectiveness": "Government effectiveness (WGI)",
    "voice_accountability": "Voice and accountability (WGI)",
    "regulatory_quality": "Regulatory quality (WGI)",
    "energy_imports_pct_use": "Net energy imports (% of energy use, World Bank)",
    "renewable_share_pct": "Renewable energy share (%, World Bank)",
    "water_stress_pct": "Water stress (% of freshwater withdrawn, World Bank)",
}
# ... and totals summed over relationships for the latest year.
_LATEST = "MATCH ()-[x:{rel}]->() WITH max(x.year) AS latest "
REL_METRICS: dict[str, tuple[str, str]] = {
    "exports_usd": ("Total exports (USD, BACI)", _LATEST.format(rel="EXPORTS_TO") +
                    "MATCH (c:Country)-[r:EXPORTS_TO]->(:Country) WHERE r.year = latest WITH c, sum(r.value) AS value, latest"),
    "imports_usd": ("Total imports (USD, BACI)", _LATEST.format(rel="EXPORTS_TO") +
                    "MATCH (:Country)-[r:EXPORTS_TO]->(c:Country) WHERE r.year = latest WITH c, sum(r.value) AS value, latest"),
    "energy_imports_usd": ("Energy imports (USD, BACI HS 27)", _LATEST.format(rel="IMPORTS_ENERGY_FROM") +
                           "MATCH (c:Country)-[r:IMPORTS_ENERGY_FROM]->(:Country) WHERE r.year = latest WITH c, sum(r.value) AS value, latest"),
    "energy_exports_usd": ("Energy exports (USD, BACI HS 27)", _LATEST.format(rel="IMPORTS_ENERGY_FROM") +
                           "MATCH (:Country)-[r:IMPORTS_ENERGY_FROM]->(c:Country) WHERE r.year = latest WITH c, sum(r.value) AS value, latest"),
    "arms_imports_usd": ("Arms imports (USD, customs)", _LATEST.format(rel="TRADES_ARMS_WITH") +
                         "MATCH (:Country)-[r:TRADES_ARMS_WITH]->(c:Country) WHERE r.year = latest WITH c, sum(r.value) AS value, latest"),
    "arms_exports_usd": ("Arms exports (USD, customs)", _LATEST.format(rel="TRADES_ARMS_WITH") +
                         "MATCH (c:Country)-[r:TRADES_ARMS_WITH]->(:Country) WHERE r.year = latest WITH c, sum(r.value) AS value, latest"),
    "defense_spending_usd_m": ("Defence spending (USD millions, SIPRI)",
                               "MATCH (:Country)-[x:SPENDS_ON_DEFENSE]->(y0:Year) WITH max(y0.year) AS latest "
                               "MATCH (c:Country)-[r:SPENDS_ON_DEFENSE]->(y:Year {year: latest}) "
                               "WITH c, coalesce(r.amount_usd_millions, r.value) AS value, latest"),
    "conflict_deaths": ("Conflict fatalities (ACLED, latest year)",
                        "MATCH (:Country)-[x:HAS_CONFLICT_STATS]->(y0:Year) WITH max(y0.year) AS latest "
                        "MATCH (c:Country)-[r:HAS_CONFLICT_STATS]->(y:Year {year: latest}) WITH c, r.total_fatalities AS value, latest"),
    "sanctioned_entities": ("Entities sanctioned by the US (OFAC)",
                            "MATCH (:Country)-[r:IMPOSED_SANCTIONS_ON]->(c:Country) WITH c, r.value AS value, null AS latest"),
}

# ── Bilateral relations (for "partners" and "between") ───────────────────────
# relation -> (rel type, out-direction meaning, value expr, latest-year?, unit)
RELATIONS: dict[str, tuple[str, str, str, bool, str]] = {
    "trade":      ("EXPORTS_TO", "exports to", "r.value", True, "USD"),
    "energy":     ("EXPORTS_ENERGY_TO", "supplies energy to", "r.value", True, "USD"),
    "arms_trade": ("TRADES_ARMS_WITH", "sells arms to", "r.value", True, "USD"),
    "sanctions":  ("IMPOSED_SANCTIONS_ON", "sanctions", "r.value", False, "designated entities"),
    "dependency": ("HAS_TRADE_DEPENDENCY_ON", "depends on (trade share)", "r.dependency", True, "share"),
    "alignment":  ("ALIGNED_WITH", "votes with at the UN", "r.vote_similarity", False, "similarity"),
    "agreements": ("HAS_TRADE_AGREEMENT_WITH", "has a trade agreement with", "1.0", False, "agreement"),
    "diplomacy":  ("DIPLOMATIC_INTERACTION", "has diplomatic events with", "1.0", False, "events"),
    "opposition": ("OPPOSES", "opposes", "coalesce(r.normalized_weight, r.value)", False, "score"),
}

TOOLS_DOC = f"""Tools (pick exactly one):
1. rank_countries(metric, order="desc"|"asc", limit<=25, min_population_m=0)
   metric is one of: {", ".join(list(NODE_METRICS) + list(REL_METRICS))}
2. partners(country, relation, direction="out"|"in", limit<=25)
   relation is one of: {", ".join(RELATIONS)}
   direction "out" = country -> others (e.g. what the US exports / whom it sells arms to);
   "in" = others -> country (e.g. who supplies energy to Germany, who sells arms to India).
3. between(country_a, country_b)  - every relationship linking two countries
4. country_profile(country)        - key scores and indicators of one country
"""

ROUTER_PROMPT = f"""You translate a question about countries into ONE tool call over a geopolitical knowledge graph.
{TOOLS_DOC}
Examples:
"which country imports the most arms from the USA" -> {{"tool":"partners","args":{{"country":"United States","relation":"arms_trade","direction":"out","limit":10}}}}
"which country has the most energy needs" -> {{"tool":"rank_countries","args":{{"metric":"energy_imports_usd","order":"desc","limit":10}}}}
"most indebted governments" -> {{"tool":"rank_countries","args":{{"metric":"gov_debt_pct_gdp","order":"desc","limit":10,"min_population_m":1}}}}
"who supplies energy to Germany" -> {{"tool":"partners","args":{{"country":"Germany","relation":"energy","direction":"in","limit":10}}}}
"relationship between India and China" -> {{"tool":"between","args":{{"country_a":"India","country_b":"China"}}}}
Reply with JSON only: {{"tool": "...", "args": {{...}}}}"""

ANSWER_PROMPT = """You are an intelligence analyst. Answer the question in 2-4 sentences using ONLY the rows provided
(they come from a knowledge graph built from World Bank, IMF, SIPRI, CEPII BACI, ACLED, GDELT, UN votes, OFAC, EM-DAT).
Name countries and quote the numbers with their units, rounded for reading: money as $1.8B / $410B / $2.3T,
percentages with one decimal, scores with two decimals. Mention the year when given.
If the rows do not answer the question, say exactly what they show instead.
Never invent figures. Reply as JSON: {"answer": "..."}"""


class AskRequest(BaseModel):
    question: str = Field(min_length=3, max_length=500)


def _country(name: Any) -> str:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("country is required")
    resolved = resolve_country(name)
    if not resolved:
        raise ValueError(f"unknown country {name!r}")
    return resolved


def _limit(v: Any, default: int = 10) -> int:
    try:
        return max(1, min(25, int(v)))
    except (TypeError, ValueError):
        return default


def run_tool(tool: str, args: dict[str, Any]) -> dict[str, Any]:
    conn = Neo4jConnection()
    if tool == "rank_countries":
        metric = str(args.get("metric", ""))
        order = "ASC" if str(args.get("order", "desc")).lower() == "asc" else "DESC"
        limit = _limit(args.get("limit"))
        min_pop = float(args.get("min_population_m") or 0)
        pop = "AND coalesce(c.population_m, 0) >= $min_pop" if min_pop > 0 else ""
        if metric in NODE_METRICS:
            q = (f"MATCH (c:Country) WHERE c.`{metric}` IS NOT NULL {pop} "
                 f"RETURN c.name AS country, c.`{metric}` AS value, c.`{metric}_year` AS year "
                 f"ORDER BY value {order} LIMIT $limit")
            label = NODE_METRICS[metric]
        elif metric in REL_METRICS:
            label, head = REL_METRICS[metric]
            q = (f"{head} WHERE value IS NOT NULL {pop.replace('AND', 'AND') if pop else ''} "
                 f"RETURN c.name AS country, value, latest AS year ORDER BY value {order} LIMIT $limit")
        else:
            raise ValueError(f"unknown metric {metric!r}")
        rows = conn.run_query(q, {"limit": limit, "min_pop": min_pop})
        return {"rows": rows, "columns": [("country", "Country", "text"), ("value", label, "number"), ("year", "Year", "text")],
                "query": f"rank_countries({metric}, {order.lower()}, limit={limit})", "metric_label": label}

    if tool == "partners":
        country = _country(args.get("country"))
        rel = str(args.get("relation", ""))
        if rel not in RELATIONS:
            raise ValueError(f"unknown relation {rel!r}")
        rel_type, meaning, value_expr, latest, unit = RELATIONS[rel]
        direction = "in" if str(args.get("direction", "out")).lower() == "in" else "out"
        limit = _limit(args.get("limit"))
        pattern = "(c:Country {name: $country})-[r:%s]->(p:Country)" if direction == "out" else "(p:Country)-[r:%s]->(c:Country {name: $country})"
        year_filter = _LATEST.format(rel=rel_type) if latest else ""
        where = "WHERE r.year = latest" if latest else ""
        q = (f"{year_filter}MATCH {pattern % rel_type} {where} "
             f"WITH p, sum({value_expr}) AS value{', latest' if latest else ''} "
             f"RETURN p.name AS partner, value{', latest AS year' if latest else ', null AS year'} "
             f"ORDER BY value DESC LIMIT $limit")
        rows = conn.run_query(q, {"country": country, "limit": limit})
        desc = f"{country} {meaning} …" if direction == "out" else f"… {meaning} {country}"
        return {"rows": rows, "columns": [("partner", "Partner", "text"), ("value", unit, "number"), ("year", "Year", "text")],
                "query": f"partners({country}, {rel}, {direction})", "metric_label": desc}

    if tool == "between":
        a, b = _country(args.get("country_a")), _country(args.get("country_b"))
        q = """
            MATCH (a:Country {name: $a})-[r]-(b:Country {name: $b})
            WITH type(r) AS relation, startNode(r).name AS from_country, r
            RETURN relation, from_country, count(r) AS links, max(r.year) AS latest_year,
                   max(coalesce(r.value, r.vote_similarity, r.dependency)) AS max_value
            ORDER BY links DESC"""
        rows = conn.run_query(q, {"a": a, "b": b})
        return {"rows": rows, "columns": [("relation", "Relationship", "text"), ("from_country", "From", "text"),
                                          ("links", "Links", "number"), ("latest_year", "Latest year", "text"),
                                          ("max_value", "Max value", "number")],
                "query": f"between({a}, {b})", "metric_label": f"{a} ↔ {b}"}

    if tool == "country_profile":
        country = _country(args.get("country"))
        keys = list(NODE_METRICS)
        props = conn.run_query("MATCH (c:Country {name: $name}) RETURN properties(c) AS p", {"name": country})
        p = props[0]["p"] if props else {}
        rows = [{"metric": NODE_METRICS[k], "value": p[k], "year": p.get(f"{k}_year")} for k in keys if p.get(k) is not None]
        return {"rows": rows, "columns": [("metric", "Metric", "text"), ("value", "Value", "number"), ("year", "Year", "text")],
                "query": f"country_profile({country})", "metric_label": country}

    raise ValueError(f"unknown tool {tool!r}")


def _rule_route(question: str) -> Optional[dict[str, Any]]:
    """Tiny no-LLM fallback for the most common shapes."""
    from common.country_resolver import find_entities
    q = question.lower()
    countries = [e.name for e in find_entities(question) if e.kind == "country"]
    if len(countries) >= 2 and re.search(r"between|relation", q):
        return {"tool": "between", "args": {"country_a": countries[0], "country_b": countries[1]}}
    rel = ("arms_trade" if re.search(r"\barms?\b|weapon", q) else
           "energy" if re.search(r"energy|oil|gas", q) else
           "sanctions" if "sanction" in q else "trade" if re.search(r"trade|export|import", q) else None)
    if countries and rel:
        direction = "in" if re.search(r"\b(to|into)\s+" + re.escape(countries[0].split(",")[0].lower()), q) or "supplies" in q else "out"
        if re.search(r"import\w* .*from", q):
            direction = "out"   # "imports from the USA" = USA -> others
        return {"tool": "partners", "args": {"country": countries[0], "relation": rel, "direction": direction, "limit": 10}}
    if re.search(r"energy (need|import|depend)", q):
        return {"tool": "rank_countries", "args": {"metric": "energy_imports_usd", "limit": 10}}
    if countries:
        return {"tool": "country_profile", "args": {"country": countries[0]}}
    return None


def _fmt_rows_for_llm(rows: list[dict]) -> str:
    return json.dumps(rows[:25], default=str)[:6000]


@router.post("/ask")
def ask(req: AskRequest) -> dict[str, Any]:
    question = req.question.strip()
    plan: Optional[dict[str, Any]] = None
    routed_by = "rules"
    try:
        res = llm.complete_json(ROUTER_PROMPT, question, max_tokens=250)
        if isinstance(res.data.get("tool"), str):
            plan, routed_by = res.data, res.provider
    except llm.LLMUnavailable:
        pass
    if plan is None:
        plan = _rule_route(question)
    if plan is None:
        raise HTTPException(status_code=422, detail="Could not map the question to a graph query. Try naming a country or a metric.")

    try:
        result = run_tool(str(plan.get("tool")), plan.get("args") or {})
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"{exc} (plan: {plan})")

    rows = result["rows"]
    answer, answered_by = None, "template"
    if rows:
        try:
            value_unit = next((lbl for k, lbl, _ in result["columns"] if k == "value"), "n/a")
            res = llm.complete_json(
                ANSWER_PROMPT,
                f"Question: {question}\nQuery: {result['query']}\n"
                f"Meaning of the value column: {result['metric_label']} (unit: {value_unit})\n"
                f"Rows: {_fmt_rows_for_llm(rows)}",
                max_tokens=400,
            )
            answer, answered_by = res.data.get("answer"), res.provider
        except llm.LLMUnavailable:
            pass
    if not answer:
        if not rows:
            answer = f"The graph has no data for {result['query']}."
        else:
            top = rows[0]
            first_val = next((v for k, v in top.items() if isinstance(v, (int, float))), None)
            answer = f"{result['metric_label']}: top result is {list(top.values())[0]}" + (f" ({first_val:,.2f})." if first_val is not None else ".")

    return {
        "question": question,
        "answer": answer,
        "plan": plan,
        "query": result["query"],
        "metric_label": result["metric_label"],
        "columns": [{"key": k, "label": lbl, "format": fmt} for k, lbl, fmt in result["columns"]],
        "rows": rows,
        "routed_by": routed_by,
        "answered_by": answered_by,
    }
