from __future__ import annotations

"""
GET /graph/insights?module=overview|defence|economy|geopolitics|climate

Small cards for the home page's left column. Each card is one Cypher query
over the graph, returned as {id, title, kind, unit, source, data}:

  kind "bars"   -> data: [{label, value}]          (ranked list)
  kind "series" -> data: [{label, unit, points: [{x, y}]}]  (per-year trends)
  kind "split"  -> data: [{label, value}]          (share of a whole)

Every value is read from Neo4j; nothing is computed in the browser.
"""

import time
from typing import Any, Callable

from fastapi import APIRouter, HTTPException

from common.db import Neo4jConnection

router = APIRouter(prefix="/graph", tags=["graph insights"])

_cache: dict[str, tuple[float, Any]] = {}
_TTL = 300


def _bars(q: str) -> Callable[[Neo4jConnection], list[dict]]:
    return lambda c: [{"label": r["label"], "value": float(r["value"])} for r in c.run_query(q) if r["value"] is not None]


def _series(spec: list[tuple[str, str, str, float]]) -> Callable[[Neo4jConnection], list[dict]]:
    """spec: (label, unit, cypher returning x,y, divisor)"""
    def run(c: Neo4jConnection) -> list[dict]:
        out = []
        for label, unit, q, div in spec:
            pts = [{"x": int(r["x"]), "y": float(r["y"]) / div} for r in c.run_query(q) if r["x"] is not None and r["y"] is not None]
            if pts:
                out.append({"label": label, "unit": unit, "points": pts})
        return out
    return run


_TRADE_BY_YEAR = "MATCH (:Country)-[r:EXPORTS_TO]->(:Country) RETURN r.year AS x, sum(r.value) AS y ORDER BY x"
_ENERGY_BY_YEAR = "MATCH (:Country)-[r:IMPORTS_ENERGY_FROM]->(:Country) RETURN r.year AS x, sum(r.value) AS y ORDER BY x"
_ARMS_BY_YEAR = "MATCH (:Country)-[r:TRADES_ARMS_WITH]->(:Country) RETURN r.year AS x, sum(r.value) AS y ORDER BY x"

DOMAIN_OF_REL = {
    "Economy": ["EXPORTS_TO", "IMPORTS_FROM", "HAS_TRADE_VOLUME_WITH", "HAS_TRADE_DEPENDENCY_ON", "IMPORTS_ENERGY_FROM",
                "EXPORTS_ENERGY_TO", "HAS_TRADE_AGREEMENT_WITH", "IMPOSED_SANCTIONS_ON", "HAS_GDP", "HAS_INFLATION",
                "HAS_TRADE_BALANCE", "IS_MAJOR_EXPORT_PARTNER_OF", "HAS_HIGH_DEPENDENCY_ON"],
    "Defence": ["SPENDS_ON_DEFENSE", "EXPORTS_ARMS", "IMPORTS_ARMS", "HAS_CONFLICT_STATS", "MEMBER_OF", "TRADES_ARMS_WITH",
                "IS_INFLUENTIAL_TO", "BELONGS_TO"],
    "Geopolitics": ["DIPLOMATIC_INTERACTION", "ALIGNED_WITH", "OPPOSES", "PART_OF_BLOC", "HAS_POLITICAL_SYSTEM"],
    "Climate": ["EXPERIENCED", "EMITS", "HAS_RESOURCE_STRESS", "DISRUPTS_SUPPLY_CHAIN", "AFFECTED_BY", "CAUSED_DAMAGE",
                "RESULTED_IN_FATALITIES", "INCREASES_CONFLICT_RISK", "AFFECTS_ECONOMY", "DEPENDS_ON_RESOURCE"],
    "Indicators": ["HAS_INDICATOR"],
}


def _domain_split(c: Neo4jConnection) -> list[dict]:
    out = []
    for domain, rels in DOMAIN_OF_REL.items():
        n = 0
        for t in rels:
            try:
                n += c.run_query(f"MATCH ()-[r:`{t}`]->() RETURN count(r) AS n")[0]["n"]
            except RuntimeError:
                pass
        if n:
            out.append({"label": domain, "value": float(n)})
    return out


CARDS: dict[str, list[dict[str, Any]]] = {
    "overview": [
        {"id": "top_risk", "title": "Top risk countries", "kind": "bars", "unit": "global risk (0–1), population ≥ 1M", "source": "Composite score",
         "run": _bars("MATCH (c:Country) WHERE c.global_risk_score IS NOT NULL AND coalesce(c.population_m, 0) >= 1 RETURN c.name AS label, c.global_risk_score AS value ORDER BY value DESC LIMIT 6")},
        {"id": "trends", "title": "Relationship trends", "kind": "series", "unit": "per year", "source": "BACI customs data",
         "run": _series([("World trade", "$T", _TRADE_BY_YEAR, 1e12), ("Energy trade", "$B", _ENERGY_BY_YEAR, 1e9), ("Arms trade", "$B", _ARMS_BY_YEAR, 1e9)])},
        {"id": "domains", "title": "Graph data by domain", "kind": "split", "unit": "relationships", "source": "Neo4j count store",
         "run": _domain_split},
    ],
    "defence": [
        {"id": "spenders", "title": "Top defence spenders", "kind": "bars", "unit": "USD millions", "source": "SIPRI",
         "run": _bars("""MATCH (:Country)-[x:SPENDS_ON_DEFENSE]->(y0:Year) WITH max(y0.year) AS latest
                         MATCH (c:Country)-[r:SPENDS_ON_DEFENSE]->(y:Year {year: latest})
                         RETURN c.name AS label, coalesce(r.amount_usd_millions, r.value) AS value ORDER BY value DESC LIMIT 6""")},
        {"id": "sipri_transfers", "title": "Biggest major-arms transfers (10 yrs)", "kind": "bars", "unit": "SIPRI TIV", "source": "SIPRI Arms Transfers Database",
         "run": _bars("""MATCH ()-[x:TRANSFERS_ARMS_TO]->() WITH max(x.year) AS hi
                         MATCH (a:Country)-[r:TRANSFERS_ARMS_TO]->(b:Country) WHERE r.year > hi - 10
                         RETURN a.name + ' → ' + b.name AS label, sum(r.tiv) AS value ORDER BY value DESC LIMIT 6""")},
        {"id": "arms_flows", "title": "Biggest arms flows (customs, latest year)", "kind": "bars", "unit": "USD", "source": "BACI HS 93 + tanks + warships",
         "run": _bars("""MATCH ()-[x:TRADES_ARMS_WITH]->() WITH max(x.year) AS latest
                         MATCH (a:Country)-[r:TRADES_ARMS_WITH]->(b:Country) WHERE r.year = latest
                         RETURN a.name + ' → ' + b.name AS label, r.value AS value ORDER BY value DESC LIMIT 6""")},
        {"id": "companies", "title": "Biggest arms companies", "kind": "bars", "unit": "USD millions", "source": "SIPRI Top 100",
         "run": _bars("""MATCH (co:ArmsCompany)-[r:HAS_ARMS_REVENUE]->() WITH max(r.year) AS latest
                         MATCH (co:ArmsCompany)-[r:HAS_ARMS_REVENUE {year: latest}]->()
                         OPTIONAL MATCH (co)-[:HEADQUARTERED_IN]->(c:Country)
                         RETURN co.name + coalesce(' · ' + c.iso2, '') AS label, r.value AS value ORDER BY value DESC LIMIT 6""")},
        {"id": "conflict", "title": "Conflict deaths (latest year)", "kind": "bars", "unit": "fatalities", "source": "ACLED",
         "run": _bars("""MATCH (:Country)-[x:HAS_CONFLICT_STATS]->(y0:Year) WITH max(y0.year) AS latest
                         MATCH (c:Country)-[r:HAS_CONFLICT_STATS]->(y:Year {year: latest})
                         RETURN c.name AS label, r.total_fatalities AS value ORDER BY value DESC LIMIT 6""")},
        {"id": "arms_trend", "title": "Arms market per year", "kind": "series", "unit": "per year", "source": "SIPRI Top 100 · BACI customs",
         "run": _series([
             ("Top-100 arms revenues", "$B", "MATCH (g:GlobalMetric {code: 'sipri_top100_total'}) UNWIND range(0, size(g.years) - 1) AS i RETURN g.years[i] AS x, g.values[i] AS y ORDER BY x", 1),
             ("Major-arms transfers", "k TIV", "MATCH ()-[r:TRANSFERS_ARMS_TO]->() WHERE r.year >= 2002 RETURN r.year AS x, sum(r.tiv) AS y ORDER BY x", 1e3),
             ("Arms trade (customs)", "$B", _ARMS_BY_YEAR, 1e9),
         ])},
    ],
    "economy": [
        {"id": "exporters", "title": "Top exporters (latest year)", "kind": "bars", "unit": "USD", "source": "CEPII BACI",
         "run": _bars("""MATCH ()-[x:EXPORTS_TO]->() WITH max(x.year) AS latest
                         MATCH (c:Country)-[r:EXPORTS_TO]->(:Country) WHERE r.year = latest
                         RETURN c.name AS label, sum(r.value) AS value ORDER BY value DESC LIMIT 6""")},
        {"id": "trade_vuln", "title": "Most trade-vulnerable", "kind": "bars", "unit": "score (0–1), population ≥ 1M", "source": "Economy analytics",
         "run": _bars("MATCH (c:Country) WHERE c.trade_vulnerability_score IS NOT NULL AND coalesce(c.population_m, 0) >= 1 RETURN c.name AS label, c.trade_vulnerability_score AS value ORDER BY value DESC LIMIT 6")},
        {"id": "sanctions", "title": "Sanctions intensity", "kind": "bars", "unit": "designated entities", "source": "OFAC SDN",
         "run": _bars("MATCH (:Country)-[r:IMPOSED_SANCTIONS_ON]->(c:Country) RETURN c.name AS label, r.value AS value ORDER BY value DESC LIMIT 6")},
        {"id": "trade_trend", "title": "World trade per year", "kind": "series", "unit": "$T", "source": "CEPII BACI",
         "run": _series([("World trade", "$T", _TRADE_BY_YEAR, 1e12), ("Energy trade", "$B", _ENERGY_BY_YEAR, 1e9)])},
    ],
    "geopolitics": [
        {"id": "central", "title": "Most central in diplomacy", "kind": "bars", "unit": "centrality (0–1)", "source": "GDELT network",
         "run": _bars("MATCH (c:Country) WHERE c.diplomatic_centrality_score IS NOT NULL RETURN c.name AS label, c.diplomatic_centrality_score AS value ORDER BY value DESC LIMIT 6")},
        {"id": "systems", "title": "Political systems", "kind": "split", "unit": "countries", "source": "V-Dem (latest year)",
         "run": _bars("""MATCH (c:Country)-[r:HAS_POLITICAL_SYSTEM]->(p) WITH c, r, p ORDER BY r.year DESC
                         WITH c, head(collect(p.name)) AS label RETURN label, toFloat(count(*)) AS value ORDER BY value DESC""")},
        {"id": "blocs", "title": "UN voting blocs (Louvain)", "kind": "bars", "unit": "members", "source": "UNGA votes",
         "run": _bars("""MATCH (c:Country) WHERE c.bloc_id IS NOT NULL
                         WITH c.bloc_id AS b, collect(c.name) AS members
                         RETURN 'Bloc ' + toString(b) + ' · ' + members[0] AS label, toFloat(size(members)) AS value ORDER BY value DESC LIMIT 6""")},
        {"id": "unstable", "title": "Least politically stable", "kind": "bars", "unit": "WGI estimate (−2.5…2.5)", "source": "World Bank WGI",
         "run": _bars("MATCH (c:Country) WHERE c.political_stability IS NOT NULL RETURN c.name AS label, c.political_stability AS value ORDER BY value ASC LIMIT 6")},
    ],
    "climate": [
        {"id": "climate_risk", "title": "Highest climate risk", "kind": "bars", "unit": "score (0–1)", "source": "EM-DAT + analytics",
         "run": _bars("MATCH (c:Country) WHERE c.climate_risk_score IS NOT NULL RETURN c.name AS label, c.climate_risk_score AS value ORDER BY value DESC LIMIT 6")},
        {"id": "disasters", "title": "Disasters per year", "kind": "series", "unit": "events", "source": "EM-DAT",
         "run": _series([("Disasters", "events", "MATCH (:Country)-[r:EXPERIENCED]->() WHERE r.year IS NOT NULL RETURN r.year AS x, toFloat(count(r)) AS y ORDER BY x", 1),
                         ("Deaths", "k", "MATCH (:Country)-[r:EXPERIENCED]->() WHERE r.year IS NOT NULL RETURN r.year AS x, sum(coalesce(r.deaths, 0)) AS y ORDER BY x", 1e3)])},
        {"id": "water", "title": "Most water-stressed", "kind": "bars", "unit": "% of renewable freshwater withdrawn", "source": "World Bank",
         "run": _bars("MATCH (c:Country) WHERE c.water_stress_pct IS NOT NULL AND coalesce(c.population_m, 0) >= 1 RETURN c.name AS label, c.water_stress_pct AS value ORDER BY value DESC LIMIT 6")},
        {"id": "co2", "title": "Highest CO₂ per capita", "kind": "bars", "unit": "t CO₂e per person, population ≥ 1M", "source": "World Bank",
         "run": _bars("""MATCH (c:Country)-[r:EMITS]->() WHERE r.co2_pc IS NOT NULL AND coalesce(c.population_m, 0) >= 1 WITH c, r ORDER BY r.year DESC
                         WITH c, head(collect(r.co2_pc)) AS value RETURN c.name AS label, value ORDER BY value DESC LIMIT 6""")},
    ],
}


@router.get("/insights")
def insights(module: str = "overview") -> dict[str, Any]:
    if module not in CARDS:
        raise HTTPException(status_code=404, detail=f"Unknown module {module!r}")
    hit = _cache.get(module)
    if hit and time.time() - hit[0] < _TTL:
        return hit[1]
    conn = Neo4jConnection()
    cards = []
    for card in CARDS[module]:
        try:
            data = card["run"](conn)
        except RuntimeError as exc:
            cards.append({k: v for k, v in card.items() if k != "run"} | {"data": [], "error": str(exc)[:200]})
            continue
        if data:
            cards.append({k: v for k, v in card.items() if k != "run"} | {"data": data})
    out = {"module": module, "cards": cards}
    _cache[module] = (time.time(), out)
    return out
