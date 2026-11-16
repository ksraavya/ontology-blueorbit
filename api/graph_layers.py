from __future__ import annotations

"""
Map layers straight from the graph.

  GET /graph/layers                      -> available relationship layers (+ edge counts)
  GET /graph/relations?layer=trade&limit=300[&country=India]
  GET /graph/metrics                     -> metrics the map can be coloured by
  GET /graph/metric?key=gdp_growth_pct   -> one value per country

Every edge carries ISO numeric codes (matching the Natural Earth map ids),
its real value with unit, the year, and a human label, so the globe never
has to guess coordinates from names and every line is traceable.
"""

import time
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

from common.db import Neo4jConnection

router = APIRouter(prefix="/graph", tags=["graph layers"])

_LATEST = "MATCH ()-[x:{rel}]->() WHERE x.year IS NOT NULL WITH max(x.year) AS latest "

# layer -> definition. Each query returns: src, dst, value, year, extra
# (all filtered to Country nodes; the cleanup step relabels non-countries).
LAYERS: dict[str, dict[str, Any]] = {
    "trade": {
        "label": "Trade flows (exports)", "domain": "economy", "color": "#22c55e", "unit": "USD",
        "source": "CEPII BACI", "directed": True,
        "query": _LATEST.format(rel="EXPORTS_TO") + """
            MATCH (a:Country)-[r:EXPORTS_TO]->(b:Country) WHERE r.year = latest {country}
            RETURN a.name AS src, b.name AS dst, a.iso_numeric AS src_iso, b.iso_numeric AS dst_iso,
                   r.value AS value, r.year AS year, r.dependency AS extra
            ORDER BY value DESC LIMIT $limit""",
        "fmt": "{src} → {dst}: ${value_b} exports ({year})",
    },
    "energy": {
        "label": "Energy supply", "domain": "economy", "color": "#f59e0b", "unit": "USD",
        "source": "CEPII BACI (HS 27)", "directed": True,
        "query": _LATEST.format(rel="IMPORTS_ENERGY_FROM") + """
            MATCH (b:Country)-[r:IMPORTS_ENERGY_FROM]->(a:Country) WHERE r.year = latest {country}
            RETURN a.name AS src, b.name AS dst, a.iso_numeric AS src_iso, b.iso_numeric AS dst_iso,
                   r.value AS value, r.year AS year, r.dependency AS extra
            ORDER BY value DESC LIMIT $limit""",
        "fmt": "{src} supplies {dst}: ${value_b} energy ({extra_pct} of its energy imports, {year})",
    },
    "dependency": {
        "label": "Critical trade dependency", "domain": "economy", "color": "#ef4444", "unit": "share",
        "source": "Derived from BACI", "directed": True,
        "query": _LATEST.format(rel="HAS_TRADE_DEPENDENCY_ON") + """
            MATCH (a:Country)-[r:HAS_TRADE_DEPENDENCY_ON]->(b:Country) WHERE r.year = latest {country}
            RETURN a.name AS src, b.name AS dst, a.iso_numeric AS src_iso, b.iso_numeric AS dst_iso,
                   r.dependency AS value, r.year AS year, null AS extra
            ORDER BY value DESC LIMIT $limit""",
        "fmt": "{src} depends on {dst} for {value_pct} of its trade ({year})",
    },
    "sanctions": {
        "label": "Sanctions (OFAC)", "domain": "economy", "color": "#dc2626", "unit": "entities",
        "source": "OFAC SDN list", "directed": True,
        "query": """
            MATCH (a:Country)-[r:IMPOSED_SANCTIONS_ON]->(b:Country) WHERE true {country}
            RETURN a.name AS src, b.name AS dst, a.iso_numeric AS src_iso, b.iso_numeric AS dst_iso,
                   r.value AS value, r.year AS year, null AS extra
            ORDER BY value DESC LIMIT $limit""",
        "fmt": "{src} sanctions {dst}: {value_int} designated entities",
    },
    "agreements": {
        "label": "Trade agreements (WTO)", "domain": "economy", "color": "#14b8a6", "unit": "agreement",
        "source": "WTO RTA database", "directed": False,
        "query": """
            MATCH (a:Country)-[r:HAS_TRADE_AGREEMENT_WITH]->(b:Country) WHERE a.name < b.name {country}
            WITH a, b, collect(DISTINCT r.agreement_name) AS names
            RETURN a.name AS src, b.name AS dst, a.iso_numeric AS src_iso, b.iso_numeric AS dst_iso,
                   toFloat(size(names)) AS value, null AS year, names[0] AS extra
            ORDER BY value DESC LIMIT $limit""",
        "fmt": "{src} ↔ {dst}: {value_int} trade agreement(s), e.g. {extra}",
    },
    "alignment": {
        "label": "UN voting alignment", "domain": "geopolitics", "color": "#3b82f6", "unit": "similarity",
        "source": "UNGA roll-call votes", "directed": False,
        # Strongest partner per country: one line each, so the map shows blocs, not micro-state pairs.
        "query": """
            MATCH (a:Country)-[r:ALIGNED_WITH]->(b:Country) WHERE r.vote_similarity IS NOT NULL {country}
            WITH a, b, r ORDER BY r.vote_similarity DESC
            WITH a, collect({{b: b, r: r}})[0] AS top
            WITH a, top.b AS b, top.r AS r
            RETURN a.name AS src, b.name AS dst, a.iso_numeric AS src_iso, b.iso_numeric AS dst_iso,
                   r.vote_similarity AS value, r.year AS year, null AS extra
            ORDER BY value DESC LIMIT $limit""",
        "fmt": "{src} votes with {dst} {value_pct} of the time at the UN ({year})",
    },
    "opposition": {
        "label": "Diplomatic opposition", "domain": "geopolitics", "color": "#a855f7", "unit": "score",
        "source": "UNGA votes + GDELT", "directed": False,
        "query": """
            MATCH (a:Country)-[r:OPPOSES]->(b:Country) WHERE true {country}
            RETURN a.name AS src, b.name AS dst, a.iso_numeric AS src_iso, b.iso_numeric AS dst_iso,
                   coalesce(r.normalized_weight, r.value) AS value, r.year AS year, r.basis AS extra
            ORDER BY value DESC LIMIT $limit""",
        "fmt": "{src} opposes {dst} ({extra})",
    },
    "diplomacy": {
        "label": "Diplomatic events (GDELT)", "domain": "geopolitics", "color": "#6366f1", "unit": "events",
        "source": "GDELT 2.0 events", "directed": False,
        "query": """
            MATCH (a:Country)-[r:DIPLOMATIC_INTERACTION]->(b:Country) WHERE a <> b {country}
            WITH a, b, count(r) AS n, avg(r.alignment_score) AS tone
            RETURN a.name AS src, b.name AS dst, a.iso_numeric AS src_iso, b.iso_numeric AS dst_iso,
                   toFloat(n) AS value, null AS year, tone AS extra
            ORDER BY value DESC LIMIT $limit""",
        "fmt": "{src} ↔ {dst}: {value_int} diplomatic events",
    },
    "arms_transfers": {
        "label": "Arms transfers (SIPRI)", "domain": "defense", "color": "#dc2626", "unit": "SIPRI TIV",
        "source": "SIPRI Arms Transfers Database", "directed": True,
        # last 10 years summed: major platforms are delivered in lumps, single years are noisy
        "query": """
            MATCH ()-[x:TRANSFERS_ARMS_TO]->() WITH max(x.year) AS hi
            MATCH (a:Country)-[r:TRANSFERS_ARMS_TO]->(b:Country) WHERE r.year > hi - 10 {country}
            WITH a, b, hi, sum(r.tiv) AS value, collect(r.weapons)[0] AS w
            RETURN a.name AS src, b.name AS dst, a.iso_numeric AS src_iso, b.iso_numeric AS dst_iso,
                   value, hi AS year, w[0..2] AS extra
            ORDER BY value DESC LIMIT $limit""",
        "fmt": "{src} → {dst}: {value_int} TIV of major arms over 10 years to {year} (e.g. {extra})",
    },
    "arms_trade": {
        "label": "Arms trade (customs)", "domain": "defense", "color": "#ef4444", "unit": "USD",
        "source": "CEPII BACI HS 93 + tanks + warships", "directed": True,
        "query": _LATEST.format(rel="TRADES_ARMS_WITH") + """
            MATCH (a:Country)-[r:TRADES_ARMS_WITH]->(b:Country) WHERE r.year = latest {country}
            RETURN a.name AS src, b.name AS dst, a.iso_numeric AS src_iso, b.iso_numeric AS dst_iso,
                   r.value AS value, r.year AS year, null AS extra
            ORDER BY value DESC LIMIT $limit""",
        "fmt": "{src} → {dst}: ${value_b} arms & ammunition ({year}, customs data)",
    },
    "arms": {
        "label": "Arms-export influence", "domain": "defense", "color": "#b91c1c", "unit": "influence",
        "source": "SIPRI-derived", "directed": True,
        "query": """
            MATCH (a:Country)-[r:IS_INFLUENTIAL_TO]->(b:Country) WHERE true {country}
            RETURN a.name AS src, b.name AS dst, a.iso_numeric AS src_iso, b.iso_numeric AS dst_iso,
                   coalesce(r.normalized_weight, r.value) AS value, r.year AS year, r.domain AS extra
            ORDER BY value DESC LIMIT $limit""",
        "fmt": "{src} has {extra} influence over {dst}",
    },
    "alliances": {
        "label": "Alliance co-membership", "domain": "defense", "color": "#0ea5e9", "unit": "alliance",
        "source": "Curated memberships", "directed": False,
        "query": """
            MATCH (a:Country)-[:MEMBER_OF]->(al:Alliance)<-[:MEMBER_OF]-(b:Country)
            WHERE a.name < b.name {country}
            WITH a, b, collect(al.name) AS alliances
            RETURN a.name AS src, b.name AS dst, a.iso_numeric AS src_iso, b.iso_numeric AS dst_iso,
                   toFloat(size(alliances)) AS value, null AS year, alliances AS extra
            ORDER BY value DESC, src LIMIT $limit""",
        "fmt": "{src} & {dst}: {extra}",
    },
    "climate_risk": {
        "label": "Climate → conflict & supply risk", "domain": "climate", "color": "#ea580c", "unit": "score",
        "source": "EM-DAT + bridge model", "directed": True,
        "query": """
            MATCH (a:Country)-[r:INCREASES_CONFLICT_RISK|AFFECTS_ECONOMY|DISRUPTS_SUPPLY_CHAIN]->(b:Country)
            WHERE a <> b {country}
            RETURN a.name AS src, b.name AS dst, a.iso_numeric AS src_iso, b.iso_numeric AS dst_iso,
                   coalesce(r.normalized_weight, r.value) AS value, r.year AS year, type(r) AS extra
            ORDER BY value DESC LIMIT $limit""",
        "fmt": "{src} → {dst}: {extra}",
    },
}

_COUNTRY_FILTER = "AND (a.name = $country OR b.name = $country)"


def _fmt(layer: dict, row: dict) -> str:
    v = row.get("value") or 0.0
    extra = row.get("extra")
    fields = {
        "src": row["src"], "dst": row["dst"], "year": row.get("year") or "",
        "value_b": f"{v / 1e9:,.1f}B" if v >= 1e8 else f"{v / 1e6:,.0f}M",
        "value_pct": f"{v * 100:.0f}%", "value_int": f"{int(v):,}",
        "extra": ", ".join(extra) if isinstance(extra, list) else (extra if extra is not None else ""),
        "extra_pct": f"{float(extra) * 100:.0f}%" if isinstance(extra, (int, float)) else "?",
    }
    try:
        return layer["fmt"].format(**fields)
    except (KeyError, ValueError):
        return f"{row['src']} → {row['dst']}"


_cache: dict[tuple, tuple[float, Any]] = {}
_TTL = 300


def _cached(key: tuple, fn):
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < _TTL:
        return hit[1]
    val = fn()
    _cache[key] = (time.time(), val)
    return val


@router.get("/layers")
def layers() -> dict[str, Any]:
    def build():
        conn = Neo4jConnection()
        out = []
        for key, l in LAYERS.items():
            q = l["query"].format(country="")
            try:
                n = len(conn.run_query(q, {"limit": 5000}))
            except RuntimeError:
                n = 0
            out.append({
                "key": key, "label": l["label"], "domain": l["domain"], "color": l["color"],
                "unit": l["unit"], "source": l["source"], "directed": l["directed"], "available_edges": n,
            })
        return {"layers": out}
    return _cached(("layers",), build)


@router.get("/countries")
def countries() -> dict[str, Any]:
    """Every country with its ISO numeric code: the frontend joins map shapes on this."""
    return _cached(("countries",), lambda: {
        "countries": Neo4jConnection().run_query(
            "MATCH (c:Country) RETURN c.name AS name, c.iso_numeric AS iso ORDER BY name"
        )
    })


_PROFILE_QUERIES: dict[str, str] = {
    "node": "MATCH (c:Country {name: $name}) RETURN properties(c) AS p",
    "alliances": "MATCH (c:Country {name: $name})-[:MEMBER_OF]->(a:Alliance) RETURN collect(a.name) AS v",
    "region": "MATCH (c:Country {name: $name})-[:BELONGS_TO]->(r:Region) RETURN collect(r.name) AS v",
    "political": """
        MATCH (c:Country {name: $name})-[r:HAS_POLITICAL_SYSTEM]->(p)
        RETURN p.name AS system, r.score AS score, r.year AS year ORDER BY r.year DESC LIMIT 1""",
    "gdp": """
        MATCH (c:Country {name: $name})-[r:HAS_GDP]->() RETURN r.year AS year, r.value AS value ORDER BY year""",
    "inflation": """
        MATCH (c:Country {name: $name})-[r:HAS_INFLATION]->() RETURN r.year AS year, r.value AS value ORDER BY year""",
    "defense_spending": """
        MATCH (c:Country {name: $name})-[r:SPENDS_ON_DEFENSE]->(y:Year)
        RETURN y.year AS year, coalesce(r.amount_usd_millions, r.value) AS value ORDER BY year""",
    "conflict": """
        MATCH (c:Country {name: $name})-[r:HAS_CONFLICT_STATS]->(y:Year)
        RETURN y.year AS year, r.total_fatalities AS fatalities, r.violence_events AS violence_events,
               r.civilian_events AS civilian_events, r.fatality_trend AS trend ORDER BY year""",
    "arms_exports": """
        MATCH (c:Country {name: $name})-[r:EXPORTS_ARMS]->(y:Year)
        RETURN y.year AS year, r.value AS value ORDER BY year""",
    "export_partners": """
        MATCH ()-[x:EXPORTS_TO]->() WITH max(x.year) AS latest
        MATCH (c:Country {name: $name})-[r:EXPORTS_TO]->(p:Country) WHERE r.year = latest
        RETURN p.name AS country, r.value AS value, r.year AS year ORDER BY value DESC LIMIT 15""",
    "import_partners": """
        MATCH ()-[x:EXPORTS_TO]->() WITH max(x.year) AS latest
        MATCH (p:Country)-[r:EXPORTS_TO]->(c:Country {name: $name}) WHERE r.year = latest
        RETURN p.name AS country, r.value AS value, r.year AS year ORDER BY value DESC LIMIT 15""",
    "energy_suppliers": """
        MATCH ()-[x:IMPORTS_ENERGY_FROM]->() WITH max(x.year) AS latest
        MATCH (c:Country {name: $name})-[r:IMPORTS_ENERGY_FROM]->(p:Country) WHERE r.year = latest
        RETURN p.name AS country, r.value AS value, r.dependency AS share ORDER BY value DESC LIMIT 15""",
    "trade_totals": """
        MATCH ()-[x:EXPORTS_TO]->() WITH max(x.year) AS latest
        MATCH (c:Country {name: $name})
        OPTIONAL MATCH (c)-[e:EXPORTS_TO]->(:Country) WHERE e.year = latest
        WITH c, latest, sum(e.value) AS exports
        OPTIONAL MATCH (:Country)-[i:EXPORTS_TO]->(c) WHERE i.year = latest
        RETURN latest AS year, exports, sum(i.value) AS imports""",
    "agreements": """
        MATCH (c:Country {name: $name})-[r:HAS_TRADE_AGREEMENT_WITH]-(p:Country)
        RETURN count(DISTINCT p) AS partners, collect(DISTINCT r.agreement_name)[0..6] AS names""",
    "sanctions_received": """
        MATCH (s:Country)-[r:IMPOSED_SANCTIONS_ON]->(c:Country {name: $name})
        RETURN s.name AS by, r.value AS entities""",
    "sanctions_imposed": """
        MATCH (c:Country {name: $name})-[r:IMPOSED_SANCTIONS_ON]->(t:Country)
        RETURN t.name AS on, r.value AS entities ORDER BY entities DESC LIMIT 15""",
    "aligned": """
        MATCH (c:Country {name: $name})-[r:ALIGNED_WITH]-(p:Country) WHERE r.vote_similarity IS NOT NULL
        RETURN p.name AS country, max(r.vote_similarity) AS similarity ORDER BY similarity DESC LIMIT 15""",
    "opposed": """
        MATCH (c:Country {name: $name})-[r:OPPOSES]-(p:Country)
        RETURN p.name AS country, max(coalesce(r.normalized_weight, r.value)) AS score ORDER BY score DESC LIMIT 5""",
    "bloc": """
        MATCH (c:Country {name: $name})-[r:PART_OF_BLOC]-(p:Country)
        RETURN r.bloc_id AS bloc_id, count(DISTINCT p) AS members, collect(DISTINCT p.name)[0..8] AS sample
        ORDER BY members DESC LIMIT 1""",
    "diplomacy": """
        MATCH (c:Country {name: $name})-[r:DIPLOMATIC_INTERACTION]-(p:Country) WHERE p <> c
        RETURN p.name AS country, count(r) AS events ORDER BY events DESC LIMIT 15""",
    "disasters": """
        MATCH (c:Country {name: $name})-[r]->(d)
        WHERE type(r) IN ['EXPERIENCED', 'AFFECTED_BY']
        RETURN type(r) AS rel, labels(d)[0] AS kind, coalesce(d.name, d.type, d.disaster_type) AS name,
               r.year AS year, properties(r) AS props
        ORDER BY r.year DESC LIMIT 12""",
    "arms_suppliers": """
        MATCH ()-[x:TRANSFERS_ARMS_TO]->() WITH max(x.year) AS hi
        MATCH (s)-[r:TRANSFERS_ARMS_TO]->(c:Country {name: $name}) WHERE r.year > hi - 10
        RETURN s.name AS country, sum(r.tiv) AS value, collect(r.weapons)[0][0..2] AS weapons
        ORDER BY value DESC LIMIT 15""",
    "arms_recipients": """
        MATCH ()-[x:TRANSFERS_ARMS_TO]->() WITH max(x.year) AS hi
        MATCH (c {name: $name})-[r:TRANSFERS_ARMS_TO]->(t:Country) WHERE r.year > hi - 10
        RETURN t.name AS country, sum(r.tiv) AS value, collect(r.weapons)[0][0..2] AS weapons
        ORDER BY value DESC LIMIT 15""",
    "arms_companies": """
        MATCH (co:ArmsCompany)-[:HEADQUARTERED_IN]->(c:Country {name: $name})
        MATCH (co)-[r:HAS_ARMS_REVENUE]->()
        WITH co, r ORDER BY r.year DESC
        WITH co, head(collect(r)) AS last
        RETURN co.name AS company, last.value AS value, last.year AS year, last.rank AS rank, last.arms_share AS arms_share
        ORDER BY value DESC LIMIT 15""",
    "connections": """
        MATCH (c:Country {name: $name})-[r]-() RETURN type(r) AS type, count(*) AS n ORDER BY n DESC""",
    "risk_rank": """
        MATCH (c:Country {name: $name}) WHERE c.global_risk_score IS NOT NULL
        MATCH (o:Country) WHERE o.global_risk_score IS NOT NULL
        RETURN sum(CASE WHEN o.global_risk_score > c.global_risk_score THEN 1 ELSE 0 END) + 1 AS rank,
               count(o) AS of""",
}


@router.get("/country/{name}/profile")
def country_profile(name: str) -> dict[str, Any]:
    """
    Everything the country panel shows, from the graph, in one request.
    Each section is a separate small query so one missing domain (e.g.
    climate not loaded) leaves just that section empty instead of failing.
    """
    def build():
        conn = Neo4jConnection()
        out: dict[str, Any] = {"country": name}
        for key, q in _PROFILE_QUERIES.items():
            try:
                rows = conn.run_query(q, {"name": name})
            except RuntimeError as exc:
                out[key] = None
                out.setdefault("errors", {})[key] = str(exc)[:200]
                continue
            if key == "node":
                out["properties"] = rows[0]["p"] if rows else None
            elif key in ("alliances", "region"):
                out[key] = rows[0]["v"] if rows else []
            elif key in ("political", "trade_totals", "agreements", "bloc", "risk_rank"):
                out[key] = rows[0] if rows else None
            else:
                out[key] = rows
        if out.get("properties") is None:
            raise HTTPException(status_code=404, detail=f"No country named {name!r} in the graph")
        return out
    return _cached(("profile", name), build)


@router.get("/profiles")
def profiles() -> dict[str, Any]:
    """Compact per-country rows for the map (scores + flags), one query."""
    return _cached(("profiles",), lambda: {
        "profiles": Neo4jConnection().run_query(
            """
            MATCH (c:Country)
            OPTIONAL MATCH (c)-[:BELONGS_TO]->(r:Region)
            OPTIONAL MATCH (c)-[:MEMBER_OF]->(a:Alliance)
            WITH c, head(collect(DISTINCT r.name)) AS region, collect(DISTINCT a.name) AS alliances
            RETURN c.name AS country, c.iso_numeric AS iso, region, alliances,
                   c.nuclear_status AS nuclear, c.un_p5 AS p5, c.is_regional_power AS regional_power,
                   c.global_risk_score AS global_risk, c.strategic_influence_score AS strategic_influence,
                   c.overall_vulnerability_score AS vulnerability, c.conflict_risk_score AS conflict_risk,
                   c.military_strength_score AS military_strength, c.defense_spending_score AS defense_spending_score,
                   c.arms_export_score AS arms_export_score, c.economic_influence_score AS economic_influence,
                   c.trade_vulnerability_score AS trade_vulnerability, c.energy_vulnerability_score AS energy_vulnerability,
                   c.diplomatic_centrality_score AS diplomatic_centrality, c.climate_risk_score AS climate_risk,
                   c.live_risk_score AS live_risk, c.gdp_usd_bn AS gdp_usd_bn
            ORDER BY country
            """
        )
    })


@router.get("/country/{name}/indicators")
def country_indicators(name: str) -> dict[str, Any]:
    """Every indicator time series for one country (IMF / World Bank), grouped by domain."""
    def build():
        rows = Neo4jConnection().run_query(
            """
            MATCH (c:Country {name: $name})-[r:HAS_INDICATOR]->(i:Indicator)
            WITH i, r ORDER BY r.year
            RETURN i.code AS code, i.key AS key, i.name AS label, i.unit AS unit,
                   i.domain AS domain, i.direction AS direction, i.source AS source,
                   collect({year: r.year, value: r.value, projected: r.projected}) AS series
            ORDER BY domain, label
            """,
            {"name": name},
        )
        return {"country": name, "indicators": rows}
    return _cached(("ci", name), build)


@router.get("/relations")
def relations(
    layer: str,
    limit: int = Query(200, ge=1, le=2000),
    country: Optional[str] = None,
) -> dict[str, Any]:
    if layer not in LAYERS:
        raise HTTPException(status_code=404, detail=f"Unknown layer {layer!r}. See /graph/layers.")
    l = LAYERS[layer]

    def build():
        q = l["query"].format(country=_COUNTRY_FILTER if country else "")
        rows = Neo4jConnection().run_query(q, {"limit": limit, "country": country})
        vals = [float(r["value"]) for r in rows if r.get("value") is not None]
        hi = max(vals) if vals else 1.0
        lo = min(vals) if vals else 0.0
        span = (hi - lo) or 1.0
        edges = []
        for r in rows:
            if r.get("value") is None:
                continue
            edges.append({
                "from": r["src"], "to": r["dst"],
                "from_iso": r.get("src_iso"), "to_iso": r.get("dst_iso"),
                "value": float(r["value"]), "year": r.get("year"),
                # 0.15..1 so the weakest drawn line stays visible
                "weight": round(0.15 + 0.85 * (float(r["value"]) - lo) / span, 4),
                "label": _fmt(l, r),
            })
        return {
            "layer": layer, "label": l["label"], "color": l["color"], "unit": l["unit"],
            "source": l["source"], "directed": l["directed"], "edges": edges,
        }
    return _cached(("rel", layer, limit, country), build)


# =========================================================
# CHOROPLETH METRICS
# =========================================================

_SCORE_METRICS = [
    ("global_risk_score", "Global risk", "composite", "up"),
    ("strategic_influence_score", "Strategic influence", "composite", None),
    ("overall_vulnerability_score", "Overall vulnerability", "composite", "up"),
    ("trade_vulnerability_score", "Trade vulnerability", "economy", "up"),
    ("economic_influence_score", "Economic influence", "economy", None),
    ("energy_vulnerability_score", "Energy vulnerability", "economy", "up"),
    ("conflict_risk_score", "Conflict risk", "defense", "up"),
    ("military_strength_score", "Military strength", "defense", None),
    ("defense_spending_score", "Defence spending", "defense", None),
    ("diplomatic_centrality_score", "Diplomatic centrality", "geopolitics", None),
    ("climate_risk_score", "Climate risk", "climate", "up"),
]


@router.get("/metrics")
def metrics() -> dict[str, Any]:
    def build():
        out = [
            {"key": k, "label": lbl, "domain": d, "unit": "score 0-1", "direction": dirn, "source": "Graph analytics"}
            for k, lbl, d, dirn in _SCORE_METRICS
        ]
        try:
            rows = Neo4jConnection().run_query(
                "MATCH (i:Indicator) RETURN i.key AS key, i.name AS label, i.domain AS domain, "
                "i.unit AS unit, i.direction AS direction, i.source AS source ORDER BY i.domain, i.name"
            )
            out += rows
        except RuntimeError:
            pass
        return {"metrics": out}
    return _cached(("metrics",), build)


@router.get("/metric")
def metric(key: str) -> dict[str, Any]:
    if not key.replace("_", "").isalnum():
        raise HTTPException(status_code=400, detail="Invalid metric key")

    def build():
        rows = Neo4jConnection().run_query(
            f"MATCH (c:Country) WHERE c.`{key}` IS NOT NULL "
            f"RETURN c.name AS country, c.iso_numeric AS iso, c.`{key}` AS value, c.`{key}_year` AS year"
        )
        vals = sorted(float(r["value"]) for r in rows)
        return {
            "key": key,
            "values": rows,
            "count": len(rows),
            "min": vals[0] if vals else None,
            "max": vals[-1] if vals else None,
            # 5th/95th percentiles so one outlier (e.g. 300% inflation) doesn't wash out the scale
            "p05": vals[int(len(vals) * 0.05)] if vals else None,
            "p95": vals[min(len(vals) - 1, int(len(vals) * 0.95))] if vals else None,
        }
    return _cached(("metric", key), build)
