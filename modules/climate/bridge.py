from __future__ import annotations
import sys
sys.path.insert(0, '.')

"""
Climate Bridge Layer — Impact Propagation
"""

import logging
from collections import defaultdict

from common.db import Neo4jConnection
from common.intelligence.composite import weighted_score
from common.intelligence.normalization import clamp, normalize_by_max
from common.ontology import (
    AFFECTS_ECONOMY,
    DISRUPTS_SUPPLY_CHAIN,
    INCREASES_CONFLICT_RISK,
    get_relation_type,
)
from modules.climate.constants import COUNTRY_LABEL

logger = logging.getLogger(__name__)

SEVERE_DEATHS_THRESHOLD = 100
SEVERE_DAMAGE_THRESHOLD = 500_000_000
IMPACT_WEIGHTS = {"deaths": 0.4, "damage": 0.4, "trade_vulnerability": 0.2}


def _fetch_severe_events(conn: Neo4jConnection, year: int) -> list[dict]:
    query = """
        MATCH (c:Country)-[r:EXPERIENCED]->(e:ClimateEvent)
        WHERE e.year = $year
          AND (coalesce(r.deaths, 0) >= $deaths_threshold OR coalesce(r.value, 0) >= $damage_threshold)
        RETURN c.name AS country,
               e.name AS event,
               coalesce(r.deaths, 0) AS deaths,
               coalesce(r.value, 0) AS damage_usd,
               coalesce(r.risk, 0) AS risk_score,
               r.disaster_type AS disaster_type
    """
    rows = conn.run_query(
        query,
        {
            "year": year,
            "deaths_threshold": SEVERE_DEATHS_THRESHOLD,
            "damage_threshold": SEVERE_DAMAGE_THRESHOLD,
        },
    )
    return rows or []


def _fetch_trade_partners(conn: Neo4jConnection, country: str, year: int) -> list[dict]:
    query = """
        MATCH (c:Country {name: $country})-[r:EXPORTS_TO]->(partner:Country)
        WHERE r.year = $year
        RETURN partner.name AS partner,
               r.dependency AS dependency,
               r.normalized_weight AS trade_weight
        ORDER BY r.normalized_weight DESC
        LIMIT 10
    """
    rows = conn.run_query(query, {"country": country, "year": year})
    return rows or []


def _fetch_energy_dependents(conn: Neo4jConnection, country: str, year: int) -> list[dict]:
    query = """
        MATCH (dependent:Country)-[r:IMPORTS_ENERGY_FROM]->(c:Country {name: $country})
        WHERE r.year = $year
        RETURN dependent.name AS dependent,
               r.dependency AS dependency,
               r.normalized_weight AS energy_weight
        ORDER BY r.dependency DESC
        LIMIT 10
    """
    rows = conn.run_query(query, {"country": country, "year": year})
    return rows or []


def _fetch_economic_partners(conn: Neo4jConnection, country: str) -> list[dict]:
    query = """
        MATCH (c:Country {name: $country})-[r:HAS_TRADE_VOLUME_WITH]-(partner:Country)
        WHERE r.year = 2024
        RETURN partner.name AS partner,
               r.normalized_weight AS volume_weight
        ORDER BY r.normalized_weight DESC
        LIMIT 5
    """
    rows = conn.run_query(query, {"country": country})
    return rows or []


def compute_supply_chain_disruptions(
    severe_events: list[dict],
    conn: Neo4jConnection,
    trade_year: int = 2024,
) -> list[dict]:
    if not severe_events:
        return []
    max_deaths = max(float(e.get("deaths") or 0.0) for e in severe_events)
    max_damage = max(float(e.get("damage_usd") or 0.0) for e in severe_events)
    out: list[dict] = []

    for event in severe_events:
        country = event.get("country")
        if not country:
            continue
        partners = _fetch_trade_partners(conn, country, trade_year)
        if not partners:
            logger.warning("No trade partners found for %s in %s", country, trade_year)
            continue
        norm_deaths = clamp(normalize_by_max(float(event.get("deaths") or 0.0), max_deaths))
        norm_damage = clamp(normalize_by_max(float(event.get("damage_usd") or 0.0), max_damage))
        trade_vuln = clamp(float(event.get("risk_score") or 0.0))
        base_impact = clamp(
            weighted_score(
                {
                    "deaths": norm_deaths,
                    "damage": norm_damage,
                    "trade_vulnerability": trade_vuln,
                },
                IMPACT_WEIGHTS,
            )
        )
        for partner in partners:
            target = partner.get("partner")
            if not target or target == country:
                continue
            trade_weight = clamp(float(partner.get("trade_weight") or 0.0))
            final_score = clamp(base_impact * trade_weight)
            if final_score < 0.05:
                continue
            out.append(
                {
                    "source": country,
                    "target": target,
                    "rel": DISRUPTS_SUPPLY_CHAIN,
                    "source_label": COUNTRY_LABEL,
                    "target_label": COUNTRY_LABEL,
                    "properties": {
                        "value": final_score,
                        "normalized_weight": final_score,
                        "year": trade_year,
                        "source_event": event.get("event"),
                        "disaster_type": event.get("disaster_type"),
                        "confidence": 0.7,
                        "relation_type": get_relation_type(DISRUPTS_SUPPLY_CHAIN),
                    },
                }
            )
    return out


def compute_conflict_risk_propagation(
    severe_events: list[dict],
    conn: Neo4jConnection,
    trade_year: int = 2024,
) -> list[dict]:
    out: list[dict] = []
    for event in severe_events:
        country = event.get("country")
        if not country:
            continue
        dependents = _fetch_energy_dependents(conn, country, trade_year)
        if not dependents:
            continue
        risk_score = clamp(float(event.get("risk_score") or 0.0))
        for dep in dependents:
            target = dep.get("dependent")
            if not target or target == country:
                continue
            dependency = clamp(float(dep.get("dependency") or 0.0))
            conflict_score = clamp(risk_score * dependency)
            if conflict_score < 0.05:
                continue
            out.append(
                {
                    "source": country,
                    "target": target,
                    "rel": INCREASES_CONFLICT_RISK,
                    "source_label": COUNTRY_LABEL,
                    "target_label": COUNTRY_LABEL,
                    "properties": {
                        "value": conflict_score,
                        "normalized_weight": conflict_score,
                        "year": trade_year,
                        "source_event": event.get("event"),
                        "disaster_type": event.get("disaster_type"),
                        "confidence": 0.6,
                        "relation_type": get_relation_type(INCREASES_CONFLICT_RISK),
                    },
                }
            )
    return out


def compute_economic_impact_propagation(
    severe_events: list[dict],
    conn: Neo4jConnection,
    trade_year: int = 2024,
) -> list[dict]:
    out: list[dict] = []
    for event in severe_events:
        country = event.get("country")
        if not country:
            continue
        partners = _fetch_economic_partners(conn, country)
        if not partners:
            continue
        risk_score = clamp(float(event.get("risk_score") or 0.0))
        for p in partners:
            target = p.get("partner")
            if not target or target == country:
                continue
            volume_weight = clamp(float(p.get("volume_weight") or 0.0))
            economy_impact = clamp(risk_score * volume_weight * 0.8)
            if economy_impact < 0.05:
                continue
            out.append(
                {
                    "source": country,
                    "target": target,
                    "rel": AFFECTS_ECONOMY,
                    "source_label": COUNTRY_LABEL,
                    "target_label": COUNTRY_LABEL,
                    "properties": {
                        "value": economy_impact,
                        "normalized_weight": economy_impact,
                        "year": trade_year,
                        "source_event": event.get("event"),
                        "disaster_type": event.get("disaster_type"),
                        "confidence": 0.65,
                        "relation_type": get_relation_type(AFFECTS_ECONOMY),
                    },
                }
            )
    return out


def _batch_write_bridge_edges(
    conn: Neo4jConnection,
    edges: list[dict],
    batch_size: int = 500,
) -> int:
    if not edges:
        return 0
    groups: dict[str, list[dict]] = defaultdict(list)
    for edge in edges:
        groups[edge["rel"]].append(edge)

    total = 0
    for rel_type, rows_edges in groups.items():
        query = f"""
            UNWIND $rows AS row
            MERGE (a:Country {{name: row.source}})
            MERGE (b:Country {{name: row.target}})
            MERGE (a)-[r:{rel_type} {{year: row.props.year}}]->(b)
            SET r += row.props
        """
        for i in range(0, len(rows_edges), batch_size):
            chunk = rows_edges[i : i + batch_size]
            rows = [{"source": e["source"], "target": e["target"], "props": e["properties"]} for e in chunk]
            conn.run_query(query, {"rows": rows})
            total += len(rows)
    return total


def run_bridge(year: int = 2024) -> dict[str, int]:
    conn = Neo4jConnection()
    try:
        severe_events = _fetch_severe_events(conn, year)
        if not severe_events:
            logger.warning("No severe events found for year=%s", year)
            return {
                "severe_events_found": 0,
                "supply_chain_edges": 0,
                "conflict_risk_edges": 0,
                "economic_impact_edges": 0,
                "total_edges_written": 0,
            }
        supply = compute_supply_chain_disruptions(severe_events, conn, year)
        conflict = compute_conflict_risk_propagation(severe_events, conn, year)
        economy = compute_economic_impact_propagation(severe_events, conn, year)
        total_written = _batch_write_bridge_edges(conn, supply + conflict + economy)
        return {
            "severe_events_found": len(severe_events),
            "supply_chain_edges": len(supply),
            "conflict_risk_edges": len(conflict),
            "economic_impact_edges": len(economy),
            "total_edges_written": total_written,
        }
    finally:
        conn.close()
