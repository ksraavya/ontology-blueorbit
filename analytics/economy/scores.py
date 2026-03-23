from __future__ import annotations

import logging
import time
from typing import Any

from common.db import Neo4jConnection
from common.intelligence.aggregation import average, max_value
from common.intelligence.composite import weighted_score
from common.intelligence.dependency import diversification_score
from common.intelligence.growth import growth_trend
from common.intelligence.normalization import clamp, normalize_by_max

logger = logging.getLogger(__name__)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compute_economic_power(year: int = 2024) -> int:
    conn = Neo4jConnection()
    try:
        query = """
            MATCH (c:Country)-[r:HAS_GDP]->(:Metric)
            WHERE r.year = $year
            RETURN c.name AS country, r.normalized_weight AS score
        """
        rows = conn.run_query(query, {"year": year})

        updated = 0
        for row in rows:
            country = row.get("country")
            score_raw = _safe_float(row.get("score"))
            if not country or score_raw is None:
                continue

            score = clamp(score_raw, 0.0, 1.0)
            upd = """
                MATCH (c:Country {name: $country})
                SET c.economic_power_score = $score,
                    c.economic_power_year = $year
            """
            conn.run_query(upd, {"country": country, "score": score, "year": year})
            updated += 1

        logger.info("compute_economic_power: updated=%d", updated)
        return updated
    finally:
        conn.close()


def compute_trade_vulnerability(years: list[int] | None = None) -> int:
    if years is None:
        years = list(range(2018, 2025))

    conn = Neo4jConnection()
    try:
        by_country: dict[str, list[float]] = {}
        for year in years:
            query = """
                MATCH (c:Country)-[r:EXPORTS_TO]->(:Country)
                WHERE r.year = $year
                RETURN c.name AS country, max(r.dependency) AS max_dep
            """
            rows = conn.run_query(query, {"year": year})
            for row in rows:
                country = row.get("country")
                max_dep = _safe_float(row.get("max_dep"))
                if not country or max_dep is None:
                    continue
                by_country.setdefault(country, []).append(max_dep)

        updated = 0
        for country, values in by_country.items():
            avg_max_dep = average(values)
            score = clamp(avg_max_dep, 0.0, 1.0)
            upd = """
                MATCH (c:Country {name: $country})
                SET c.trade_vulnerability_score = $score
            """
            conn.run_query(upd, {"country": country, "score": score})
            updated += 1

        logger.info("compute_trade_vulnerability: updated=%d", updated)
        return updated
    finally:
        conn.close()


def compute_energy_vulnerability(years: list[int] | None = None) -> int:
    if years is None:
        years = list(range(2018, 2025))

    conn = Neo4jConnection()
    try:
        by_country: dict[str, list[float]] = {}
        for year in years:
            query = """
                MATCH (c:Country)-[r:IMPORTS_ENERGY_FROM]->(:Country)
                WHERE r.year = $year
                RETURN c.name AS country, max(r.dependency) AS max_dep
            """
            rows = conn.run_query(query, {"year": year})
            for row in rows:
                country = row.get("country")
                max_dep = _safe_float(row.get("max_dep"))
                if not country or max_dep is None:
                    continue
                by_country.setdefault(country, []).append(max_dep)

        updated = 0
        for country, values in by_country.items():
            avg_max_dep = average(values)
            score = clamp(avg_max_dep, 0.0, 1.0)
            upd = """
                MATCH (c:Country {name: $country})
                SET c.energy_vulnerability_score = $score
            """
            conn.run_query(upd, {"country": country, "score": score})
            updated += 1

        logger.info("compute_energy_vulnerability: updated=%d", updated)
        return updated
    finally:
        conn.close()


def compute_partner_diversification(years: list[int] | None = None) -> int:
    if years is None:
        years = list(range(2018, 2025))

    conn = Neo4jConnection()
    try:
        by_country: dict[str, list[float]] = {}
        for year in years:
            query = """
                MATCH (c:Country)-[r:EXPORTS_TO]->(:Country)
                WHERE r.year = $year
                RETURN c.name AS country, collect(r.dependency) AS deps
            """
            rows = conn.run_query(query, {"year": year})
            for row in rows:
                country = row.get("country")
                deps = row.get("deps") or []
                if not country:
                    continue

                deps_floats: list[float] = []
                for d in deps:
                    dv = _safe_float(d)
                    if dv is not None:
                        deps_floats.append(dv)

                score_year = diversification_score(deps_floats)
                by_country.setdefault(country, []).append(score_year)

        updated = 0
        for country, values in by_country.items():
            avg_div = average(values)
            score = clamp(avg_div, 0.0, 1.0)
            upd = """
                MATCH (c:Country {name: $country})
                SET c.partner_diversification_score = $score
            """
            conn.run_query(upd, {"country": country, "score": score})
            updated += 1

        logger.info("compute_partner_diversification: updated=%d", updated)
        return updated
    finally:
        conn.close()


def compute_trade_balance_health(years: list[int] | None = None) -> int:
    if years is None:
        years = list(range(2018, 2025))

    conn = Neo4jConnection()
    try:
        query = """
            MATCH (c:Country)-[r:HAS_TRADE_BALANCE]->(:Metric)
            WHERE r.year IN $years
            WITH c, r ORDER BY r.year
            RETURN c.name AS country,
                   collect(r.value) AS values,
                   collect(r.year) AS years_list
        """
        rows = conn.run_query(query, {"years": years})

        latest_by_country: dict[str, float] = {}
        trend_by_country: dict[str, str] = {}

        for row in rows:
            country = row.get("country")
            values = row.get("values") or []
            years_list = row.get("years_list") or []
            if not country or len(values) != len(years_list) or not values:
                continue

            pairs = []
            for y, v in zip(years_list, values):
                yi = _safe_float(y)
                vf = _safe_float(v)
                if yi is None or vf is None:
                    continue
                pairs.append((int(yi), vf))

            if not pairs:
                continue

            pairs.sort(key=lambda t: t[0])
            sorted_values = [v for _, v in pairs]
            trend = growth_trend(sorted_values)
            if trend == "increasing":
                trend_score = 1.0
            elif trend == "decreasing":
                trend_score = 0.0
            else:
                trend_score = 0.5

            latest_balance = sorted_values[-1]
            latest_by_country[country] = latest_balance
            trend_by_country[country] = trend

        max_abs_balance = max_value([abs(v) for v in latest_by_country.values()])

        updated = 0
        for country, latest_balance in latest_by_country.items():
            norm_balance = normalize_by_max(latest_balance, max_abs_balance)
            trend_score = 1.0 if trend_by_country[country] == "increasing" else 0.0
            if trend_by_country[country] == "stable":
                trend_score = 0.5

            score = weighted_score(
                metrics={"trend": trend_score, "balance": norm_balance},
                weights={"trend": 0.6, "balance": 0.4},
            )
            score = clamp(score, 0.0, 1.0)

            logger.debug(
                "compute_trade_balance_health: country=%s latest=%s trend=%s score=%s",
                country,
                latest_balance,
                trend_by_country[country],
                score,
            )

            upd = """
                MATCH (c:Country {name: $country})
                SET c.trade_balance_score = $score,
                    c.trade_balance_trend = $trend
            """
            conn.run_query(
                upd,
                {
                    "country": country,
                    "score": score,
                    "trend": trend_by_country[country],
                },
            )
            updated += 1

        logger.info("compute_trade_balance_health: updated=%d", updated)
        return updated
    finally:
        conn.close()


def compute_inflation_stability(years: list[int] | None = None) -> int:
    if years is None:
        years = list(range(2000, 2025))

    conn = Neo4jConnection()
    try:
        query = """
            MATCH (c:Country)-[r:HAS_INFLATION]->(:Metric)
            WHERE r.year IN $years
            WITH c, r ORDER BY r.year
            RETURN c.name AS country,
                   collect(r.value) AS values,
                   collect(r.year) AS years_list
        """
        rows = conn.run_query(query, {"years": years})

        updated = 0
        for row in rows:
            country = row.get("country")
            values = row.get("values") or []
            years_list = row.get("years_list") or []
            if not country or len(values) != len(years_list) or not values:
                continue

            pairs = []
            for y, v in zip(years_list, values):
                yi = _safe_float(y)
                vf = _safe_float(v)
                if yi is None or vf is None:
                    continue
                pairs.append((int(yi), vf))
            if not pairs:
                continue

            pairs.sort(key=lambda t: t[0])
            sorted_values = [v for _, v in pairs]

            avg_inflation = average(sorted_values)
            trend = growth_trend(sorted_values)

            instability = clamp(normalize_by_max(avg_inflation, 20.0), 0.0, 1.0)
            # stability = 1.0 - instability (implemented via diversification_score)
            stability = diversification_score([instability])

            if trend == "increasing":
                stability *= 0.7
            elif trend == "decreasing":
                stability *= 1.1

            score = clamp(stability, 0.0, 1.0)

            logger.debug(
                "compute_inflation_stability: country=%s avg=%s trend=%s score=%s",
                country,
                avg_inflation,
                trend,
                score,
            )

            upd = """
                MATCH (c:Country {name: $country})
                SET c.inflation_stability_score = $score,
                    c.avg_inflation = $avg_inflation,
                    c.inflation_trend = $trend
            """
            conn.run_query(
                upd,
                {
                    "country": country,
                    "score": score,
                    "avg_inflation": avg_inflation,
                    "trend": trend,
                },
            )
            updated += 1

        logger.info("compute_inflation_stability: updated=%d", updated)
        return updated
    finally:
        conn.close()


def compute_trade_integration(year: int = 2024) -> int:
    # `year` is unused by the query contract (agreements are not time-filtered),
    # but kept for API consistency.
    _ = year

    conn = Neo4jConnection()
    try:
        query = """
            MATCH (c:Country)-[:HAS_TRADE_AGREEMENT_WITH]->(:Country)
            RETURN c.name AS country, count(*) AS agreement_count
        """
        rows = conn.run_query(query)
        counts: dict[str, float] = {}
        for row in rows:
            country = row.get("country")
            cnt = _safe_float(row.get("agreement_count"))
            if not country or cnt is None:
                continue
            counts[country] = cnt

        max_count = max_value(list(counts.values()))

        updated = 0
        for country, agreement_count in counts.items():
            score = normalize_by_max(agreement_count, max_count)
            score = clamp(score, 0.0, 1.0)
            upd = """
                MATCH (c:Country {name: $country})
                SET c.trade_integration_score = $score
            """
            conn.run_query(upd, {"country": country, "score": score})
            updated += 1

        logger.info("compute_trade_integration: updated=%d", updated)
        return updated
    finally:
        conn.close()


def compute_economic_influence() -> int:
    conn = Neo4jConnection()
    try:
        query_scores = """
            MATCH (c:Country)
            WHERE c.economic_power_score IS NOT NULL
              AND c.partner_diversification_score IS NOT NULL
              AND c.trade_integration_score IS NOT NULL
            RETURN c.name AS country,
                   c.economic_power_score AS gdp_power,
                   c.partner_diversification_score AS diversification,
                   c.trade_integration_score AS trade_integration
        """
        score_rows = conn.run_query(query_scores)

        query_volume = """
            MATCH (c:Country)-[r:HAS_TRADE_VOLUME_WITH]-(:Country)
            WHERE r.year = 2024
            RETURN c.name AS country, sum(r.value) AS total_volume
        """
        vol_rows = conn.run_query(query_volume)

        volume_by_country: dict[str, float] = {}
        for row in vol_rows:
            country = row.get("country")
            total_volume = _safe_float(row.get("total_volume"))
            if not country or total_volume is None:
                continue
            volume_by_country[country] = total_volume

        volumes = list(volume_by_country.values())
        max_volume = max_value(volumes) if volumes else 0.0

        updated = 0
        for row in score_rows:
            country = row.get("country")
            if not country:
                continue

            gdp_power = _safe_float(row.get("gdp_power"))
            diversification = _safe_float(row.get("diversification"))
            trade_integration = _safe_float(row.get("trade_integration"))
            total_volume = volume_by_country.get(country)

            if (
                gdp_power is None
                or diversification is None
                or trade_integration is None
                or total_volume is None
            ):
                continue

            normalized_volume = normalize_by_max(total_volume, max_volume)
            score = weighted_score(
                metrics={
                    "gdp_power": gdp_power,
                    "trade_volume": normalized_volume,
                    "diversification": diversification,
                    "trade_integration": trade_integration,
                },
                weights={
                    "gdp_power": 0.35,
                    "trade_volume": 0.30,
                    "diversification": 0.20,
                    "trade_integration": 0.15,
                },
            )
            score = clamp(score, 0.0, 1.0)

            upd = """
                MATCH (c:Country {name: $country})
                SET c.economic_influence_score = $score
            """
            conn.run_query(upd, {"country": country, "score": score})
            updated += 1

        logger.info("compute_economic_influence: updated=%d", updated)
        return updated
    finally:
        conn.close()


def compute_all_economic_scores(
    years: list[int] | None = None,
    latest_year: int = 2024,
) -> dict[str, int]:
    if years is None:
        years = list(range(2018, 2025))

    stage_counts: dict[str, int] = {}

    start = time.perf_counter()

    logger.info("compute_all_economic_scores: stage compute_economic_power")
    stage_counts["compute_economic_power"] = compute_economic_power(latest_year)

    logger.info("compute_all_economic_scores: stage compute_trade_vulnerability")
    stage_counts["compute_trade_vulnerability"] = compute_trade_vulnerability(years)

    logger.info("compute_all_economic_scores: stage compute_energy_vulnerability")
    stage_counts["compute_energy_vulnerability"] = compute_energy_vulnerability(years)

    logger.info("compute_all_economic_scores: stage compute_partner_diversification")
    stage_counts["compute_partner_diversification"] = compute_partner_diversification(years)

    logger.info("compute_all_economic_scores: stage compute_trade_balance_health")
    stage_counts["compute_trade_balance_health"] = compute_trade_balance_health(years)

    logger.info("compute_all_economic_scores: stage compute_inflation_stability")
    stage_counts["compute_inflation_stability"] = compute_inflation_stability()

    logger.info("compute_all_economic_scores: stage compute_trade_integration")
    stage_counts["compute_trade_integration"] = compute_trade_integration(latest_year)

    logger.info("compute_all_economic_scores: stage compute_economic_influence")
    stage_counts["compute_economic_influence"] = compute_economic_influence()

    elapsed_s = time.perf_counter() - start
    logger.info(
        "compute_all_economic_scores: done in %.2fs",
        elapsed_s,
    )
    return stage_counts

