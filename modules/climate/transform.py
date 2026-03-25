from __future__ import annotations
import sys
sys.path.insert(0, '.')

import logging
from functools import lru_cache
from typing import Any

from common.entity_mapper import normalize_entity
from modules.climate.constants import CLIMATE_ENTITY_TYPE, DISASTER_TYPES, EMDAT_YEAR_MAX, EMDAT_YEAR_MIN

logger = logging.getLogger(__name__)


@lru_cache(maxsize=4096)
def _normalize_cached(name: str) -> str | None:
    return normalize_entity(name, CLIMATE_ENTITY_TYPE)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _is_blank_entity(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _is_valid_year(year: int) -> bool:
    return EMDAT_YEAR_MIN <= year <= EMDAT_YEAR_MAX


def transform_disaster_data(rows: list[dict]) -> list[dict]:
    cleaned: list[dict] = []
    for row in rows:
        country_raw = row.get("country")
        if _is_blank_entity(country_raw):
            continue
        country = _normalize_cached(str(country_raw))
        if _is_blank_entity(country):
            continue
        year = _safe_int(row.get("year"))
        if year is None or not _is_valid_year(year):
            continue
        disaster_type = str(row.get("disaster_type", "")).strip()
        if disaster_type not in DISASTER_TYPES:
            continue
        deaths = _safe_float(row.get("deaths"))
        damage_usd = _safe_float(row.get("damage_usd"))
        affected = _safe_float(row.get("affected"))
        cleaned.append(
            {
                "country": country,
                "year": year,
                "disaster_type": disaster_type,
                "deaths": 0.0 if deaths is None else max(0.0, deaths),
                "damage_usd": 0.0 if damage_usd is None else max(0.0, damage_usd),
                "affected": 0.0 if affected is None else max(0.0, affected),
            }
        )
    return cleaned


def transform_emissions_data(rows: list[dict]) -> list[dict]:
    cleaned: list[dict] = []
    for row in rows:
        country = _normalize_cached(str(row.get("name", "")))
        if _is_blank_entity(country):
            continue
        indicator_type = str(row.get("indicator_type", "")).strip().lower()
        if indicator_type not in {"co2", "forest"}:
            continue
        year = _safe_int(row.get("year"))
        if year is None:
            continue
        value = _safe_float(row.get("value"))
        if value is None or value < 0:
            continue
        cleaned.append(
            {
                "country": country,
                "indicator_type": indicator_type,
                "value": value,
                "year": year,
            }
        )
    return cleaned


def transform_temperature_data(rows: list[dict]) -> list[dict]:
    cleaned: list[dict] = []
    for row in rows:
        country = _normalize_cached(str(row.get("name", "")))
        if _is_blank_entity(country):
            continue
        mean_temp_c = _safe_float(row.get("mean_temp_c"))
        if mean_temp_c is None:
            continue
        lat = _safe_float(row.get("lat"))
        lon = _safe_float(row.get("lon"))
        cleaned.append(
            {
                "country": country,
                "mean_temp_c": mean_temp_c,
                "lat": lat,
                "lon": lon,
            }
        )
    return cleaned


def transform_earthquake_data(rows: list[dict]) -> list[dict]:
    cleaned: list[dict] = []
    for row in rows:
        country = _normalize_cached(str(row.get("country", "")))
        if _is_blank_entity(country):
            continue
        year = _safe_int(row.get("year"))
        if year is None:
            continue
        quake_count = _safe_int(row.get("quake_count"))
        max_magnitude = _safe_float(row.get("max_magnitude"))
        avg_magnitude = _safe_float(row.get("avg_magnitude"))
        if quake_count is None or quake_count < 1 or max_magnitude is None:
            continue
        cleaned.append(
            {
                "country": country,
                "year": year,
                "quake_count": quake_count,
                "max_magnitude": max_magnitude,
                "avg_magnitude": 0.0 if avg_magnitude is None else avg_magnitude,
            }
        )
    return cleaned


def transform_all(data: dict[str, list[dict]]) -> dict[str, list[dict]]:
    emissions = transform_emissions_data(data.get("co2", []) + data.get("forest", []))
    return {
        "disasters": transform_disaster_data(data.get("disasters", [])),
        "emissions": emissions,
        "temperature": transform_temperature_data(data.get("temperature", [])),
        "earthquakes": transform_earthquake_data(data.get("earthquakes", [])),
    }
