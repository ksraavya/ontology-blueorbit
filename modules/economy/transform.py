from __future__ import annotations

import logging
from typing import Any

import pycountry

from common.entity_mapper import normalize_entity
from modules.economy.constants import ECONOMY_ENTITY_TYPE

logger = logging.getLogger(__name__)

from functools import lru_cache

@lru_cache(maxsize=2048)
def _normalize_cached(name: str) -> str | None:
    return normalize_entity(name, ECONOMY_ENTITY_TYPE)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return None
            return float(s)
        return float(value)
    except (ValueError, TypeError, OverflowError):
        return None


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return None
            return int(float(s))
        return int(value)
    except (ValueError, TypeError, OverflowError):
        return None


def _is_valid_year(year: int) -> bool:
    return 1990 <= year <= 2030


def _is_blank_entity(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False

def _is_real_country_code(code: str) -> bool:
    """Returns True only for valid ISO 3166-1 alpha-3 country codes."""
    if not code or len(code) != 3:
        return False
    try:
        pycountry.countries.get(alpha_3=code.upper())
        return pycountry.countries.get(alpha_3=code.upper()) is not None
    except Exception:
        return False

def transform_trade_data(rows: list[dict]) -> list[dict]:
    out: list[dict] = []
    skipped_value_nonpositive = 0

    for row in rows:
        src = row.get("source")
        tgt = row.get("target")
        if _is_blank_entity(src) or _is_blank_entity(tgt):
            logger.warning(
                "Skipped trade row: empty source or target (source=%r target=%r)",
                src,
                tgt,
            )
            continue

        src_n = _normalize_cached(src)
        if not src_n:
            logger.warning(
                "Skipped trade row: could not normalize source (source=%r)",
                src,
            )
            continue

        tgt_n = _normalize_cached(tgt)
        if not tgt_n:
            logger.warning(
                "Skipped trade row: could not normalize target (target=%r)",
                tgt,
            )
            continue

        val = _safe_float(row.get("value"))
        if val is None:
            logger.warning(
                "Skipped trade row: unconvertible value (source=%r target=%r value=%r)",
                src,
                tgt,
                row.get("value"),
            )
            continue
        if val <= 0:
            skipped_value_nonpositive += 1
            continue

        yr = _safe_int(row.get("year"))
        if yr is None or not _is_valid_year(yr):
            logger.warning(
                "Skipped trade row: invalid year (source=%r target=%r year=%r)",
                src,
                tgt,
                row.get("year"),
            )
            continue

        typ = row.get("type")
        out.append(
            {
                "source": src_n,
                "target": tgt_n,
                "value": val,
                "year": yr,
                "type": typ,
            }
        )

    if skipped_value_nonpositive:
        logger.info(
            "Skipped %d rows with value <= 0",
            skipped_value_nonpositive,
        )

    logger.info(
        "transform_trade_data: input=%d output=%d",
        len(rows),
        len(out),
    )
    return out


def transform_macro_data(rows: list[dict]) -> list[dict]:
    out: list[dict] = []

    for row in rows:
        country = row.get("country")
        if _is_blank_entity(country):
            logger.warning(
                "Skipped macro row: empty country (country=%r)",
                country,
            )
            continue

        country_code = row.get("country_code", "")
        if not _is_real_country_code(country_code):
            logger.warning(
                "Skipped macro row: non-country aggregate (country=%r code=%r)",
                country,
                country_code,
            )
            continue

        country_n = _normalize_cached(country)
        if not country_n:
            logger.warning(
                "Skipped macro row: could not normalize country (country=%r)",
                country,
            )
            continue

        val = _safe_float(row.get("value"))
        if val is None:
            logger.warning(
                "Skipped macro row: unconvertible value (country=%r value=%r)",
                country,
                row.get("value"),
            )
            continue

        yr = _safe_int(row.get("year"))
        if yr is None or not _is_valid_year(yr):
            logger.warning(
                "Skipped macro row: invalid year (country=%r year=%r)",
                country,
                row.get("year"),
            )
            continue

        out.append(
            {
                "country": country_n,
                "indicator": row.get("indicator"),
                "value": val,
                "year": yr,
                "currency": row.get("currency"),
            }
        )

    logger.info(
        "transform_macro_data: input=%d output=%d",
        len(rows),
        len(out),
    )
    return out


def transform_all(data: dict[str, list[dict]]) -> dict[str, list[dict]]:
    trade_clean = transform_trade_data(data["trade"])
    energy_clean = transform_trade_data(data["energy"])
    macro_clean = transform_macro_data(data["macro"])
    logger.info(
        "transform_all: trade=%d energy=%d macro=%d",
        len(trade_clean),
        len(energy_clean),
        len(macro_clean),
    )
    return {
        "trade": trade_clean,
        "energy": energy_clean,
        "macro": macro_clean,
        "orgs": data.get("orgs", []),
    }
