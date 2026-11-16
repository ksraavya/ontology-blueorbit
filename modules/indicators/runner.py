"""
Fetch + load country indicators (IMF WEO, World Bank WDI and WGI).

    python -m modules.indicators.runner             # use cached responses when present
    python -m modules.indicators.runner --refresh   # re-download everything

Every API response is saved to data/raw/indicators/<source>_<code>.json
before loading, so:
  - a rebuild works even when an API is down (World Bank 503s often),
  - each number in the graph is traceable to the file it came from,
  - failed downloads can be retried later without refetching the rest.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import pycountry
import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from common.db import Neo4jConnection  # noqa: E402
from common.entity_mapper import canonical_country  # noqa: E402
from modules.indicators.catalog import FIRST_YEAR, INDICATORS, LAST_ACTUAL_YEAR, IndicatorDef  # noqa: E402

logger = logging.getLogger(__name__)
CACHE = ROOT / "data" / "raw" / "indicators"

Series = dict[str, dict[int, float]]   # iso3 -> {year: value}


# =========================================================
# FETCH
# =========================================================

def _get_json(url: str, attempts: int = 4, timeout: int = 90):
    for i in range(attempts):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code in (429, 502, 503, 504):
                raise requests.HTTPError(f"{r.status_code} {r.text[:80]}")
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as exc:
            wait = 3 * (i + 1)
            logger.warning("GET failed (%s), retry %d/%d in %ds: %s", url[:90], i + 1, attempts, wait, exc)
            time.sleep(wait)
    return None


def _fetch_imf(code: str) -> Series | None:
    data = _get_json(f"https://www.imf.org/external/datamapper/api/v1/{code}")
    if not data or "values" not in data:
        return None
    out: Series = {}
    for iso3, years in (data["values"].get(code) or {}).items():
        out[iso3] = {int(y): float(v) for y, v in years.items() if v is not None}
    return out


def _fetch_wb(code: str, wb_source: int) -> Series | None:
    out: Series = {}
    page, pages = 1, 1
    while page <= pages:
        url = (
            f"https://api.worldbank.org/v2/country/all/indicator/{code}"
            f"?format=json&per_page=2000&date={FIRST_YEAR}:{LAST_ACTUAL_YEAR}&source={wb_source}&page={page}"
        )
        payload = _get_json(url)
        if not isinstance(payload, list) or len(payload) < 2:
            # Never return (and cache) a partial download: a page failing
            # midway used to leave e.g. military spending for 100 of 200
            # countries cached forever. All pages or nothing.
            logger.warning("WB %s: page %d of %d failed, discarding partial download", code, page, pages)
            return None
        pages = int(payload[0].get("pages") or 1)
        for dp in payload[1] or []:
            iso3, val, year = dp.get("countryiso3code"), dp.get("value"), dp.get("date")
            if iso3 and val is not None and year and str(year).isdigit():
                out.setdefault(iso3, {})[int(year)] = float(val)
        page += 1
        time.sleep(0.3)
    return out


def fetch(ind: IndicatorDef, refresh: bool = False) -> Series | None:
    CACHE.mkdir(parents=True, exist_ok=True)
    path = CACHE / f"{ind.source}_{ind.code}.json"
    if path.exists() and not refresh:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return {iso: {int(y): v for y, v in ys.items()} for iso, ys in raw.items()}
    series = _fetch_imf(ind.code) if ind.source == "imf" else _fetch_wb(ind.code, ind.wb_source)
    if series:
        path.write_text(json.dumps(series), encoding="utf-8")
    return series


# =========================================================
# LOAD
# =========================================================

def _iso3_to_country(iso3: str) -> str | None:
    """IMF/WB region codes (WEOWORLD, EUU, ...) are not countries -> None."""
    if iso3 == "XKX" or iso3 == "UVK":
        return canonical_country("Kosovo")
    c = pycountry.countries.get(alpha_3=iso3)
    return canonical_country(c.name) if c else None


def load(conn: Neo4jConnection, ind: IndicatorDef, series: Series) -> tuple[int, int]:
    conn.run_query(
        "MERGE (i:Indicator {code: $code}) "
        "SET i.name = $name, i.unit = $unit, i.domain = $domain, i.source = $source, "
        "    i.key = $key, i.direction = $direction",
        {
            "code": ind.code, "name": ind.name, "unit": ind.unit, "domain": ind.domain,
            "source": "IMF WEO" if ind.source == "imf" else ("World Bank WGI" if ind.wb_source == 75 else "World Bank WDI"),
            "key": ind.key, "direction": ind.direction,
        },
    )
    edges, latest = [], []
    for iso3, years in series.items():
        name = _iso3_to_country(iso3)
        if not name:
            continue
        actual = {y: v for y, v in years.items() if FIRST_YEAR <= y <= LAST_ACTUAL_YEAR}
        for y, v in years.items():
            if y >= FIRST_YEAR:
                edges.append({"name": name, "year": y, "value": v, "projected": y > LAST_ACTUAL_YEAR})
        if actual:
            y = max(actual)
            latest.append({"name": name, "year": y, "value": actual[y]})

    for i in range(0, len(edges), 5000):
        conn.run_query(
            "UNWIND $rows AS r "
            "MATCH (c:Country {name: r.name}) MATCH (i:Indicator {code: $code}) "
            "MERGE (c)-[e:HAS_INDICATOR {year: r.year}]->(i) "
            "SET e.value = r.value, e.projected = r.projected",
            {"rows": edges[i:i + 5000], "code": ind.code},
        )
    conn.run_query(
        f"UNWIND $rows AS r MATCH (c:Country {{name: r.name}}) "
        f"SET c.`{ind.key}` = r.value, c.`{ind.key}_year` = r.year",
        {"rows": latest},
    )
    return len(edges), len(latest)


def run(refresh: bool = False) -> dict[str, str]:
    conn = Neo4jConnection()
    report: dict[str, str] = {}
    for ind in INDICATORS:
        series = fetch(ind, refresh=refresh)
        if not series:
            report[ind.code] = "FAILED (API unavailable, no cache)"
            logger.warning("indicator %s: no data", ind.code)
            continue
        n_edges, n_countries = load(conn, ind, series)
        report[ind.code] = f"{n_edges} values, latest for {n_countries} countries"
        logger.info("indicator %-22s %s", ind.code, report[ind.code])
    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true")
    args = ap.parse_args()
    for code, state in run(refresh=args.refresh).items():
        print(f"  {code:<24} {state}")
