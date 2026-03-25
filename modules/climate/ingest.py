from __future__ import annotations
import sys
sys.path.insert(0, '.')

"""
Climate module — ingestion layer.

Sources:
    1. EM-DAT        local CSV  — disaster events
    2. World Bank    REST API   — CO2 per capita, forest coverage %
    3. REST Countries REST API  — country centroids + area (used by NASA + USGS)
    4. NASA POWER    REST API   — mean annual surface temperature
    5. USGS          REST API   — earthquakes magnitude ≥ 5.0

Hazard risk classification is NOT ingested here — it is derived
entirely in compute.py from the above sources.
"""

import logging
import math
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pycountry
import requests

from common.entity_mapper import normalize_entity
from modules.climate.constants import (
    BBOX_MAX_PADDING_DEG,
    BBOX_MIN_PADDING_DEG,
    DISASTER_TYPES,
    EMDAT_COLUMN_MAP,
    EMDAT_FILENAME,
    EMDAT_YEAR_MAX,
    EMDAT_YEAR_MIN,
    NASA_COMMUNITY,
    NASA_PARAMETER,
    NASA_POWER_BASE,
    NASA_SLEEP_S,
    REST_COUNTRIES_BASE,
    REST_COUNTRIES_SLEEP_S,
    USGS_BASE,
    USGS_LIMIT,
    USGS_MIN_MAGNITUDE,
    USGS_SLEEP_S,
    USGS_YEARS,
    WB_API_BASE,
    WB_CO2_INDICATOR,
    WB_FOREST_INDICATOR,
    WB_SLEEP_S,
    WB_YEARS_MRV,
)

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
logger  = logging.getLogger(__name__)

# NASA POWER month keys used when "ANN" key is absent
_NASA_MONTH_KEYS: frozenset[str] = frozenset({
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
})

# Alternate EM-DAT damage column names across different export formats
_EMDAT_DAMAGE_ALTERNATES: tuple[str, ...] = (
    "Total Damage ('000 US$)",
    "Total Damage (USD)",
    "Damage ('000 US$)",
    "AdjustedDamage",
)


# ── Country index ─────────────────────────────────────────────────────────────

def _resolve_emdat_file() -> tuple[Path, str]:
    """
    Resolve EM-DAT input file in data/raw.

    Priority:
    1) Exact constants filename (typically CSV)
    2) Any emdat_disasters*.csv
    3) Any emdat_disasters*.xlsx / *.xls

    Returns:
        (path, kind) where kind is "csv" or "excel".
    """
    preferred = RAW_DIR / EMDAT_FILENAME
    if preferred.exists():
        return preferred, "csv"

    for pattern, kind in (
        ("emdat_disasters*.csv", "csv"),
        ("emdat_disasters*.xlsx", "excel"),
        ("emdat_disasters*.xls", "excel"),
    ):
        matches = sorted(RAW_DIR.glob(pattern))
        if matches:
            return matches[0], kind

    raise FileNotFoundError(
        f"EM-DAT file not found in {RAW_DIR.resolve()}.\n"
        f"Expected {EMDAT_FILENAME} or emdat_disasters*.csv/xlsx/xls.\n"
        "Download from https://www.emdat.be and place it in data/raw/"
    )

def _build_country_index() -> dict[str, dict]:
    """
    Build a complete ISO country index from pycountry.

    Names are passed through normalize_entity so they exactly match
    the Country nodes already in Neo4j (e.g. "Russian Federation" not "Russia").

    Returns:
        {alpha_2: {name, alpha_2, alpha_3, numeric}}
    """
    index: dict[str, dict] = {}
    for country in pycountry.countries:
        alpha_2 = getattr(country, "alpha_2", None)
        alpha_3 = getattr(country, "alpha_3", None)
        numeric = getattr(country, "numeric", None)
        if not alpha_2 or not alpha_3:
            continue
        normalized_name = normalize_entity(country.name, "country")
        if not normalized_name:
            continue
        index[alpha_2] = {
            "name":    normalized_name,
            "alpha_2": alpha_2,
            "alpha_3": alpha_3,
            "numeric": numeric,
        }
    logger.info("_build_country_index: %d ISO countries indexed", len(index))
    return index


# ── REST Countries helpers ────────────────────────────────────────────────────

def _fetch_country_centroids(country_index: dict[str, dict]) -> dict[str, dict]:
    """
    Fetch lat/lon centroid and land area for every country via REST Countries API.

    Endpoint: GET {REST_COUNTRIES_BASE}/alpha/{iso2}
    Response fields used:
        latlng  → [lat, lon]
        area    → km²

    Returns:
        {alpha_2: {lat, lon, name, area_km2}}

    Countries whose fetch fails are logged and skipped — they will simply
    have no NASA POWER or USGS data rather than crashing the pipeline.
    """
    centroids: dict[str, dict] = {}
    total = len(country_index)

    for idx, (iso2, payload) in enumerate(country_index.items(), start=1):
        url = f"{REST_COUNTRIES_BASE}/alpha/{iso2}"
        try:
            response = requests.get(url, timeout=25)
            response.raise_for_status()
            data = response.json()

            # API returns a list with one item
            item = data[0] if isinstance(data, list) and data else data
            if not isinstance(item, dict):
                logger.warning("Unexpected REST Countries response for %s", iso2)
                time.sleep(REST_COUNTRIES_SLEEP_S)
                continue

            latlng = item.get("latlng")
            if not isinstance(latlng, list) or len(latlng) < 2:
                logger.warning("Missing or malformed latlng for %s", iso2)
                time.sleep(REST_COUNTRIES_SLEEP_S)
                continue

            lat      = float(latlng[0])
            lon      = float(latlng[1])
            area_raw = item.get("area")
            # area can be None for very small territories — default to 100 km²
            # which gives a ~1° padding, enough for a micro-state
            area_km2 = float(area_raw) if area_raw is not None else 100.0

            centroids[iso2] = {
                "lat":      lat,
                "lon":      lon,
                "name":     payload["name"],
                "area_km2": area_km2,
            }

        except requests.HTTPError as exc:
            logger.warning(
                "REST Countries HTTP error for %s (%s): %s",
                iso2, payload.get("name"), exc,
            )
        except requests.RequestException as exc:
            logger.warning(
                "REST Countries request failed for %s (%s): %s",
                iso2, payload.get("name"), exc,
            )
        except (ValueError, KeyError, TypeError) as exc:
            logger.warning(
                "REST Countries parse error for %s (%s): %s",
                iso2, payload.get("name"), exc,
            )

        if idx % 50 == 0:
            logger.info(
                "REST Countries centroids: %d / %d fetched, %d succeeded",
                idx, total, len(centroids),
            )
        time.sleep(REST_COUNTRIES_SLEEP_S)

    logger.info(
        "_fetch_country_centroids: %d / %d countries with centroids",
        len(centroids), total,
    )
    return centroids


def _build_bbox_index(centroids: dict[str, dict]) -> dict[str, dict]:
    """
    Build axis-aligned bounding boxes from centroids + area-proportional padding.

    padding_deg = clamp(sqrt(area_km²) / 100, BBOX_MIN_PADDING_DEG, BBOX_MAX_PADDING_DEG)

    Small nations (e.g. Vatican ~0.44 km²) get the minimum padding so that
    nearby earthquakes can still be assigned to them.
    Large nations (e.g. Russia ~17M km²) are capped so the bbox stays sane.

    Returns:
        {alpha_2: {min_lat, max_lat, min_lon, max_lon, name, area_km2}}
    """
    bbox: dict[str, dict] = {}
    for iso2, payload in centroids.items():
        area_km2 = float(payload.get("area_km2") or 100.0)
        pad = math.sqrt(max(area_km2, 1.0)) / 100.0
        pad = float(min(BBOX_MAX_PADDING_DEG, max(BBOX_MIN_PADDING_DEG, pad)))

        lat = float(payload["lat"])
        lon = float(payload["lon"])

        bbox[iso2] = {
            "min_lat":  max(-90.0,  lat - pad),
            "max_lat":  min( 90.0,  lat + pad),
            "min_lon":  max(-180.0, lon - pad),
            "max_lon":  min( 180.0, lon + pad),
            "name":     payload["name"],
            "area_km2": area_km2,
        }
    logger.info("_build_bbox_index: %d bounding boxes built", len(bbox))
    return bbox


# ── EM-DAT ────────────────────────────────────────────────────────────────────

def load_emdat_data() -> list[dict]:
    """
    Load the EM-DAT disaster CSV from RAW_DIR / EMDAT_FILENAME.

    Column handling
    ---------------
    - Rename only the columns present in EMDAT_COLUMN_MAP (resilient to
      missing optional columns).
    - Handle the damage column across multiple EM-DAT export formats via
      _EMDAT_DAMAGE_ALTERNATES fallback list.
    - Convert damage from '000 USD → USD (× 1000).
    - Fill NaN deaths / damage / affected with 0.0.
    - Filter rows to DISASTER_TYPES and years [EMDAT_YEAR_MIN, EMDAT_YEAR_MAX].

    Raises:
        FileNotFoundError if the CSV does not exist.
    """
    file_path, file_kind = _resolve_emdat_file()
    logger.info("load_emdat_data: using input file %s", file_path.name)

    if file_kind == "excel":
        # Supports .xlsx exports saved directly from EM-DAT downloads.
        df = pd.read_excel(file_path)
    else:
        # Encoding: EM-DAT exports are usually UTF-8 but older exports use latin-1
        try:
            df = pd.read_csv(file_path, encoding="utf-8", low_memory=False)
        except UnicodeDecodeError:
            logger.warning("UTF-8 decode failed for EM-DAT, retrying with latin-1")
            df = pd.read_csv(file_path, encoding="latin-1", low_memory=False)

    logger.info("load_emdat_data: raw CSV rows=%d columns=%d", len(df), len(df.columns))

    # Normalize incoming headers and drop duplicated columns from malformed exports.
    df.columns = [str(c).strip() for c in df.columns]
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
        logger.warning("load_emdat_data: duplicate columns detected; keeping first occurrence")

    # Some EM-DAT exports use Start Year instead of Year.
    if "Year" not in df.columns:
        for year_alias in ("Start Year", "Start year", "Start_Year"):
            if year_alias in df.columns:
                df = df.rename(columns={year_alias: "Year"})
                logger.info("load_emdat_data: year column remapped from %r to 'Year'", year_alias)
                break

    # ── Damage column normalisation ───────────────────────────────────────────
    # Primary name comes from EMDAT_COLUMN_MAP; if absent, try alternates.
    primary_damage_col = "Total Damage, Adjusted ('000 US$)"
    if primary_damage_col not in df.columns:
        for alt in _EMDAT_DAMAGE_ALTERNATES:
            if alt in df.columns:
                df = df.rename(columns={alt: primary_damage_col})
                logger.info(
                    "load_emdat_data: damage column remapped from %r to canonical name",
                    alt,
                )
                break
        else:
            # No damage column found at all — create a zero column so the
            # rest of the pipeline can continue
            logger.warning(
                "load_emdat_data: no damage column found in CSV; "
                "setting damage to 0 for all rows. "
                "Available columns: %s",
                list(df.columns),
            )
            df[primary_damage_col] = 0.0

    # ── Column selection and rename ───────────────────────────────────────────
    existing_map = {
        src: dst
        for src, dst in EMDAT_COLUMN_MAP.items()
        if src in df.columns
    }
    missing_required = {"Country", "Year", "Disaster Type"} - set(existing_map.keys())
    if missing_required:
        raise ValueError(
            f"EM-DAT CSV is missing required columns: {missing_required}. "
            f"Available columns: {list(df.columns)}"
        )

    df = df[list(existing_map.keys())].rename(columns=existing_map)

    # ── Filtering ─────────────────────────────────────────────────────────────
    df = df[df["disaster_type"].isin(DISASTER_TYPES)].copy()

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df[
        (df["year"] >= EMDAT_YEAR_MIN) & (df["year"] <= EMDAT_YEAR_MAX)
    ].copy()

    # ── Numeric coercion ──────────────────────────────────────────────────────
    for col in ("deaths", "damage_thousands_usd", "affected"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        else:
            df[col] = 0.0

    df["damage_usd"] = df["damage_thousands_usd"] * 1_000.0
    df = df.drop(columns=["damage_thousands_usd"], errors="ignore")

    records = df.to_dict(orient="records")
    logger.info(
        "load_emdat_data: returning %d rows after filtering "
        "(disaster_types=%s, years=%d–%d)",
        len(records), DISASTER_TYPES, EMDAT_YEAR_MIN, EMDAT_YEAR_MAX,
    )
    return records


# ── World Bank ────────────────────────────────────────────────────────────────

def _fetch_wb_indicator(iso2: str, indicator: str) -> list[dict]:
    """
    Fetch one World Bank indicator for one country.

    Endpoint: {WB_API_BASE}/country/{iso2}/indicator/{indicator}
              ?format=json&per_page=100&mrv={WB_YEARS_MRV}

    Response: [metadata_dict, [datapoints]]
    Each datapoint: {date: "2023", value: float|None, countryiso3code: str, ...}
    Datapoints where value is None are skipped.

    Returns [] on any HTTP or parse error (caller logs progress separately).
    """
    url = (
        f"{WB_API_BASE}/country/{iso2}/indicator/{indicator}"
        f"?format=json&per_page=100&mrv={WB_YEARS_MRV}"
    )
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        payload = response.json()

        if (
            not isinstance(payload, list)
            or len(payload) < 2
            or not isinstance(payload[1], list)
        ):
            return []

        rows: list[dict] = []
        for dp in payload[1]:
            value = dp.get("value")
            if value is None:
                continue
            rows.append({
                "iso2":      iso2,
                "indicator": indicator,
                "value":     value,
                "year":      dp.get("date"),
                "iso3":      dp.get("countryiso3code"),
            })
        return rows

    except requests.HTTPError as exc:
        logger.debug("WB HTTP error iso2=%s indicator=%s: %s", iso2, indicator, exc)
        return []
    except requests.RequestException as exc:
        logger.debug("WB request error iso2=%s indicator=%s: %s", iso2, indicator, exc)
        return []
    except (ValueError, KeyError, TypeError) as exc:
        logger.debug("WB parse error iso2=%s indicator=%s: %s", iso2, indicator, exc)
        return []


def load_worldbank_co2_data(country_index: dict[str, dict]) -> list[dict]:
    """
    Fetch CO2 per capita (EN.ATM.CO2E.PC) for all ISO countries.

    Iterates country_index, calls _fetch_wb_indicator for each,
    enriches rows with the normalized country name and indicator_type="co2".
    Sleeps WB_SLEEP_S between requests.
    Logs progress every 50 countries.
    """
    out: list[dict] = []
    total = len(country_index)

    for idx, (iso2, info) in enumerate(country_index.items(), start=1):
        rows = _fetch_wb_indicator(iso2, WB_CO2_INDICATOR)
        for row in rows:
            row["name"]           = info["name"]
            row["indicator_type"] = "co2"
            out.append(row)

        if idx % 50 == 0:
            logger.info(
                "WorldBank CO2: %d / %d countries processed, %d rows so far",
                idx, total, len(out),
            )
        time.sleep(WB_SLEEP_S)

    logger.info(
        "load_worldbank_co2_data: %d total rows for %d countries",
        len(out), total,
    )
    return out


def load_worldbank_forest_data(country_index: dict[str, dict]) -> list[dict]:
    """
    Fetch forest area % (AG.LND.FRST.ZS) for all ISO countries.
    Same structure as load_worldbank_co2_data but indicator_type="forest".
    """
    out: list[dict] = []
    total = len(country_index)

    for idx, (iso2, info) in enumerate(country_index.items(), start=1):
        rows = _fetch_wb_indicator(iso2, WB_FOREST_INDICATOR)
        for row in rows:
            row["name"]           = info["name"]
            row["indicator_type"] = "forest"
            out.append(row)

        if idx % 50 == 0:
            logger.info(
                "WorldBank Forest: %d / %d countries processed, %d rows so far",
                idx, total, len(out),
            )
        time.sleep(WB_SLEEP_S)

    logger.info(
        "load_worldbank_forest_data: %d total rows for %d countries",
        len(out), total,
    )
    return out


# ── NASA POWER ────────────────────────────────────────────────────────────────

def load_nasa_power_data(centroids: dict[str, dict]) -> list[dict]:
    """
    Fetch mean annual surface temperature (T2M) from NASA POWER for each
    country centroid.

    Endpoint: {NASA_POWER_BASE}?parameters=T2M&community=RE
                                &longitude={lon}&latitude={lat}&format=JSON

    Response path:
        response["properties"]["parameter"]["T2M"]
        → dict of {"JAN": float, ..., "ANN": float, ...}

    Strategy:
        1. Use "ANN" key directly (annual mean provided by NASA).
        2. If "ANN" absent, average the 12 monthly keys (JAN–DEC).
        3. Skip the country if neither is available.

    Sleeps NASA_SLEEP_S between requests.
    All errors are logged with WARNING (not silently swallowed).
    """
    out: list[dict] = []
    total = len(centroids)

    for idx, (iso2, item) in enumerate(centroids.items(), start=1):
        lat  = item["lat"]
        lon  = item["lon"]
        name = item["name"]
        url  = (
            f"{NASA_POWER_BASE}"
            f"?parameters={NASA_PARAMETER}"
            f"&community={NASA_COMMUNITY}"
            f"&longitude={lon}"
            f"&latitude={lat}"
            f"&format=JSON"
        )
        try:
            response = requests.get(url, timeout=45)
            response.raise_for_status()
            payload = response.json()

            t2m = (
                payload
                .get("properties", {})
                .get("parameter", {})
                .get("T2M", {})
            )
            if not isinstance(t2m, dict) or not t2m:
                logger.warning(
                    "NASA POWER: empty T2M for %s (%s)", iso2, name
                )
                time.sleep(NASA_SLEEP_S)
                continue

            # Prefer the pre-computed annual mean
            ann = t2m.get("ANN")
            if ann is None:
                monthly_vals = [
                    float(v)
                    for k, v in t2m.items()
                    if k in _NASA_MONTH_KEYS and v is not None
                ]
                ann = sum(monthly_vals) / len(monthly_vals) if monthly_vals else None

            if ann is None:
                logger.warning(
                    "NASA POWER: could not compute annual mean for %s (%s)",
                    iso2, name,
                )
                time.sleep(NASA_SLEEP_S)
                continue

            out.append({
                "alpha_2":    iso2,
                "name":       name,
                "lat":        lat,
                "lon":        lon,
                "mean_temp_c": float(ann),
            })

        except requests.HTTPError as exc:
            logger.warning(
                "NASA POWER HTTP error for %s (%s): %s", iso2, name, exc
            )
        except requests.RequestException as exc:
            logger.warning(
                "NASA POWER request failed for %s (%s): %s", iso2, name, exc
            )
        except (ValueError, KeyError, TypeError) as exc:
            logger.warning(
                "NASA POWER parse error for %s (%s): %s", iso2, name, exc
            )

        if idx % 50 == 0:
            logger.info(
                "NASA POWER: %d / %d countries processed, %d succeeded",
                idx, total, len(out),
            )
        time.sleep(NASA_SLEEP_S)

    logger.info(
        "load_nasa_power_data: %d / %d countries with temperature data",
        len(out), total,
    )
    return out


# ── USGS ──────────────────────────────────────────────────────────────────────

def _assign_quake_to_country(
    lat: float,
    lon: float,
    bbox_index: dict[str, dict],
) -> str | None:
    """
    Find the country whose bounding box contains (lat, lon).

    If multiple bboxes match (common near borders), return the country
    with the smallest area_km² — i.e. the most geographically specific one.
    Returns None if no bbox matches.
    """
    matches: list[tuple[float, str]] = []
    for box in bbox_index.values():
        if (
            box["min_lat"] <= lat <= box["max_lat"]
            and box["min_lon"] <= lon <= box["max_lon"]
        ):
            matches.append((float(box.get("area_km2", 1e12)), box["name"]))

    if not matches:
        return None

    matches.sort(key=lambda x: x[0])
    return matches[0][1]


def load_usgs_earthquake_data(bbox_index: dict[str, dict]) -> list[dict]:
    """
    Fetch all earthquakes ≥ USGS_MIN_MAGNITUDE for each year in USGS_YEARS.

    Endpoint: {USGS_BASE}?format=geojson
                          &starttime={year}-01-01&endtime={year}-12-31
                          &minmagnitude={USGS_MIN_MAGNITUDE}&limit={USGS_LIMIT}

    GeoJSON structure:
        response["features"] → list of feature dicts
        feature["geometry"]["coordinates"] → [lon, lat, depth]
        feature["properties"]["mag"]       → magnitude float

    Earthquake → country assignment:
        Uses _assign_quake_to_country() with the bbox_index.
        Unassigned earthquakes (open ocean etc.) are silently discarded.

    Aggregation per (country_name, year):
        quake_count, max_magnitude, avg_magnitude

    Sleeps USGS_SLEEP_S between year requests (USGS enforces rate limits).
    """
    # {(country_name, year): [magnitude, ...]}
    grouped: dict[tuple[str, int], list[float]] = defaultdict(list)

    for year in USGS_YEARS:
        url = (
            f"{USGS_BASE}"
            f"?format=geojson"
            f"&starttime={year}-01-01"
            f"&endtime={year}-12-31"
            f"&minmagnitude={USGS_MIN_MAGNITUDE}"
            f"&limit={USGS_LIMIT}"
        )
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            features = response.json().get("features", [])

            assigned = 0
            for feature in features:
                coords = (
                    feature.get("geometry") or {}
                ).get("coordinates", [])
                if len(coords) < 2:
                    continue

                lon = coords[0]
                lat = coords[1]
                mag = (feature.get("properties") or {}).get("mag")
                if mag is None:
                    continue

                country_name = _assign_quake_to_country(
                    float(lat), float(lon), bbox_index
                )
                if not country_name:
                    continue

                grouped[(country_name, year)].append(float(mag))
                assigned += 1

            logger.info(
                "USGS year=%d: %d features fetched, %d assigned to countries",
                year, len(features), assigned,
            )

        except requests.HTTPError as exc:
            logger.warning("USGS HTTP error year=%d: %s", year, exc)
        except requests.RequestException as exc:
            logger.warning("USGS request failed year=%d: %s", year, exc)
        except (ValueError, KeyError, TypeError) as exc:
            logger.warning("USGS parse error year=%d: %s", year, exc)

        time.sleep(USGS_SLEEP_S)

    # Aggregate magnitudes into summary stats
    out: list[dict] = []
    for (country, year), mags in grouped.items():
        if not mags:
            continue
        out.append({
            "country":       country,
            "year":          year,
            "quake_count":   len(mags),
            "max_magnitude": max(mags),
            "avg_magnitude": sum(mags) / len(mags),
        })

    logger.info(
        "load_usgs_earthquake_data: %d (country, year) pairs across %d years",
        len(out), len(USGS_YEARS),
    )
    return out


# ── Orchestrator ──────────────────────────────────────────────────────────────

def load_all(years: list[int] | None = None) -> dict[str, list[dict]]:
    """
    Load all five climate data sources in dependency order.

    Args:
        years: unused — year ranges are controlled by per-source constants.
               Accepted for API consistency with other module runners.

    Pipeline order:
        1. Build ISO country index        (pycountry — no network call)
        2. Fetch country centroids        (REST Countries — needed by NASA + USGS)
        3. Build bbox index from centroids (local computation)
        4. Load EM-DAT disasters          (local CSV)
        5. Load World Bank CO2            (REST API)
        6. Load World Bank Forest         (REST API)
        7. Load NASA POWER temperature    (REST API)
        8. Load USGS earthquakes          (REST API)

    Returns:
        {
            "disasters":   list[dict],   # EM-DAT rows
            "co2":         list[dict],   # World Bank CO2 rows
            "forest":      list[dict],   # World Bank Forest rows
            "temperature": list[dict],   # NASA POWER rows
            "earthquakes": list[dict],   # USGS aggregated rows
        }

    Note: "hazard_risk" is NOT included — it is derived in compute.py.
    """
    if years is not None:
        logger.info(
            "load_all: 'years' argument ignored — "
            "year ranges are governed by per-source constants (USGS_YEARS, WB_YEARS_MRV, etc.)"
        )

    # ── Step 1: country index (pycountry, no I/O) ─────────────────────────
    logger.info("=== Stage 1/8: Building ISO country index ===")
    country_index = _build_country_index()

    # ── Step 2: centroids via REST Countries ──────────────────────────────
    logger.info("=== Stage 2/8: Fetching country centroids (REST Countries) ===")
    centroids = _fetch_country_centroids(country_index)

    # ── Step 3: bounding boxes (local) ────────────────────────────────────
    logger.info("=== Stage 3/8: Building bounding box index ===")
    bbox_index = _build_bbox_index(centroids)

    # ── Step 4: EM-DAT disasters (local CSV) ──────────────────────────────
    logger.info("=== Stage 4/8: Loading EM-DAT disasters ===")
    disasters = load_emdat_data()

    # ── Step 5: World Bank CO2 ────────────────────────────────────────────
    logger.info("=== Stage 5/8: Fetching World Bank CO2 data ===")
    co2 = load_worldbank_co2_data(country_index)

    # ── Step 6: World Bank Forest ─────────────────────────────────────────
    logger.info("=== Stage 6/8: Fetching World Bank Forest data ===")
    forest = load_worldbank_forest_data(country_index)

    # ── Step 7: NASA POWER temperature ────────────────────────────────────
    logger.info("=== Stage 7/8: Fetching NASA POWER temperature data ===")
    temperature = load_nasa_power_data(centroids)

    # ── Step 8: USGS earthquakes ──────────────────────────────────────────
    logger.info("=== Stage 8/8: Fetching USGS earthquake data ===")
    earthquakes = load_usgs_earthquake_data(bbox_index)

    logger.info(
        "load_all complete — disasters=%d co2=%d forest=%d "
        "temperature=%d earthquakes=%d",
        len(disasters), len(co2), len(forest),
        len(temperature), len(earthquakes),
    )

    return {
        "disasters":   disasters,
        "co2":         co2,
        "forest":      forest,
        "temperature": temperature,
        "earthquakes": earthquakes,
    }