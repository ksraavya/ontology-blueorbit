from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from analytics.economy.queries import (
    get_bilateral_trade,
    get_country_economic_profile,
    get_energy_dependencies,
    get_energy_supplier_profile,
    get_gdp_comparison,
    get_gdp_trend,
    get_high_dependency_network,
    get_inflation_history,
    get_inflation_ranking,
    get_major_energy_exporters,
    get_major_partners_network,
    get_most_energy_vulnerable,
    get_most_trade_vulnerable,
    get_sanctions_network,
    get_shared_agreements,
    get_top_economies,
    get_top_trade_pairs,
    get_trade_agreement_partners,
    get_trade_balance_history,
    get_trade_dependencies,
    get_trade_surplus_ranking,
    get_trade_volume_trend,
    search_countries,
)

router = APIRouter(prefix="/economy", tags=["economy"])


# =========================================================
# COUNTRY PROFILE
# =========================================================

@router.get("/country/{name}")
def economic_profile(
    name: str,
    year: int = Query(default=2024, ge=2000, le=2030),
) -> dict[str, Any]:
    """
    Full economic intelligence profile for a country.
    Returns all scores, macro data, top partners,
    energy suppliers, sanctions status, and trade agreements.
    """
    result = get_country_economic_profile(name, year)
    if not result:
        raise HTTPException(status_code=404, detail=f"Country '{name}' not found")
    return result


@router.get("/country/{name}/dependencies")
def trade_dependencies(
    name: str,
    year: int = Query(default=2024, ge=2000, le=2030),
    min_dependency: float = Query(default=0.05, ge=0.0, le=1.0),
) -> list[dict[str, Any]]:
    """
    All export-destination trade dependencies for a country above threshold.
    Each row shows how much of the country's total exports go to a given
    partner — a high value means heavy reliance on that partner as an
    export destination.
    """
    return get_trade_dependencies(name, year, min_dependency)


@router.get("/country/{name}/energy")
def energy_dependencies(
    name: str,
    year: int = Query(default=2024, ge=2000, le=2030),
) -> list[dict[str, Any]]:
    """
    All energy import dependencies for a country,
    sorted by dependency descending.
    """
    return get_energy_dependencies(name, year)


@router.get("/country/{name}/high-dependencies")
def high_dependency_network(name: str) -> dict[str, Any]:
    """
    Countries this country has high dependency on (> 0.7)
    and countries that have high dependency on this country (reverse).
    """
    return get_high_dependency_network(name)


@router.get("/country/{name}/gdp")
def gdp_trend(
    name: str,
    start_year: int = Query(default=2000, ge=1960, le=2030),
    end_year: int = Query(default=2024, ge=1960, le=2030),
) -> list[dict[str, Any]]:
    """
    GDP trend for a country across a year range.
    """
    return get_gdp_trend(name, start_year, end_year)


@router.get("/country/{name}/inflation")
def inflation_history(
    name: str,
    start_year: int = Query(default=2000, ge=1960, le=2030),
    end_year: int = Query(default=2024, ge=1960, le=2030),
) -> list[dict[str, Any]]:
    """
    Inflation history for a country across years.
    """
    return get_inflation_history(name, start_year, end_year)


@router.get("/country/{name}/trade-balance")
def trade_balance_history(
    name: str,
    start_year: int = Query(default=2018, ge=2000, le=2030),
    end_year: int = Query(default=2024, ge=2000, le=2030),
) -> list[dict[str, Any]]:
    """
    Trade balance history for a country across years.
    """
    return get_trade_balance_history(name, start_year, end_year)


@router.get("/country/{name}/sanctions")
def sanctions_network(name: str) -> dict[str, Any]:
    """
    Full sanctions picture — who sanctions this country
    and who this country sanctions.
    """
    return get_sanctions_network(name)


@router.get("/country/{name}/agreements")
def trade_agreements(name: str) -> list[dict[str, Any]]:
    """
    All active trade agreement partners for a country.
    """
    return get_trade_agreement_partners(name)


@router.get("/country/{name}/energy-exports")
def energy_exports(
    name: str,
    year: int = Query(default=2024, ge=2000, le=2030),
) -> list[dict[str, Any]]:
    """
    Countries that import energy from this supplier,
    with dependency values. Shows geopolitical leverage.
    """
    return get_energy_supplier_profile(name, year)


# =========================================================
# BILATERAL
# =========================================================

@router.get("/bilateral/{country_a}/{country_b}")
def bilateral_trade(
    country_a: str,
    country_b: str,
) -> dict[str, Any]:
    """
    Full bilateral trade relationship between two countries.
    Includes trade flows (both directions), energy flows (both directions),
    bilateral volume trend, shared agreements, and sanctions.
    """
    return get_bilateral_trade(country_a, country_b)


@router.get("/bilateral/{country_a}/{country_b}/agreements")
def bilateral_agreements(
    country_a: str,
    country_b: str,
) -> list[dict[str, Any]]:
    """
    Trade agreements shared by two countries.
    """
    return get_shared_agreements(country_a, country_b)


@router.get("/bilateral/{country_a}/{country_b}/volume")
def bilateral_volume(
    country_a: str,
    country_b: str,
) -> list[dict[str, Any]]:
    """
    Bilateral trade volume trend across all available years.
    """
    return get_trade_volume_trend(country_a, country_b)


# =========================================================
# RANKINGS
# =========================================================

@router.get("/rankings/influence")
def influence_ranking(
    limit: int = Query(default=20, ge=1, le=200),
) -> list[dict[str, Any]]:
    """
    Countries ranked by economic influence score (descending).
    """
    return get_top_economies(limit)


@router.get("/rankings/trade-vulnerability")
def trade_vulnerability_ranking(
    limit: int = Query(default=20, ge=1, le=200),
) -> list[dict[str, Any]]:
    """
    Countries ranked by trade vulnerability (most vulnerable first).
    """
    return get_most_trade_vulnerable(limit)


@router.get("/rankings/energy-vulnerability")
def energy_vulnerability_ranking(
    limit: int = Query(default=20, ge=1, le=200),
) -> list[dict[str, Any]]:
    """
    Countries ranked by energy vulnerability (most vulnerable first).
    """
    return get_most_energy_vulnerable(limit)


@router.get("/rankings/inflation")
def inflation_ranking(
    order: str = Query(default="worst", pattern="^(worst|best)$"),
    limit: int = Query(default=20, ge=1, le=200),
) -> list[dict[str, Any]]:
    """
    Countries ranked by inflation stability.
    order=worst → most unstable first, order=best → most stable first.
    """
    return get_inflation_ranking(order, limit)


@router.get("/rankings/trade-surplus")
def trade_surplus_ranking(
    limit: int = Query(default=20, ge=1, le=200),
) -> list[dict[str, Any]]:
    """
    Countries ranked by trade balance health score (descending).
    """
    return get_trade_surplus_ranking(limit)


@router.get("/rankings/energy-exporters")
def energy_exporters_ranking(
    year: int = Query(default=2024, ge=2000, le=2030),
    limit: int = Query(default=10, ge=1, le=50),
) -> list[dict[str, Any]]:
    """
    Countries ranked by total energy export value for a given year.
    """
    return get_major_energy_exporters(year, limit)


# =========================================================
# TRADE VOLUME
# =========================================================

@router.get("/trade-pairs")
def top_trade_pairs(
    year: int = Query(default=2024, ge=2018, le=2030),
    limit: int = Query(default=20, ge=1, le=100),
) -> list[dict[str, Any]]:
    """
    Highest bilateral trade volume pairs for a given year.
    """
    return get_top_trade_pairs(year, limit)


@router.get("/partners-network")
def major_partners_network(
    year: int = Query(default=2024, ge=2018, le=2030),
) -> list[dict[str, Any]]:
    """
    Top export partner relationships (rank 1 only) across all countries.
    Useful for force-directed graph visualization.
    """
    return get_major_partners_network(year)


# =========================================================
# COMPARISON
# =========================================================

@router.get("/compare/gdp")
def gdp_comparison(
    countries: str = Query(
        description="Comma-separated country names, e.g. India,China,Germany"
    ),
    year: int = Query(default=2024, ge=2000, le=2030),
) -> list[dict[str, Any]]:
    """
    GDP comparison across multiple countries for a given year.
    """
    country_list = [c.strip() for c in countries.split(",") if c.strip()]
    if not country_list:
        raise HTTPException(status_code=400, detail="Provide at least one country name")
    return get_gdp_comparison(country_list, year)


# =========================================================
# SEARCH
# =========================================================

@router.get("/search")
def search(
    q: str = Query(description="Partial country name"),
    limit: int = Query(default=10, ge=1, le=50),
) -> list[dict[str, Any]]:
    """
    Search countries by partial name match.
    Returns matches sorted by economic influence score.
    Useful for autocomplete.
    """
    if len(q) < 2:
        raise HTTPException(
            status_code=400,
            detail="Query must be at least 2 characters",
        )
    return search_countries(q, limit)