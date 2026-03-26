from __future__ import annotations
import sys
sys.path.insert(0, '.')

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from analytics.climate.queries import (
    get_bilateral_climate,
    get_climate_risk_ranking,
    get_conflict_risk_propagation,
    get_countries_by_hazard,
    get_country_climate_profile,
    get_deadliest_events,
    get_disaster_damage,
    get_disaster_fatalities,
    get_disaster_history,
    get_earthquake_history,
    get_emissions_comparison,
    get_emissions_trend,
    get_hazard_risk,
    get_highest_seismic_risk,
    get_most_deforested,
    get_most_disaster_prone,
    get_multi_hazard_countries,
    get_people_affected,
    get_resource_dependencies,
    get_supply_chain_disruptions,
    get_temperature_ranking,
    get_top_emitters,
    search_countries_by_climate,
)

router = APIRouter(prefix="/climate", tags=["climate"])


# =========================================================
# COUNTRY PROFILE
# =========================================================

@router.get("/country/{name}")
def climate_profile(
    name: str,
    year: int = Query(default=2024, ge=2000, le=2030),
) -> dict[str, Any]:
    """
    Full climate intelligence profile for a country.
    Returns all scores, emissions, temperature, disaster summary,
    hazard risks, supply chain impact, and conflict risk.
    """
    result = get_country_climate_profile(name, year)
    if not result:
        raise HTTPException(status_code=404, detail=f"Country '{name}' not found")
    return result


@router.get("/country/{name}/disasters")
def disaster_history(
    name: str,
    start_year: int = Query(default=2000, ge=1900, le=2030),
    end_year: int = Query(default=2024, ge=1900, le=2030),
    disaster_type: str | None = Query(
        default=None,
        description="Flood | Drought | Storm | Earthquake | Wildfire | Tsunami | Landslide | Volcanic activity | Extreme temperature"
    ),
) -> list[dict[str, Any]]:
    """
    Disaster history for a country. Filter by disaster type and year range.
    """
    return get_disaster_history(name, start_year, end_year, disaster_type)


@router.get("/country/{name}/fatalities")
def disaster_fatalities(
    name: str,
    start_year: int = Query(default=2000, ge=1900, le=2030),
    end_year: int = Query(default=2024, ge=1900, le=2030),
) -> list[dict[str, Any]]:
    """
    Fatalities from climate events for a country across years.
    """
    return get_disaster_fatalities(name, start_year, end_year)


@router.get("/country/{name}/damage")
def disaster_damage(
    name: str,
    start_year: int = Query(default=2000, ge=1900, le=2030),
    end_year: int = Query(default=2024, ge=1900, le=2030),
) -> list[dict[str, Any]]:
    """
    Economic damage from climate events for a country.
    """
    return get_disaster_damage(name, start_year, end_year)


@router.get("/country/{name}/affected")
def people_affected(
    name: str,
    start_year: int = Query(default=2000, ge=1900, le=2030),
    end_year: int = Query(default=2024, ge=1900, le=2030),
) -> list[dict[str, Any]]:
    """
    People affected by climate events for a country.
    """
    return get_people_affected(name, start_year, end_year)


@router.get("/country/{name}/emissions")
def emissions_trend(
    name: str,
    start_year: int = Query(default=2014, ge=2000, le=2030),
    end_year: int = Query(default=2024, ge=2000, le=2030),
) -> list[dict[str, Any]]:
    """
    CO2 emissions and forest coverage trend for a country.
    """
    return get_emissions_trend(name, start_year, end_year)


@router.get("/country/{name}/hazard-risk")
def hazard_risk(name: str) -> list[dict[str, Any]]:
    """
    All hazard risk classifications for a country.
    """
    return get_hazard_risk(name)


@router.get("/country/{name}/earthquakes")
def earthquake_history(name: str) -> list[dict[str, Any]]:
    """
    USGS earthquake history for a country by year.
    """
    return get_earthquake_history(name)


@router.get("/country/{name}/supply-chain-impact")
def supply_chain_impact(
    name: str,
    limit: int = Query(default=20, ge=1, le=100),
) -> list[dict[str, Any]]:
    """
    Countries whose supply chains are disrupted by this country's climate events.
    """
    return get_supply_chain_disruptions(name, limit)


@router.get("/country/{name}/conflict-risk")
def conflict_risk(
    name: str,
    limit: int = Query(default=20, ge=1, le=100),
) -> list[dict[str, Any]]:
    """
    Countries at increased conflict risk due to this country's climate events.
    """
    return get_conflict_risk_propagation(name, limit)


@router.get("/country/{name}/resource-dependency")
def resource_dependency(
    name: str,
    limit: int = Query(default=20, ge=1, le=100),
) -> list[dict[str, Any]]:
    """
    Resource dependencies for a country (deforestation signal).
    """
    return get_resource_dependencies(name, limit)


# =========================================================
# BILATERAL
# =========================================================

@router.get("/bilateral/{country_a}/{country_b}")
def bilateral_climate(
    country_a: str,
    country_b: str,
) -> dict[str, Any]:
    """
    Full bilateral climate relationship between two countries.
    Includes shared hazards, mutual supply chain links, conflict risk.
    """
    return get_bilateral_climate(country_a, country_b)


# =========================================================
# RANKINGS
# =========================================================

@router.get("/rankings/climate-risk")
def climate_risk_ranking(
    limit: int = Query(default=20, ge=1, le=200),
) -> list[dict[str, Any]]:
    """
    Countries ranked by overall climate risk score (descending).
    """
    return get_climate_risk_ranking(limit)


@router.get("/rankings/disaster-prone")
def disaster_prone_ranking(
    limit: int = Query(default=20, ge=1, le=200),
) -> list[dict[str, Any]]:
    """
    Countries with highest disaster frequency score.
    """
    return get_most_disaster_prone(limit)


@router.get("/rankings/temperature")
def temperature_ranking(
    order: str = Query(default="hottest", pattern="^(hottest|coldest)$"),
    limit: int = Query(default=20, ge=1, le=200),
) -> list[dict[str, Any]]:
    """
    Countries ranked by mean temperature.
    order=hottest → highest first, order=coldest → lowest first.
    """
    return get_temperature_ranking(order, limit)


@router.get("/rankings/emitters")
def emitters_ranking(
    limit: int = Query(default=20, ge=1, le=200),
) -> list[dict[str, Any]]:
    """
    Countries ranked by CO2 per capita (highest first).
    """
    return get_top_emitters(limit)


@router.get("/rankings/deforested")
def deforestation_ranking(
    limit: int = Query(default=20, ge=1, le=200),
) -> list[dict[str, Any]]:
    """
    Countries with lowest forest coverage (most deforested first).
    """
    return get_most_deforested(limit)


@router.get("/rankings/seismic-risk")
def seismic_risk_ranking(
    limit: int = Query(default=20, ge=1, le=100),
) -> list[dict[str, Any]]:
    """
    Countries with highest earthquake risk scores.
    """
    return get_highest_seismic_risk(limit)


# =========================================================
# HAZARD ANALYSIS
# =========================================================

@router.get("/hazard/{hazard_type}")
def countries_by_hazard(
    hazard_type: str,
    risk_level: str = Query(default="High", pattern="^(High|Medium|Low)$"),
    limit: int = Query(default=30, ge=1, le=200),
) -> list[dict[str, Any]]:
    """
    Countries at given risk level for a specific hazard type.
    hazard_type: Flood | Drought | Cyclone | Earthquake
    """
    valid_hazards = {"Flood", "Drought", "Cyclone", "Earthquake"}
    if hazard_type not in valid_hazards:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid hazard_type. Must be one of: {valid_hazards}"
        )
    return get_countries_by_hazard(hazard_type, risk_level, limit)


@router.get("/hazard/multi-risk/countries")
def multi_hazard_countries(
    min_high_hazards: int = Query(default=2, ge=1, le=4),
    limit: int = Query(default=20, ge=1, le=100),
) -> list[dict[str, Any]]:
    """
    Countries with multiple High-level hazard risks simultaneously.
    """
    return get_multi_hazard_countries(min_high_hazards, limit)


# =========================================================
# GLOBAL EVENTS
# =========================================================

@router.get("/events/deadliest")
def deadliest_events(
    limit: int = Query(default=20, ge=1, le=100),
    start_year: int = Query(default=2000, ge=1900, le=2030),
    end_year: int = Query(default=2024, ge=1900, le=2030),
) -> list[dict[str, Any]]:
    """
    Deadliest climate events globally across a year range.
    """
    return get_deadliest_events(limit, start_year, end_year)


# =========================================================
# COUNTRY → COUNTRY IMPACT
# =========================================================

@router.get("/impact/supply-chain")
def global_supply_chain_disruptions(
    limit: int = Query(default=20, ge=1, le=100),
) -> list[dict[str, Any]]:
    """
    Top supply chain disruption pairs triggered by climate events globally.
    """
    return get_supply_chain_disruptions(None, limit)


@router.get("/impact/conflict-risk")
def global_conflict_risk(
    limit: int = Query(default=20, ge=1, le=100),
) -> list[dict[str, Any]]:
    """
    Top conflict risk propagation pairs triggered by climate events globally.
    """
    return get_conflict_risk_propagation(None, limit)


@router.get("/impact/resource-dependency")
def global_resource_dependencies(
    limit: int = Query(default=20, ge=1, le=100),
) -> list[dict[str, Any]]:
    """
    Top resource dependency edges globally (deforestation signal).
    """
    return get_resource_dependencies(None, limit)


# =========================================================
# COMPARISON
# =========================================================

@router.get("/compare/emissions")
def emissions_comparison(
    countries: str = Query(
        description="Comma-separated country names e.g. India,China,Germany"
    ),
    year: int = Query(default=2024, ge=2000, le=2030),
) -> list[dict[str, Any]]:
    """
    Emissions comparison across multiple countries for a given year.
    """
    country_list = [c.strip() for c in countries.split(",") if c.strip()]
    if not country_list:
        raise HTTPException(
            status_code=400,
            detail="Provide at least one country name"
        )
    return get_emissions_comparison(country_list, year)


# =========================================================
# SEARCH
# =========================================================

@router.get("/search")
def search(
    q: str = Query(description="Partial country name"),
    limit: int = Query(default=10, ge=1, le=50),
) -> list[dict[str, Any]]:
    """
    Search countries by partial name, returns climate risk score.
    """
    if len(q) < 2:
        raise HTTPException(
            status_code=400,
            detail="Query must be at least 2 characters",
        )
    return search_countries_by_climate(q, limit)