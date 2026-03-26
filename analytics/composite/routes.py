from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from analytics.composite.queries import (
    compare_countries,
    get_composite_scores_coverage,
    get_country_composite_profile,
    get_global_risk_ranking,
    get_influence_network,
    get_most_exposed_countries,
    get_risk_vs_influence_matrix,
    get_strategic_influence_ranking,
    get_vulnerability_ranking,
)

router = APIRouter(prefix="/composite", tags=["composite"])


# =========================================================
# COUNTRY PROFILE
# =========================================================

@router.get("/country/{name}")
def composite_profile(name: str) -> dict[str, Any]:
    """
    Full composite intelligence profile for a single country.
    Combines global risk, strategic influence, overall vulnerability,
    and all component score breakdowns.
    """
    result = get_country_composite_profile(name)
    if not result:
        raise HTTPException(status_code=404, detail=f"Country '{name}' not found")
    return result


# =========================================================
# RANKINGS
# =========================================================

@router.get("/rankings/global-risk")
def global_risk_ranking(
    limit: int = Query(default=30, ge=1, le=200),
) -> list[dict[str, Any]]:
    """
    Countries ranked by global_risk_score (trade × 0.4 + defense × 0.3 + climate × 0.3).
    """
    return get_global_risk_ranking(limit)


@router.get("/rankings/influence")
def strategic_influence_ranking(
    limit: int = Query(default=30, ge=1, le=200),
) -> list[dict[str, Any]]:
    """
    Countries ranked by strategic_influence_score
    (economic × 0.4 + military × 0.3 + geopolitical × 0.3).
    """
    return get_strategic_influence_ranking(limit)


@router.get("/rankings/vulnerability")
def vulnerability_ranking(
    limit: int = Query(default=30, ge=1, le=200),
    primary: str | None = Query(
        default=None,
        description="Filter by primary vulnerability: trade | energy | conflict | climate",
    ),
) -> list[dict[str, Any]]:
    """
    Countries ranked by overall_vulnerability_score (max of four dimensions).
    Optionally filter by primary_vulnerability dimension.
    """
    valid_primaries = {"trade", "energy", "conflict", "climate", None}
    if primary not in valid_primaries:
        raise HTTPException(
            status_code=400,
            detail=f"primary must be one of: trade, energy, conflict, climate",
        )
    return get_vulnerability_ranking(limit, primary)


@router.get("/rankings/most-exposed")
def most_exposed(
    limit: int = Query(default=20, ge=1, le=100),
) -> list[dict[str, Any]]:
    """
    Countries with the highest combined vulnerability + global risk score.
    These are the most exposed states in the system.
    """
    return get_most_exposed_countries(limit)


# =========================================================
# ANALYSIS
# =========================================================

@router.get("/matrix")
def risk_influence_matrix(
    limit: int = Query(default=50, ge=1, le=261),
) -> list[dict[str, Any]]:
    """
    2D risk vs influence matrix with quadrant classification:
    - stable_power:    high influence, low risk
    - contested_power: high influence, high risk
    - fragile_state:   low influence, high risk
    - stable_minor:    low influence, low risk
    """
    return get_risk_vs_influence_matrix(limit)


@router.get("/influence-network")
def influence_network(
    year: int = Query(default=2024, ge=2018, le=2030),
    limit: int = Query(default=50, ge=1, le=200),
) -> list[dict[str, Any]]:
    """
    IS_INFLUENTIAL_TO edges — who influences whom.
    Useful for force-directed graph visualization.
    """
    return get_influence_network(year, limit)


@router.get("/compare")
def compare(
    countries: str = Query(
        description="Comma-separated country names, e.g. India,China,United States"
    ),
) -> list[dict[str, Any]]:
    """
    Side-by-side composite score comparison for multiple countries.
    """
    country_list = [c.strip() for c in countries.split(",") if c.strip()]
    if len(country_list) < 2:
        raise HTTPException(
            status_code=400,
            detail="Provide at least 2 country names separated by commas",
        )
    if len(country_list) > 20:
        raise HTTPException(
            status_code=400,
            detail="Maximum 20 countries per comparison",
        )
    return compare_countries(country_list)


# =========================================================
# DIAGNOSTICS
# =========================================================

@router.get("/coverage")
def scores_coverage() -> dict[str, Any]:
    """
    Diagnostic endpoint showing how many countries have each composite score.
    Use this to verify all module analytics ran before composite analytics.
    """
    return get_composite_scores_coverage()


@router.post("/run", tags=["admin"])
def trigger_composite_analytics(year: int = Query(default=2024)) -> dict[str, Any]:
    """
    Re-run all composite analytics in the background.
    Only call this after all four module analytics have completed.
    Returns immediately — check logs for completion.
    """
    import threading
    from analytics.composite.runner import run

    try:
        thread = threading.Thread(target=run, args=(year,), daemon=True)
        thread.start()
        return {
            "status": "composite analytics started in background",
            "year":   year,
            "note":   "requires economy + defense + geopolitics + climate analytics to have run first",
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))