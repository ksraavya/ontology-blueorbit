from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from analytics.defense.queries import (
    # Original 5 (copied from modules/defense/analytics.py)
    get_conflict_summary,
    get_most_conflict_prone,
    get_spending_trend,
    get_top_arms_exporters,
    get_top_defense_spenders,
    # New functions
    get_country_defense_profile,
    get_military_comparison,
    get_alliance_network,
    get_top_arms_importers,
    get_arms_import_profile,
    get_threat_classification,
    get_simulator_ready_countries,
)

router = APIRouter(prefix="/defense", tags=["defense"])


# ══════════════════════════════════════════════════════════════════════════════
# ORIGINAL ENDPOINTS  (unchanged — same paths, same behaviour)
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/spending/top")
def spending_top(limit: int = 10) -> Dict[str, Any]:
    """Top N countries by latest-year defence spending."""
    try:
        result: List[Dict[str, Any]] = get_top_defense_spenders(limit)
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/arms/top")
def arms_top(limit: int = 10) -> Dict[str, Any]:
    """Top N arms exporters by peak year-level market share."""
    try:
        result: List[Dict[str, Any]] = get_top_arms_exporters(limit)
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/conflicts/top")
def conflicts_top(limit: int = 10) -> Dict[str, Any]:
    """Top N most conflict-affected countries."""
    try:
        result: List[Dict[str, Any]] = get_most_conflict_prone(limit)
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/spending/{country}")
def spending(country: str) -> Dict[str, Any]:
    """Year-by-year defence spending trend for a country."""
    try:
        result = get_spending_trend(country)
        if not result:
            raise HTTPException(status_code=404, detail="Country not found")
        return {"country": country, "data": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/conflicts/{country}")
def conflicts(country: str) -> Dict[str, Any]:
    """Year-by-year conflict statistics for a country."""
    try:
        result = get_conflict_summary(country)
        if not result:
            raise HTTPException(status_code=404, detail="Country not found")
        return {"country": country, "data": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/live/news")
def live_news(limit: int = 20):
    """Live defence news articles (requires live pipeline to have run)."""
    try:
        from common.db import Neo4jConnection
        conn = Neo4jConnection()
        query = """
        MATCH (c:Country)-[:MENTIONED_IN]->(n:NewsArticle)
        RETURN c.name AS country,
               n.title AS title,
               n.source AS source,
               n.published AS published,
               n.keyword AS keyword
        ORDER BY n.published DESC
        LIMIT $limit
        """
        result = conn.run_query(query, {"limit": limit})
        conn.close()
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════════════════
# NEW ENDPOINTS  — analytics scores + IMPORTS_ARMS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/profile/{country}")
def country_profile(country: str) -> Dict[str, Any]:
    """
    Full simulator-ready intelligence profile for a country.
    Includes all analytics scores, latest raw metrics, alliances, region,
    live risk, hostile co-mentions, and arms import dependency.
    """
    try:
        result = get_country_defense_profile(country)
        if not result:
            raise HTTPException(status_code=404, detail="Country not found or no analytics scores computed")
        return {"country": country, "profile": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/comparison")
def military_comparison(
    countries: Optional[str] = Query(
        default=None,
        description="Comma-separated country names to compare. "
                    "If omitted, returns top 15 by composite threat score."
    ),
    limit: int = 15
) -> Dict[str, Any]:
    """
    Side-by-side military intelligence comparison across countries.
    Returns all analytics scores plus arms export/import market share percentages.

    Example: /defense/comparison?countries=United States,China,Russia
    """
    try:
        country_list = (
            [c.strip() for c in countries.split(",") if c.strip()]
            if countries else None
        )
        result = get_military_comparison(countries=country_list, limit=limit)
        return {
            "countries_compared": len(result),
            "data": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alliances")
def alliances_overview() -> Dict[str, Any]:
    """
    All alliances with aggregate defence statistics for their members.
    Shows total spending, average threat score, nuclear member count.
    """
    try:
        result = get_alliance_network()
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alliances/{alliance_name}")
def alliance_detail(alliance_name: str) -> Dict[str, Any]:
    """
    Per-member defence intelligence for a specific alliance.
    Example: /defense/alliances/NATO
    """
    try:
        result = get_alliance_network(alliance=alliance_name)
        if not result:
            raise HTTPException(status_code=404, detail=f"Alliance '{alliance_name}' not found")
        return {"alliance": alliance_name, "members": len(result), "data": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/arms/importers")
def arms_importers(limit: int = 10) -> Dict[str, Any]:
    """
    Top N arms importers by peak annual market share across all periods (1950-2025).
    """
    try:
        result = get_top_arms_importers(limit)
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/arms/imports/{country}")
def arms_imports_history(country: str) -> Dict[str, Any]:
    """
    Full arms import history for a specific country (1950-2025 where available).
    Shows year, period, TIV value, and annual market share percentage.
    """
    try:
        result = get_arms_import_profile(country)
        if not result:
            raise HTTPException(status_code=404, detail="Country not found or no arms import data")
        return {"country": country, "data": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/threat/classification")
def threat_classification(
    year: int = Query(default=2022, description="Year for GDP/spending cross-module query"),
    limit: int = 20
) -> Dict[str, Any]:
    """
    Three-module threat classification combining defence spending % of GDP,
    political system type, and composite threat score.
    Requires Economy + Geopolitics module data to be loaded.
    """
    try:
        result = get_threat_classification(year=year, limit=limit)
        return {"year": year, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/simulator/ready")
def simulator_ready(limit: int = 50) -> Dict[str, Any]:
    """
    All countries with analytics scores populated and ready for the simulator.
    live_risk_score is included if available but not required.
    Returns up to `limit` countries ordered by composite threat score.
    """
    try:
        result = get_simulator_ready_countries(limit=limit)
        return {
            "total_ready": len(result),
            "data": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
