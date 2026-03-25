from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException

from modules.defense.analytics import (
    get_conflict_summary,
    get_most_conflict_prone,
    get_spending_trend,
    get_top_arms_exporters,
    get_top_defense_spenders,
)

router = APIRouter(prefix="/defense", tags=["defense"])





@router.get("/spending/top")
def spending_top(limit: int = 10) -> Dict[str, Any]:
    try:
        result: List[Dict[str, Any]] = get_top_defense_spenders(limit)
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/arms/top")
def arms_top(limit: int = 10) -> Dict[str, Any]:
    try:
        result: List[Dict[str, Any]] = get_top_arms_exporters(limit)
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/conflicts/top")
def conflicts_top(limit: int = 10) -> Dict[str, Any]:
    try:
        result: List[Dict[str, Any]] = get_most_conflict_prone(limit)
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/live/news")
def live_news(limit: int = 20):
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



@router.get("/spending/{country}")
def spending(country: str) -> Dict[str, Any]:
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
    try:
        result = get_conflict_summary(country)
        if not result:
            raise HTTPException(status_code=404, detail="Country not found")
        return {"country": country, "data": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

