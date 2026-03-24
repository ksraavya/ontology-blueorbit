from __future__ import annotations 
from fastapi import FastAPI, HTTPException 
from fastapi.middleware.cors import CORSMiddleware 
 
app = FastAPI( 
    title="Global Intelligence Engine", 
    description="Multi-domain geopolitical intelligence — Defence, Economy, Geopolitics, Climate", 
    version="2.0.0", 
) 
 
app.add_middleware( 
    CORSMiddleware, 
    allow_origins=["*"], 
    allow_credentials=True, 
    allow_methods=["*"], 
    allow_headers=["*"], 
) 
 
# ── Defence (always available) ─────────────────────────────────────────────── 
from modules.defense.routes import router as defense_router 
app.include_router(defense_router) 
 
# ── Economy (include when teammate's routes file is ready) ─────────────────── 
try: 
    from modules.economy.routes import router as economy_router 
    app.include_router(economy_router) 
except (ImportError, Exception): 
    pass  # Economy routes not yet available 
 
# ── Geopolitics (include when teammate's routes file is ready) ─────────────── 
try: 
    from modules.geopolitics.routes import router as geopolitics_router 
    app.include_router(geopolitics_router) 
except (ImportError, Exception): 
    pass  # Geopolitics routes not yet available 
 
 
# ── Shared endpoints (cross-module) ────────────────────────────────────────── 
 
@app.get("/", tags=["root"]) 
def root(): 
    return { 
        "status": "running", 
        "version": "2.0.0", 
        "docs": "/docs", 
        "health": "/health", 
        "modules_active": ["defence"], 
        "modules_pending": ["economy", "geopolitics", "climate"], 
    } 
 
 
@app.get("/health", tags=["root"]) 
def health(): 
    """Checks Neo4j connection and returns basic graph stats.""" 
    from common.db import Neo4jConnection 
    try: 
        conn = Neo4jConnection() 
        result = conn.run_query( 
            "MATCH (n) RETURN count(n) AS total_nodes LIMIT 1" 
        ) 
        conn.close() 
        return { 
            "status": "healthy", 
            "neo4j": "connected", 
            "total_nodes": result[0]["total_nodes"] if result else 0, 
        } 
    except Exception as e: 
        return {"status": "degraded", "neo4j": str(e)} 
 
 
@app.get("/graph/summary", tags=["root"]) 
def graph_summary(): 
    """ 
    Full inventory of all relationship types and counts. 
    Useful for team to verify all modules loaded correctly. 
    """ 
    from common.db import Neo4jConnection 
    try: 
        conn = Neo4jConnection() 
        result = conn.run_query(""" 
            MATCH ()-[r]->() 
            WITH type(r) AS rel, count(r) AS cnt 
            RETURN rel, cnt ORDER BY cnt DESC 
        """) 
        conn.close() 
        return {"relationships": result} 
    except Exception as e: 
        raise HTTPException(status_code=500, detail=str(e)) 
 
 
@app.get("/simulator/profiles", tags=["simulator"]) 
def simulator_profiles(limit: int = 261): 
    """ 
    Cross-module simulator-ready profiles combining defence + geopolitics scores. 
    Returns every country with a defense_composite_score or military_strength_score. 
    This is the primary endpoint for the simulation layer. 
    """ 
    from common.db import Neo4jConnection 
    try: 
        conn = Neo4jConnection() 
        result = conn.run_query(""" 
            MATCH (c:Country) 
            WHERE c.defense_composite_score IS NOT NULL 
               OR c.military_strength_score IS NOT NULL 
            OPTIONAL MATCH (c)-[:MEMBER_OF]->(a:Alliance) 
            OPTIONAL MATCH (c)-[:BELONGS_TO]->(reg:Region) 
            WITH c, reg, collect(DISTINCT a.name) AS alliances 
            RETURN c.name AS country, 
                   round(coalesce(c.defense_composite_score, 0.0), 4) 
                       AS defense_composite, 
                   round(coalesce(c.military_strength_score, 0.0), 4) 
                       AS military_strength, 
                   round(coalesce(c.conflict_risk_score, 0.0), 4) 
                       AS conflict_risk, 
                   round(coalesce(c.defense_spending_score, 0.0), 4) 
                       AS defense_spending, 
                   round(coalesce(c.arms_export_score, 0.0), 4) 
                       AS arms_export, 
                   round(coalesce(c.defense_burden_score, 0.0), 4) 
                       AS defense_burden, 
                   round(coalesce(c.live_risk_score, 0.0), 4) 
                       AS live_risk, 
                   coalesce(round(c.centrality, 4), 0.0) 
                       AS diplomatic_centrality, 
                   c.bloc_id AS bloc, 
                   c.nuclear_status AS nuclear, 
                   c.un_p5 AS p5, 
                   c.is_regional_power AS regional_power, 
                   alliances, 
                   reg.name AS region, 
                   c.conflict_fatality_trend AS conflict_trend 
            ORDER BY c.defense_composite_score DESC 
            LIMIT $limit 
        """, {"limit": limit}) 
        conn.close() 
        return {"total": len(result), "profiles": result} 
    except Exception as e: 
        raise HTTPException(status_code=500, detail=str(e)) 
 
 
@app.post("/defense/analytics/run", tags=["admin"]) 
def trigger_analytics(): 
    """ 
    Re-runs all defence analytics in the background. 
    Call this after running the defence pipeline to refresh scores. 
    Returns immediately — check logs for completion. 
    """ 
    import threading 
    from analytics.defense.runner import run_defense_analytics 
    try: 
        thread = threading.Thread(target=run_defense_analytics, daemon=True) 
        thread.start() 
        return {"status": "analytics recomputation started in background"} 
    except Exception as e: 
        raise HTTPException(status_code=500, detail=str(e)) 
