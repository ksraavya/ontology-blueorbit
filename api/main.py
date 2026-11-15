import sys 
sys.path.insert(0, ".") 

# Windows consoles default to cp1252, which cannot print the ✓/✗ status
# lines below; when output is piped to a file that crashed startup.
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")
 
from dotenv import load_dotenv 
load_dotenv() 
 
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI 
from fastapi.middleware.cors import CORSMiddleware 

from common.db import close_driver


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    close_driver()  # one shared Neo4j driver per process; close it on shutdown
 
app = FastAPI( 
    title="Intelligence Engine API", 
    description=( 
        "Geopolitical Intelligence Platform — Economy, Climate, Defense, " 
        "Geopolitics, and Scenario Simulation modules." 
    ), 
    version="0.3.0", 
    lifespan=lifespan,
) 
 
# CORS: the Next.js frontend calls the API through its own server
# (rewrites + server actions), so browsers rarely hit this directly.
# CORS_ORIGINS="https://your-app.vercel.app,http://localhost:3000" to restrict.
_origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    # Browsers reject credentials with a wildcard origin; no cookies are used.
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Health / data status ──────────────────────────────────────────────────────
try:
    from api.health import router as health_router
    app.include_router(health_router)
    print("✓ Health routes loaded")
except Exception as e:
    print(f"✗ Health routes failed: {e}")
 
# ── Graph layers (map lines + choropleth metrics) ─────────────────────────────
try:
    from api.graph_layers import router as graph_layers_router
    app.include_router(graph_layers_router)
    print("✓ Graph layer routes loaded")
except Exception as e:
    print(f"✗ Graph layer routes failed: {e}")

# ── Economy ─────────────────────────────────────────────────────────────────── 
try: 
    from modules.economy.routes import router as economy_router 
    app.include_router(economy_router) 
    print("✓ Economy routes loaded") 
except Exception as e: 
    print(f"✗ Economy routes failed: {e}") 
 
# ── Climate ─────────────────────────────────────────────────────────────────── 
try: 
    from analytics.climate.routes import router as climate_router 
    app.include_router(climate_router) 
    print("✓ Climate routes loaded") 
except Exception as e: 
    print(f"✗ Climate routes failed: {e}") 
 
# ── Defense ─────────────────────────────────────────────────────────────────── 
try: 
    from modules.defense.routes import router as defense_router 
    app.include_router(defense_router) 
    print("✓ Defense routes loaded") 
except Exception as e: 
    print(f"✗ Defense routes failed: {e}") 
 
# ── Geopolitics ─────────────────────────────────────────────────────────────── 
try: 
    from modules.geopolitics.routes import router as geopolitics_router 
    app.include_router(geopolitics_router) 
    print("✓ Geopolitics routes loaded") 
except Exception as e: 
    print(f"✗ Geopolitics routes failed: {e}") 
 
# ── Composite ───────────────────────────────────────────────────────────────── 
try: 
    from analytics.composite.routes import router as composite_router 
    app.include_router(composite_router) 
    print("✓ Composite routes loaded") 
except Exception as e: 
    print(f"✗ Composite routes failed: {e}") 
 
# ── Simulation ──────────────────────────────────────────────────────────────── 
try: 
    from simulation.api.routes import router as simulation_router 
    app.include_router(simulation_router) 
    print("✓ Simulation routes loaded") 
except Exception as e: 
    print(f"✗ Simulation routes failed: {e}") 
 
 
if __name__ == "__main__": 
    import uvicorn 
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)