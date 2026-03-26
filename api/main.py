import sys
sys.path.insert(0, ".")
 
from dotenv import load_dotenv
load_dotenv()
 
from fastapi import FastAPI
 
app = FastAPI(
    title="Intelligence Engine API",
    description="Economy, Climate, Defense, and Geopolitics modules.",
    version="0.1.0",
)
 
# ── Economy ───────────────────────────────────────────────────────────────────
try:
    from modules.economy.routes import router as economy_router
    app.include_router(economy_router)
    print("✓ Economy routes loaded")
except Exception as e:
    print(f"✗ Economy routes failed: {e}")
 
#── Climate ───────────────────────────────────────────────────────────────────
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

# ── Composite ───────────────────────────────────────────────────────────────
try:
    from analytics.composite.routes import router as composite_router
    app.include_router(composite_router)
    print("✓ Composite routes loaded")
except Exception as e:
    print(f"✗ Composite routes failed: {e}")
 
 
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)