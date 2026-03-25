from __future__ import annotations

from fastapi import FastAPI

from modules.defense.routes import router as defense_router

app = FastAPI(
    title="Global Ontology Engine",
    description="Multi-domain intelligence system",
    version="1.0.0"
)

app.include_router(defense_router)

@app.get("/")
def root():
    return {"status": "running", "module": "defence"}