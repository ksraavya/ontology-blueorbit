from __future__ import annotations

from fastapi import APIRouter, HTTPException

from simulation.engine import run_simulation, parse_intent
from simulation.models import SimulateRequest, SimulateResponse
from simulation.registry import list_available

router = APIRouter(prefix="/simulate", tags=["simulation"])


@router.post("/", response_model=SimulateResponse)
def simulate(request: SimulateRequest) -> SimulateResponse:
    """
    Run a geopolitical scenario simulation from a natural language query.

    The engine will:
    1. Use Claude to classify the scenario type and extract entities
    2. Read current state from the Neo4j graph (read-only — no writes)
    3. Compute projected score deltas for every affected country
    4. Use Claude to generate an analytical intelligence brief

    **Example queries:**
    - `"What if the US sanctions China?"`
    - `"What if Russia cuts gas to Europe?"`
    - `"What if Turkey leaves NATO and aligns with Russia?"`
    - `"What if there is a major earthquake in Japan?"`
    - `"Which countries are most fragile right now?"`
    - `"What if India joins RCEP?"`
    - `"What if China and the US enter a trade war?"`
    - `"What if North Korea conducts a nuclear test?"`

    **magnitude guide:**
    - `0.5` = partial / limited
    - `1.0` = full / normal (default)
    - `1.5` = severe / major
    - `2.0` = extreme / catastrophic
    """
    try:
        return run_simulation(request)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/scenarios")
def list_scenarios() -> dict:
    """
    List all scenario types that have a registered simulation handler.

    Returns the full list of supported scenario keywords so you know
    what kinds of queries the engine can handle.
    """
    available = list_available()
    return {
        "available": available,
        "total":     len(available),
    }


@router.post("/parse-only")
def parse_only(request: SimulateRequest) -> dict:
    """
    Parse the intent of a query **without** running the simulation.

    Useful for debugging: shows how the engine interprets your natural
    language query before it routes to a scenario handler.

    Returns the parsed ScenarioRequest fields:
    scenario_type, actor, target, third_parties, magnitude, year, extra_params.
    """
    try:
        parsed = parse_intent(request.query, request.year, request.magnitude)
        return parsed.model_dump()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))