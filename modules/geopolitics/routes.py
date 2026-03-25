from fastapi import APIRouter, HTTPException
from analytics.geopolitics.queries import (
    get_country_geopolitics_profile,
    get_diplomatic_network,
    get_blocs,
    get_top_central_countries,
    get_bilateral_geopolitics,
    get_sanctions_network,
)

router = APIRouter(prefix="/geopolitics", tags=["geopolitics"])


@router.get("/country/{name}")
def geopolitics_profile(name: str):
    result = get_country_geopolitics_profile(name)
    if not result:
        raise HTTPException(status_code=404, detail="Country not found")
    return result


@router.get("/network")
def diplomatic_network(min_score: float = 0.3):
    return get_diplomatic_network(min_score)


@router.get("/blocs")
def blocs():
    return get_blocs()


@router.get("/rankings/centrality")
def centrality_ranking(limit: int = 20):
    return get_top_central_countries(limit)


@router.get("/bilateral/{country_a}/{country_b}")
def bilateral(country_a: str, country_b: str):
    return get_bilateral_geopolitics(country_a, country_b)


@router.get("/sanctions/{name}")
def sanctions(name: str):
    return get_sanctions_network(name)