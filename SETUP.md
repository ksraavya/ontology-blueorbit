# Blue-Orbit: setup and rebuild

How to get the backend running from zero, rebuild the graph from raw data,
and keep a backup so a paused/deleted AuraDB never costs the data again.

## 1. Prerequisites

- Python **3.12** (3.14/3.15 lack wheels for some data libraries)
- Neo4j 5, either:
  - **Neo4j Desktop** (simplest on Windows): create a local DBMS, version 5.x, set a password, Start.
  - **Docker**: `docker compose up -d` in this folder (see `docker-compose.yml`).
  No plugins needed (no APOC / GDS).
- Neo4j Browser (comes with both) at http://localhost:7474 to look at the graph.

## 2. Install

```bash
py -3.12 -m venv .venv
.venv\Scripts\activate          # macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
copy .env.example .env          # then edit .env
```

Minimum `.env`: `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`.
`OPENAI_API_KEY` is optional. Without it the simulator still works (rule
parser + template brief); with it, free-text questions are parsed better and
briefs read better. Numbers never come from the LLM.

## 3. Put the data in place

```bash
python scripts/check_data.py --fix-names
```

Lists every dataset, where to download it, and whether it is present and has
the right columns. `--fix-names` renames raw downloads (BACI, GDELT, World
Bank) to the names the loaders expect. Defence data (SIPRI, ACLED) is already
in git.

| Folder | Files |
|---|---|
| `data/raw/` | `baci_trade_<year>.csv`, `country_codes.csv`, `wb_gdp.csv`, `wb_inflation.csv`, `ofac_sdn.csv`, `wto_rta.csv`, `vdem_core.csv`, `emdat_disasters.xlsx` |
| `data/raw/gdelt/` | `gdelt_<yyyymmdd>.CSV` |
| `data/raw/unga/` | `unga_votes_filtered.csv` |

## 4. Rebuild the graph

```bash
python scripts/rebuild_graph.py            # everything that has data
python scripts/rebuild_graph.py --list     # steps and what each needs
python scripts/rebuild_graph.py --from climate   # resume after a failure
python scripts/rebuild_graph.py --with-live      # also pull live news
```

Steps run in dependency order (defence → economy → geopolitics → climate →
analytics → composite). Missing data skips a step instead of failing. Loaders
use MERGE, so re-running is safe. The analytics year defaults to the latest
BACI year present, because trade data is published 1-2 years late.
Full log: `rebuild.log`.

Then regenerate the globe's relation lines from the real graph:

```bash
python scripts/export_relations.py --out ../frontend/src/lib/hardcodedRelations.ts
```

## 5. Run the API

```bash
uvicorn api.main:app --port 8000 --reload
```

- http://localhost:8000/docs — every endpoint
- http://localhost:8000/health — Neo4j status, what is loaded per domain, LLM status

## 6. Backups (do this after every successful rebuild)

The code is in git; the graph is not. Dump it and keep the file in Drive.

**Neo4j Desktop:** stop the DBMS → `…` menu → *Dump* → save `neo4j.dump`.

**Docker** (Community edition must be stopped to dump):

```bash
docker compose stop neo4j
docker run --rm -v backend_blueorbit_neo4j_data:/data -v %cd%/backups:/backups neo4j:5.26-community ^
  neo4j-admin database dump neo4j --to-path=/backups
docker compose start neo4j
```

**Restore / move to AuraDB:** AuraDB console → instance → *Import database* → upload the `.dump`.
AuraDB Free pauses after ~3 days idle and is deleted after ~30 days paused, so keep the dump.

## 7. Simulation engine: what uses AI and what does not

```
question ─► OpenAI ─► (Gemini / OpenRouter if keys set) ─► rule parser   = understanding
         ─► names matched to graph spelling ("Russia" → "Russian Federation")
         ─► year snapped to nearest year with trade data
         ─► scenario handler reads Neo4j, computes deltas                = ALL numbers
         ─► OpenAI brief, or template brief built from the numbers       = wording
```

A provider that hits its quota is skipped for 5 minutes (`LLM_COOLDOWN_SECONDS`).
The frontend's *Build scenario* mode sends `scenario_type`/`actor`/`target`
directly and needs no AI at all.
