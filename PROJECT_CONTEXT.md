# PROJECT CONTEXT REPORT

## Global Ontology Engine — Defence Module

### Last Updated: March 20, 2026

---

## WHAT THIS PROJECT IS

A multi-module intelligence system that builds a shared knowledge graph in Neo4j. Four team members each own one domain module. All modules write to the same Neo4j database. Integration happens automatically because everyone uses shared `Country` and `Year` nodes via `MERGE`.

**Team modules:**

- Defence (you) — DONE
- Economy/Trade — in progress
- Climate — in progress
- Geopolitics — in progress

---

## PROJECT STRUCTURE

```
ontology-blueorbit/
│
├── common/                        ← OWNED BY TEAM LEADER, DO NOT TOUCH
│   ├── __init__.py
│   ├── db.py                      ← Neo4j connection helper
│   ├── country_mapper.py          ← ISO normalization + custom mappings
│   ├── ontology.py                ← Global relationship definitions
│   └── config.py
│
├── modules/
│   └── defense/                   ← YOUR MODULE, FULLY BUILT
│       ├── __init__.py
│       ├── loaders/               ← Static dataset loaders
│       │   ├── milex_loader.py
│       │   ├── arms_loader.py
│       │   └── acled_loader.py
│       ├── live/                  ← Live data ingestion pipeline
│       │   ├── acled_live.py      ← Live conflict events (API)
│       │   ├── gdelt_fetcher.py   ← Global news tracking (API)
│       │   ├── rss_fetcher.py     ← Industry news (Defense News RSS)
│       │   ├── apitube_fetcher.py ← Targeted news (API)
│       │   └── run_all_live.py    ← Orchestrator for live updates
│       ├── cleaner.py             ← Data cleaning logic
│       ├── inserter.py            ← Neo4j insertion for static data
│       ├── analytics.py           ← Core analytical queries
│       ├── routes.py              ← FastAPI route definitions
│       ├── pipeline.py            ← Static data pipeline runner
│       ├── graph_enrichment.py    ← Adds Regions and Alliances
│       └── test_connection.py
│
├── api/
│   ├── __init__.py
│   └── main.py                    ← FastAPI app entry point
│
├── data/
│   ├── raw/                       ← Static XLSX/CSV source files
│   └── processed/                 ← Intermediate cleaned CSVs
│
├── query_runner.py                ← Terminal utility for Cypher queries
├── .env                           ← API keys and DB credentials
├── .gitignore
├── requirements.txt
└── PROJECT_CONTEXT.md             ← (this file)
```

---

## ENVIRONMENT & CREDENTIALS

**.env file requirements:**

```
NEO4J_URI=neo4j+ssc://cb841adb.databases.neo4j.io
NEO4J_USER=cb841adb
NEO4J_PASSWORD=...
ACLED_EMAIL=...
ACLED_PASSWORD=...
APITUBE_API_KEY=...
```

**Database:** Shared cloud instance on Neo4j AuraDB. All modules write to the same URI.

---

## GRAPH SCHEMA — DEFENCE MODULE

### 1. Static Entities & Relations

```
(Country {name}) -[:SPENDS_ON_DEFENSE {amount_usd_millions, source}]-> (Year {year})
(Country {name}) -[:EXPORTS_ARMS {tiv_millions, source}]-> (Year {year})
(Country {name}) -[:HAS_CONFLICT_STATS {fatalities, events, source}]-> (Year {year})
```

### 2. Live Entities & Relations

```
(Country {name}) -[:INVOLVED_IN]-> (ConflictEvent {event_id, type, date, fatalities})
(Country {name}) -[:MENTIONED_IN]-> (NewsArticle {title, url, source, published, keyword})
```

### 3. Enrichment Entities & Relations

```
(Country {name}) -[:BELONGS_TO]-> (Region {name})
(Country {name}) -[:MEMBER_OF]-> (Alliance {name})
```

---

## API ENDPOINTS

| Method | Endpoint                       | Description                             |
| ------ | ------------------------------ | --------------------------------------- |
| GET    | `/defense/spending/top`        | Top defense spenders (2023)             |
| GET    | `/defense/spending/{country}`  | Spending trend for a country            |
| GET    | `/defense/arms/top`            | Top arms exporters all time             |
| GET    | `/defense/conflicts/top`       | Most conflict-prone countries           |
| GET    | `/defense/conflicts/{country}` | Conflict stats for a country            |
| GET    | `/defense/live/news`           | Latest live news articles (all sources) |

---

## HOW TO RUN THINGS

**Run Static Pipeline:**
`python -m modules.defense.pipeline`

**Run Live Updates (ACLED, News):**
`python -m modules.defense.live.run_all_live`

**Enrich Graph (Regions/Alliances):**
`python -m modules.defense.graph_enrichment`

**Start API:**
`uvicorn api.main:app --reload`

---

## WHAT IS DONE

- ✅ **Static Ingestion**: SIPRI and ACLED historical data fully loaded.
- ✅ **Live Pipeline**: Real-time conflict and news ingestion (ACLED, GDELT, RSS, APITube).
- ✅ **Graph Enrichment**: Automated mapping of countries to Regions and Alliances.
- ✅ **Ontology & Mapping**: `common/` files updated with defense-specific relations and country aliases.
- ✅ **API & Analytics**: 6 endpoints and 5 core analytics functions built and tested.
- ✅ **Route Ordering**: Fixed bug where dynamic routes shadowed static ones.

## WHAT IS PENDING

- ⏳ **Cross-Module Integration**: Testing queries that join Defense with Economy/Trade once those modules are live.
- ⏳ **Temporal Analysis**: Building views that show news spikes correlated with conflict fatalities.
- ⏳ **Demo Dashboard**: Prepared Cypher queries for the final presentation.

---

## CONTEXT FOR NEW AI AGENTS

- **Shared Nodes**: Always use `MERGE` for `Country` and `Year`. These are the "glue" nodes connecting all modules.
- **Normalization**: Every country name **MUST** pass through `normalize_country()` from `common.country_mapper` before database entry.
- **Ontology**: Only use relationship names defined in `common.ontology`. New ones must be added there first.
- **Rate Limits**: GDELT and APITube have strict rate limits. The fetchers include `time.sleep()` to respect them. Do not remove these.
- **Imports**: Always run from project root using `python -m module.path`. Use the `sys.path` hack at the top of module files to ensure `common` is accessible.
