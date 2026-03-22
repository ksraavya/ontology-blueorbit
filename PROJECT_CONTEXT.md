# PROJECT CONTEXT REPORT

## Global Ontology Engine — Defence Module

### Last Updated: March 21, 2026

---

## WHAT THIS PROJECT IS

A multi-module intelligence system that builds a shared knowledge graph in Neo4j. Four team members each own one domain module. All modules write to the same Neo4j database. Integration happens automatically because everyone uses shared `Country` and `Year` nodes via `MERGE`.

**Team modules:**

- Defence (you) — DONE (Refactored to 4-Layer Architecture)
- Economy/Trade — in progress
- Climate — in progress
- Geopolitics — in progress

---

## PROJECT STRUCTURE (4-LAYER ARCHITECTURE)

The Defence module follows a decoupled 4-layer pipeline for data processing:

```
ontology-blueorbit/
│
├── common/                        ← SHARED UTILITIES
│   ├── __init__.py                ← Package marker (Added)
│   ├── db.py                      ← Neo4j connection layer (Neo4jConnection)
│   ├── country_mapper.py          ← ISO normalization + custom mappings
│   ├── entity_mapper.py           ← Multi-entity normalization logic
│   ├── graph_ops.py               ← Standardized Neo4j operations
│   └── ontology.py                ← Global relationship definitions
│
├── modules/
│   └── defense/                   ← DEFENCE INTELLIGENCE MODULE
│       ├── __init__.py
│       ├── ingest.py              ← LAYER 1: Raw data reading (SIPRI, ACLED)
│       ├── transform.py           ← LAYER 2: Cleaning and entity mapping
│       ├── compute.py             ← LAYER 3: Metric calculation (trends, weights)
│       ├── load.py                ← LAYER 4: Persistence to Neo4j
│       ├── pipeline.py            ← Orchestrator for the 4-layer flow
│       │
│       ├── live/                  ← LIVE INTELLIGENCE PIPELINE
│       │   ├── acled_live.py      ← Live conflict events (API)
│       │   ├── gdelt_fetcher.py   ← Global news tracking (API)
│       │   ├── rss_fetcher.py     ← Defense industry news (RSS)
│       │   ├── apitube_fetcher.py ← Targeted high-quality news (API)
│       │   ├── news_enrichment.py ← Intelligence extraction (NLP-lite)
│       │   └── run_all_live.py    ← Master orchestrator for live updates
│       │
│       ├── graph_enrichment.py    ← Static enrichment (Regions/Alliances)
│       └── graph_enrichment_extended.py ← Geopolitical metadata (Nuclear/P5/Regional Powers)
│
├── api/
│   ├── __init__.py
│   └── main.py                    ← FastAPI app entry point
│
├── data/
│   ├── raw/                       ← Source files (SIPRI_Milex.xlsx, SIPRI_Arms.xlsx, ACLED.csv)
│   └── processed/                 ← Cleaned intermediate data
│
├── query_runner.py                ← Analytical query utility
├── verify_enrichment.py           ← Verification tool (Transient)
├── .env                           ← API keys and credentials
└── PROJECT_CONTEXT.md             ← (this file)
```

---

## GRAPH SCHEMA — DEFENCE MODULE

### 1. Core Historical Entities

```cypher
(Country {name}) -[:SPENDS_ON_DEFENSE {amount_usd_millions, normalized_weight, source}]-> (Year {year})
(Country {name}) -[:EXPORTS_ARMS {tiv_millions, dependency_score, source}]-> (Year {year})
(Country {name}) -[:HAS_CONFLICT_STATS {fatalities, events, fatality_trend, source}]-> (Year {year})
```

### 2. Live Intelligence Signals

```cypher
(Country {name}) -[:MENTIONED_IN]-> (NewsArticle {title, url, category, enriched, published})
(Country {name}) -[:HAS_CONFLICT_SIGNAL]-> (ConflictSignal {date, category, article_count})
(Country {name}) -[r:CO_MENTIONED_WITH {count, dominant_context}]-> (Country)
```

### 3. Geopolitical Metadata

```cypher
(Country) -[:BELONGS_TO]-> (Region)
(Country) -[:MEMBER_OF]-> (Alliance)
Country Properties: {nuclear_status, is_nuclear, un_p5, is_regional_power, live_risk_score}
```

---

## HOW TO RUN THINGS

**Run Historical Pipeline (4-Layer):**
`python -m modules.defense.pipeline`

**Run Live Intelligence Refresh (Ingest + Enrich):**
`python -m modules.defense.live.run_all_live`

**Update Geopolitical Metadata:**
`python -m modules.defense.graph_enrichment_extended`

**Start API:**
`uvicorn api.main:app --reload`

---

## ACHIEVEMENTS & RECENT CHANGES

- ✅ **Architectural Refactor**: Migrated from monolithic loaders to a 4-layer decoupled architecture (`Ingest` → `Transform` → `Compute` → `Load`).
- ✅ **News Intelligence Layer**: Built a non-LLM NLP pipeline in `news_enrichment.py` that classifies articles into categories (`conflict`, `diplomacy`, `arms_trade`, `escalation`) and generates `ConflictSignal` indicators.
- ✅ **Dynamic Risk Scoring**: Implemented a `live_risk_score` (0.0 - 1.0) that correlates real-time news spikes with historical fatality trends and defense capacity.
- ✅ **Geopolitical Enrichment**: Added metadata for Nuclear status, UN Security Council P5 membership, and Regional Power status to the knowledge graph.
- ✅ **Co-Mention Networking**: Developed a category-aware co-mention network that identifies `hostile` vs `cooperative` relationships between countries based on live news context.
- ✅ **Secondary Country Extraction**: Implemented batch-optimized alias and keyword matching to identify secondary countries in news titles, increasing network density.

---

## CONTEXT FOR NEW AI AGENTS

- **Normalization**: Use `common.entity_mapper` or `common.country_mapper` for all name resolution.
- **Neo4j Labels**: Use `NewsArticle` for live news articles.
- **Batching**: Always use `UNWIND` for large-scale database updates to maintain performance.
- **Risk Calculation**: The `live_risk_score` is weighted: 40% Live News Signal, 60% Historical Conflict Intensity.
- **Co-mentions**: Only pairs with `count >= 2` articles in the last 30 days are promoted to `CO_MENTIONED_WITH` relationships.
