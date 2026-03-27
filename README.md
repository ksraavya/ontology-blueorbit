# 🌌 Ontology BlueOrbit

**Ontology BlueOrbit** is a geopolitical intelligence platform that unifies economy, defense, climate, and geopolitics signals into a graph-driven API and scenario simulation engine.  
It helps teams answer questions like **“What happens if one country sanctions another?”** using connected data and AI-assisted projections.

## 🧰 Tech Stack

| Category | Technologies |
|---|---|
| Language | Python 3.11+ |
| API Framework | FastAPI, Uvicorn |
| Database | Neo4j (graph database) |
| Data & Analytics | pandas, scipy, networkx, pyarrow |
| AI / LLM | google-genai (Gemini), OpenRouter fallback |
| Data I/O | openpyxl, requests, httpx, feedparser |
| Config | python-dotenv |

## ✨ Core Features

- Multi-domain analytics APIs for **economy**, **defense**, **geopolitics**, **climate**, and **composite risk**.
- Graph-backed country profiles, rankings, bilateral comparisons, and network views.
- Scenario simulation API (`/simulate`) for natural-language “what-if” analysis.
- Data pipelines for ingesting and transforming geopolitical datasets into Neo4j-ready structures.
- Shared ontology and intelligence utilities for relationship modeling and score aggregation.

## 🗂️ Project Structure

```text
ontology-blueorbit/
├── api/                    # FastAPI app entrypoint
├── analytics/              # Domain analytics queries, scoring, and routes
│   ├── climate/
│   ├── composite/
│   ├── defense/
│   ├── economy/
│   └── geopolitics/
├── common/                 # Shared DB, config, ontology, graph utilities
├── data/
│   ├── raw/                # Source datasets (xlsx/csv)
│   └── processed/          # Processed/cached datasets
├── modules/                # Domain data pipelines and API routes
│   ├── climate/
│   ├── defense/
│   ├── economy/
│   └── geopolitics/
├── scripts/                # Operational scripts (pipeline, DB checks)
└── simulation/             # Scenario engine, models, handlers, API routes
```

## 📦 Prerequisites & Dependencies

Before running locally, install:

- **Python 3.11+** (recommended)
- **Neo4j** instance (local or cloud)
- `pip` for Python package management

Python dependencies are listed in:

- `requirements.txt`

Environment variables (required for full functionality):

- `NEO4J_URI`
- `NEO4J_USER`
- `NEO4J_PASSWORD`
- `GEMINI_API_KEY` (for Gemini-powered simulation)
- `OPENROUTER_API_KEY` (optional fallback for LLM calls)

## 🚀 Getting Started

### 1) Clone the repository

```bash
git clone https://github.com/ksraavya/ontology-blueorbit.git
cd ontology-blueorbit
```

### 2) Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate        # macOS/Linux
```

Windows (PowerShell):

```powershell
.venv\Scripts\Activate.ps1
```

### 3) Install dependencies

```bash
pip install -r requirements.txt
```

### 4) Configure environment variables

Create a `.env` file in the project root:

```env
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
GEMINI_API_KEY=your_gemini_key
OPENROUTER_API_KEY=your_openrouter_key
```

### 5) (Optional) Run ingestion pipeline

```bash
python scripts/run_all.py
```

### 6) Start the API

```bash
python api/main.py
# or
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

## 🧪 Usage

Once the API is running, open:

- Swagger UI: `http://localhost:8000/docs`

Quick examples:

```bash
# Economy profile
curl "http://localhost:8000/economy/country/India?year=2024"

# Scenario simulation
curl -X POST "http://localhost:8000/simulate/" \
  -H "Content-Type: application/json" \
  -d '{"query":"What if the US sanctions China?","year":2024,"magnitude":1.0}'
```

---
## 👥 Meet the Team

| Module | Core Responsibility | Lead Architect | GitHub |
| :--- | :--- | :--- | :---: |
| **📈 Economy** | Trade dependencies, GDP impact analysis, and economic vulnerability. | **Sraavya** | [![GH](https://img.shields.io/badge/GitHub-181717?logo=github&logoColor=white)](https://github.com/ksraavya) |
| **🌪️ Climate** | Natural disaster modeling, resource scarcity, and migration risks. | **Bhumi** | [![GH](https://img.shields.io/badge/GitHub-181717?logo=github&logoColor=white)](https://github.com/Tbhumi04) |
| **🌍 Geopolitics** | **System Integration (`engine.py`)**, Alliance mapping, and Intent Parsing. | **Manav** | [![GH](https://img.shields.io/badge/GitHub-181717?logo=github&logoColor=white)](https://github.com/manav329) |
| **🛡️ Defence** | Military strength indexing, arms trade flows, and kinetic conflict escalation. | **Aditya** | [![GH](https://img.shields.io/badge/GitHub-181717?logo=github&logoColor=white)](https://github.com/Aaadddiii6) |

---

<p align="center">
  Made with ❤️ + ☕ by <b>Team BlueOrbit</b>
</p>

If you are using this project for research or product integration, start with `/docs`, then explore domain-specific endpoints under `/economy`, `/defense`, `/geopolitics`, `/climate`, `/composite`, and `/simulate`.
