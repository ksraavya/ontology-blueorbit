# Deploying Blue-Orbit

Three pieces, deployed in this order (each one needs the address of the one before it):

```
1. Database   Neo4j AuraDB Free        (cloud copy of our local graph)
2. Backend    Render (FastAPI)          needs: database address + password
3. Frontend   Vercel (Next.js)          needs: backend address
```

All three have free tiers. Budget about 45 minutes the first time.

**Golden rule for secrets:** the OpenAI key and database password are typed only into the
Render / Vercel / Aura dashboards. Never into a file that gets committed, and never into a
variable starting with `NEXT_PUBLIC_` (that prefix publishes the value to every browser).

---

## Part 1 — Database (Neo4j AuraDB)

### What "exporting the database" means

Neo4j can pack a whole database into one file called a **dump** (`.dump`). It contains every
node, relationship, property and index. You upload that file to the cloud and you get an
identical copy. No need to re-run the import scripts on the server.

### Step 1.1 — Make the dump (on your laptop)

A fresh dump already exists: `backups/blueorbit-20261116.dump` (52 MB). Skip to 1.2 unless the
local graph changed since.

To make a new one (Neo4j must be **stopped** while dumping — Community edition rule):

```bash
cd backend
docker stop blueorbit-neo4j
docker run --rm \
  -v blueorbit_neo4j_data:/data \
  -v "$(pwd)/backups:/backups" \
  neo4j:5.26-community \
  neo4j-admin database dump neo4j --to-path=/backups --overwrite-destination=true
docker start blueorbit-neo4j
```

On Windows Git Bash put `MSYS_NO_PATHCONV=1 ` in front of `docker run`, otherwise the
`/data` paths get mangled. Result: `backups/neo4j.dump`. Keep a copy in Google Drive — it is
the only thing you need to rebuild the cloud database from scratch.

### Step 1.2 — Create the AuraDB instance

1. Go to <https://console.neo4j.io> and sign up (Google login is fine).
2. Click **New instance** → choose **AuraDB Free** → **Create**.
3. A box shows the **password once**. Click **Download** (saves a `.txt` with URI, user,
   password). Store it in your password manager. You cannot see it again.
4. Wait until the status turns **Running** (1–3 minutes).
5. Note the **Connection URI**, it looks like `neo4j+s://a1b2c3d4.databases.neo4j.io`.

### Step 1.3 — Upload the dump

1. In the console, open your instance → **Backup & restore** tab (or the `…` menu →
   *Backup & restore*).
2. Click **Upload backup** / **Restore from file**, drag in `blueorbit-20261116.dump`.
3. Confirm the overwrite. Wait until the instance is **Running** again (a few minutes).

Fallback if the console upload fails — upload from the command line:

```bash
docker run --rm -v "$(pwd)/backups:/backups" neo4j:5.26-community \
  neo4j-admin database upload neo4j \
  --from-path=/backups \
  --to-uri=neo4j+s://<your-id>.databases.neo4j.io \
  --to-user=neo4j --to-password='<aura-password>' \
  --overwrite-destination=true
```

(the folder must contain a file named `neo4j.dump`).

### Step 1.4 — Check it worked

Click **Query** (or **Open** → Query) in the console and run:

```cypher
MATCH (c:Country) RETURN count(c);          // expect about 247
MATCH ()-[r]->() RETURN count(r);           // expect about 296,000
MATCH (m:Meta) RETURN m.built_at;           // when the graph was built
```

### Free tier limits to know

| Limit | Ours | Note |
|---|---|---|
| 200,000 nodes | ~4,300 | fine |
| 400,000 relationships | ~296,000 | fine, but adding a big new dataset could cross it |
| Pauses after ~3 days with no queries | — | console → **Resume**. Your data stays. |

---

## Part 2 — Backend (Render)

Render runs the Python API for us: it pulls the code from GitHub, installs requirements and
starts the server.

### Step 2.0 — Prerequisite: code on GitHub

Render deploys from a GitHub repo. The backend branch `feature/setup-openai-rebuild` must be
pushed to `ontology-blueorbit` first (pending repo access). Deploy either that branch or `main`
after it is merged.

### Step 2.1 — Create the web service

1. Go to <https://render.com>, sign up **with GitHub** and allow access to the repo.
2. **New +** → **Web Service** → pick the backend repo.
3. Fill in:

   | Field | Value |
   |---|---|
   | Name | `blueorbit-api` (becomes part of the URL) |
   | Branch | the branch you want to deploy |
   | Runtime | Python 3 |
   | Build command | `pip install -r requirements.txt` |
   | Start command | `uvicorn api.main:app --host 0.0.0.0 --port $PORT` |
   | Instance type | Free |

   Why `--host 0.0.0.0 --port $PORT`: Render tells the app which port to use through `$PORT`
   and only forwards traffic to servers listening on all interfaces.

### Step 2.2 — Environment variables

Same page, **Environment** section → **Add environment variable** for each:

| Key | Value |
|---|---|
| `PYTHON_VERSION` | `3.12.7` (the code is tested on 3.12; Render's default is newer) |
| `NEO4J_URI` | the `neo4j+s://…` URI from step 1.2 |
| `NEO4J_USER` | `neo4j` |
| `NEO4J_PASSWORD` | the Aura password |
| `OPENAI_API_KEY` | your OpenAI key |
| `OPENAI_MODEL` | `gpt-4o-mini` |
| `LLM_PROVIDER_ORDER` | `openai,gemini,openrouter` |
| `CORS_ORIGINS` | `*` for now; tighten in step 3.4 |

Optional: `GEMINI_API_KEY` as a backup LLM.

Click **Create Web Service**. The first build takes 3–6 minutes; watch the **Logs** tab.

### Step 2.3 — Check it worked

Your URL looks like `https://blueorbit-api.onrender.com`. Open in a browser:

- `https://blueorbit-api.onrender.com/health` → JSON with the Neo4j status and `built_at`
- `https://blueorbit-api.onrender.com/docs` → the interactive API page

If `/health` shows a database error, re-check the three `NEO4J_*` values (most common:
`bolt://` instead of `neo4j+s://`, or a copy-paste space in the password).

### Free tier behaviour

The free instance **sleeps after 15 minutes idle**; the next request takes ~50 seconds to
wake it. Before a demo, open `/health` once a minute or two beforehand. (Paid "Starter" plan
removes this.)

Do **not** run `scripts/rebuild_graph.py` on Render. Rebuilds happen on your laptop against
local Neo4j, then you dump and upload again (Part 1).

---

## Part 3 — Frontend (Vercel)

### Step 3.1 — Import the project

1. Go to <https://vercel.com>, sign up **with GitHub**.
2. **Add New…** → **Project** → import `ontology_frontend`.
3. Framework preset: **Next.js** (auto-detected). Leave build/output settings as default.
4. Branch: Vercel builds the repo's default branch for production. To deploy
   `feature/openai-usable-ui` before it is merged, either merge it first or change
   *Settings → Git → Production Branch*.

### Step 3.2 — Environment variables (before the first deploy)

Expand **Environment Variables** on the import screen and add:

| Key | Value |
|---|---|
| `BACKEND_URL` | `https://blueorbit-api.onrender.com` (no trailing slash) |
| `OPENAI_API_KEY` | your OpenAI key (used by the query page / chat) |
| `OPENAI_MODEL` | `gpt-4o-mini` |

Why before: `next.config` turns `BACKEND_URL` into the `/api/backend/*` proxy **at build
time**. If you add or change it later you must **redeploy** (Deployments → `…` → Redeploy).

Click **Deploy**. Takes 1–3 minutes. You get `https://<project>.vercel.app`.

### Step 3.3 — Check it worked

Open the Vercel URL:

- the globe loads with country data and insight cards (data comes from the backend)
- the **Data status** panel shows the build date
- run a simulation, e.g. *"US and EU sanction Russia"*
- ask a question on the query page

If the page loads but has no data: the backend is asleep (wait 50 s and refresh) or
`BACKEND_URL` is wrong (fix it, then redeploy).

### Step 3.4 — Lock down CORS (optional, recommended)

Back in Render → `blueorbit-api` → **Environment**, set:

```
CORS_ORIGINS=https://<project>.vercel.app,http://localhost:3000
```

Save (Render restarts automatically). The frontend calls the API from Vercel's servers, so
this mostly guards against other websites calling your API from a browser.

---

## Updating later

| Changed | Do this |
|---|---|
| Frontend code | push to the deployed branch → Vercel redeploys automatically |
| Backend code | push to the deployed branch → Render redeploys automatically |
| Data / graph | rebuild locally → Part 1 step 1.1 (dump) → step 1.3 (upload). Nothing else changes. |
| A secret | edit it in the dashboard → Render restarts itself; Vercel needs a Redeploy |

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Render build fails on a package | `PYTHON_VERSION` not set to 3.12.x |
| `/health` → `ServiceUnavailable` / auth error | wrong `NEO4J_*` values, or Aura paused (Resume it) |
| Frontend loads, everything empty | backend asleep, or wrong `BACKEND_URL` (redeploy after fixing) |
| Simulation works, briefs are plain text | OpenAI key missing/out of credit; the numbers still work, only the wording falls back |
| Aura upload refused | dump over free-tier limits, or the file is not a Neo4j 5 dump |
