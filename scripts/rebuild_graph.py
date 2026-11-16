"""
Rebuild the whole Neo4j graph from data/raw, in dependency order.

    python scripts/rebuild_graph.py                 # everything that has data
    python scripts/rebuild_graph.py --list          # show steps
    python scripts/rebuild_graph.py --only economy,analytics_economy
    python scripts/rebuild_graph.py --from climate  # resume after a failure
    python scripts/rebuild_graph.py --with-live     # also pull live news at the end
    python scripts/rebuild_graph.py --year 2023     # force the analytics year

Every loader uses MERGE, so re-running a step is safe (idempotent): it
updates the same nodes/edges instead of duplicating them.

Order matters because later steps read what earlier ones wrote:

    constraints
      → defense → defense_enrichment (regions, alliances, country props)
      → economy → sanctions_agreements
      → geopolitics
      → climate (its bridge stage needs economy edges)
      → analytics_economy / defense / geopolitics / climate
      → composite (needs every domain score)
      → live (optional; needs countries + conflict data)
"""
from __future__ import annotations

import argparse
import importlib.util
import logging
import os
import runpy
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)                      # loaders use paths relative to the repo root
sys.path.insert(0, str(ROOT))

from scripts.check_data import baci_years, check  # noqa: E402

log = logging.getLogger("rebuild")


def _load_script(rel: str):
    spec = importlib.util.spec_from_file_location(Path(rel).stem, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)                 # type: ignore[union-attr]
    return mod


# =========================================================
# STEPS
# =========================================================

def step_constraints(ctx: "Ctx") -> None:
    """Indexes on MERGE keys. Without them every MERGE scans the whole label."""
    from common.db import Neo4jConnection
    conn = Neo4jConnection()
    stmts = [
        "CREATE CONSTRAINT country_name IF NOT EXISTS FOR (n:Country) REQUIRE n.name IS UNIQUE",
        "CREATE CONSTRAINT year_year IF NOT EXISTS FOR (n:Year) REQUIRE n.year IS UNIQUE",
        "CREATE CONSTRAINT region_name IF NOT EXISTS FOR (n:Region) REQUIRE n.name IS UNIQUE",
        "CREATE CONSTRAINT alliance_name IF NOT EXISTS FOR (n:Alliance) REQUIRE n.name IS UNIQUE",
    ]
    for label in ("Organization", "PoliticalSystem", "Disaster", "Resource", "Commodity", "NewsArticle"):
        stmts.append(f"CREATE INDEX {label.lower()}_name IF NOT EXISTS FOR (n:{label}) ON (n.name)")
    stmts.append("CREATE INDEX conflictsignal_date IF NOT EXISTS FOR (n:ConflictSignal) ON (n.date)")
    for s in stmts:
        try:
            conn.run_query(s)
        except RuntimeError as exc:
            # Usually: duplicates already exist in an old graph. Report, don't abort.
            log.warning("constraint skipped: %s", exc)


def step_defense(ctx: "Ctx") -> None:
    from modules.defense.pipeline import run_defense_pipeline
    run_defense_pipeline()


def step_defense_enrichment(ctx: "Ctx") -> None:
    from modules.defense.graph_enrichment import insert_alliances, insert_regions, verify_enrichment
    from modules.defense.graph_enrichment_extended import enrich_country_properties
    insert_regions()
    insert_alliances()
    enrich_country_properties()
    verify_enrichment()


def step_economy(ctx: "Ctx") -> None:
    from modules.economy.runner import run
    run(years=ctx.baci_years)


def step_sanctions_agreements(ctx: "Ctx") -> None:
    # The script does its work at import time; run it as __main__.
    runpy.run_path(str(ROOT / "scripts" / "load_sanctions_and_agreements.py"), run_name="__main__")


def step_geopolitics(ctx: "Ctx") -> None:
    mod = _load_script("scripts/run_all.py")
    mod.run_geopolitics(use_cache=not ctx.no_cache, use_unga_cache=not ctx.no_cache)


def step_climate(ctx: "Ctx") -> None:
    from modules.climate.runner import run
    run(year=ctx.year)


def step_cleanup(ctx: "Ctx") -> None:
    """
    Make :Country mean "a current country", before any analytics runs.

    Loaders write whatever their source contains; some names are not
    countries: World Bank aggregates, SIPRI's "Unknown supplier(s)" and
    rebel groups, dissolved states from 1950-2000 arms data, V-Dem
    sub-national units. Left as :Country they distort every normalised
    score (max-normalising GDP against "World") and inflate the country
    count. Nothing is deleted: such nodes are relabelled
    :HistoricalState or :NonCountry and keep their edges for traceability.
    """
    from common.db import Neo4jConnection
    from common.entity_mapper import canonical_country, historical_state

    conn = Neo4jConnection()
    names = [r["name"] for r in conn.run_query("MATCH (c:Country) RETURN c.name AS name")]
    historical, non_country, duplicates = [], [], []
    for n in names:
        canon = canonical_country(n)
        if canon == n:
            continue
        if canon:
            duplicates.append((n, canon))
        elif historical_state(n):
            historical.append(n)
        else:
            non_country.append(n)

    if historical:
        conn.run_query(
            "UNWIND $names AS n MATCH (c:Country {name: n}) REMOVE c:Country SET c:HistoricalState",
            {"names": historical},
        )
    if non_country:
        conn.run_query(
            "UNWIND $names AS n MATCH (c:Country {name: n}) REMOVE c:Country SET c:NonCountry",
            {"names": non_country},
        )
    # ISO codes on every real country: a stable join key for the frontend map
    # (Natural Earth features carry ISO numeric ids) and for future loaders.
    import pycountry
    iso_rows = []
    for n in names:
        if canonical_country(n) == n:
            c = pycountry.countries.get(name=n)
            if c:
                iso_rows.append({"name": n, "iso2": c.alpha_2, "iso3": c.alpha_3, "numeric": c.numeric})
    conn.run_query(
        "UNWIND $rows AS r MATCH (c:Country {name: r.name}) "
        "SET c.iso2 = r.iso2, c.iso3 = r.iso3, c.iso_numeric = r.numeric",
        {"rows": iso_rows},
    )

    real = conn.run_query("MATCH (c:Country) RETURN count(c) AS n")[0]["n"]
    log.info(
        "cleanup: %d countries kept, %d -> :HistoricalState, %d -> :NonCountry",
        real, len(historical), len(non_country),
    )
    if duplicates:
        # Should be empty: every loader normalises through canonical_country.
        log.warning("cleanup: non-canonical country spellings still present: %s", duplicates[:20])


def step_indicators(ctx: "Ctx") -> None:
    """IMF WEO + World Bank WDI/WGI country indicators (cached under data/raw/indicators/)."""
    from modules.indicators.runner import run
    for code, state in run().items():
        log.info("  %-24s %s", code, state)


def step_arms_trade(ctx: "Ctx") -> None:
    """Bilateral arms trade from BACI customs data (HS 93, tanks, warships)."""
    from modules.defense.arms_trade_baci import run
    log.info("arms trade: %s", run())


def step_sipri_transfers(ctx: "Ctx") -> None:
    """Bilateral major-arms transfers from the SIPRI trade register."""
    from modules.defense.sipri_transfers import run
    log.info("SIPRI transfers: %s", run())


def step_sipri_extra(ctx: "Ctx") -> None:
    """SIPRI Top 100 companies, financial arms exports, Milex 1949-2025."""
    from modules.defense.sipri_extra import run
    log.info("SIPRI extra: %s", run())


def step_analytics_economy(ctx: "Ctx") -> None:
    from analytics.economy.runner import run
    run(year=ctx.year)


def step_analytics_defense(ctx: "Ctx") -> None:
    from analytics.defense.runner import run_defense_analytics
    run_defense_analytics()


def step_analytics_geopolitics(ctx: "Ctx") -> None:
    from analytics.geopolitics.runner import run
    run(years=list(range(2010, ctx.year + 1)))


def step_analytics_climate(ctx: "Ctx") -> None:
    from analytics.climate.runner import run
    run()


def step_composite(ctx: "Ctx") -> None:
    from analytics.composite.runner import run
    result = run(year=ctx.year)
    log.info("composite: %s", result)


def step_live(ctx: "Ctx") -> None:
    from modules.defense.live.run_all_live import run_all_live_updates
    run_all_live_updates()


@dataclass
class Step:
    name: str
    fn: Callable[["Ctx"], None]
    needs: list[str]          # dataset keys from check_data.DATASETS
    optional: bool = False    # only runs when asked (live)


STEPS: list[Step] = [
    Step("constraints", step_constraints, []),
    Step("defense", step_defense, ["sipri_milex", "sipri_arms_1950_1980", "acled"]),
    Step("defense_enrichment", step_defense_enrichment, []),
    Step("economy", step_economy, ["baci", "country_codes", "wb_gdp", "wb_inflation"]),
    Step("sanctions_agreements", step_sanctions_agreements, ["ofac_sdn", "wto_rta"]),
    Step("geopolitics", step_geopolitics, ["vdem", "gdelt", "unga"]),
    Step("climate", step_climate, ["emdat"]),
    Step("cleanup", step_cleanup, []),
    Step("indicators", step_indicators, []),
    Step("arms_trade", step_arms_trade, ["baci", "country_codes"]),
    Step("sipri_transfers", step_sipri_transfers, ["sipri_trade_register"]),
    Step("sipri_extra", step_sipri_extra, []),
    Step("analytics_economy", step_analytics_economy, ["baci"]),
    Step("analytics_defense", step_analytics_defense, ["sipri_milex"]),
    Step("analytics_geopolitics", step_analytics_geopolitics, ["vdem", "gdelt", "unga"]),
    Step("analytics_climate", step_analytics_climate, ["emdat"]),
    Step("composite", step_composite, []),
    Step("live", step_live, [], optional=True),
]


@dataclass
class Ctx:
    year: int
    baci_years: list[int] | None
    no_cache: bool


# =========================================================
# MAIN
# =========================================================

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--only", help="comma-separated step names")
    ap.add_argument("--from", dest="start", help="start at this step")
    ap.add_argument("--year", type=int, help="analytics year (default: latest BACI year present)")
    ap.add_argument("--with-live", action="store_true", help="run live news step at the end")
    ap.add_argument("--no-cache", action="store_true", help="ignore data/processed parquet caches")
    ap.add_argument("--force", action="store_true", help="run steps even if their data is missing")
    args = ap.parse_args()

    if args.list:
        for s in STEPS:
            print(f"{s.name:<24} needs: {', '.join(s.needs) or '-'}{'  (opt-in: --with-live)' if s.optional else ''}")
        return 0

    log_path = ROOT / "rebuild.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_path, encoding="utf-8")],
    )

    years = baci_years()
    year = args.year or (max(years) if years else 2024)
    ctx = Ctx(year=year, baci_years=years or None, no_cache=args.no_cache)
    log.info("analytics year = %s (BACI years present: %s)", year, years or "none")

    status, _ = check()
    names = [s.name for s in STEPS]
    selected = STEPS
    if args.only:
        wanted = {n.strip() for n in args.only.split(",")}
        unknown = wanted - set(names)
        if unknown:
            print(f"Unknown step(s): {sorted(unknown)}. Use --list.")
            return 2
        selected = [s for s in STEPS if s.name in wanted]
    elif args.start:
        if args.start not in names:
            print(f"Unknown step {args.start!r}. Use --list.")
            return 2
        selected = STEPS[names.index(args.start):]
    if not args.with_live and not args.only:
        selected = [s for s in selected if not s.optional]

    from common.db import Neo4jConnection
    try:
        Neo4jConnection()
    except Exception as exc:
        print(f"Cannot reach Neo4j: {exc}\nIs it running, and is .env set (NEO4J_URI/USER/PASSWORD)?")
        return 2

    results: list[tuple[str, str, float]] = []
    for s in selected:
        missing = [k for k in s.needs if not status.get(k, False)]
        if missing and not args.force:
            log.warning("SKIP %s: missing data %s", s.name, missing)
            results.append((s.name, f"skipped (missing {', '.join(missing)})", 0.0))
            continue
        log.info("=" * 60)
        log.info("STEP %s", s.name)
        t0 = time.perf_counter()
        try:
            s.fn(ctx)
            results.append((s.name, "ok", time.perf_counter() - t0))
        except Exception as exc:
            log.error("STEP %s FAILED: %s\n%s", s.name, exc, traceback.format_exc())
            results.append((s.name, f"FAILED: {exc}", time.perf_counter() - t0))

    # Stamp the build time so the UI can show "last data update" honestly.
    if any(state == "ok" for _, state, _ in results):
        try:
            Neo4jConnection().run_query("MERGE (m:Meta {key: 'graph'}) SET m.built_at = toString(datetime())")
        except Exception as exc:
            log.warning("could not stamp build time: %s", exc)

    print("\n" + "=" * 60)
    print("REBUILD SUMMARY")
    for name, state, secs in results:
        print(f"  {name:<24} {state:<50} {secs:7.1f}s")
    try:
        counts = Neo4jConnection().run_query(
            "MATCH (n) WITH count(n) AS nodes "
            "MATCH ()-[r]->() RETURN nodes, count(r) AS rels"
        )[0]
        print(f"\nGraph now: {counts['nodes']:,} nodes, {counts['rels']:,} relationships")
    except Exception:
        pass
    print(f"Full log: {log_path}")
    print("Next: back up the graph (see SETUP.md → Backups) and start the API.")
    return 1 if any(state.startswith("FAILED") for _, state, _ in results) else 0


if __name__ == "__main__":
    sys.exit(main())
