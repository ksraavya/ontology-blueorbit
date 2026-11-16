"""
Bilateral transfers of major conventional arms from the SIPRI Arms
Transfers Database trade register (data/raw/sipri_trade_register.csv).

    python -m modules.defense.sipri_transfers

The register has one row per deal per delivery year with supplier,
recipient, weapon designation/description, armament category and the
SIPRI trend-indicator value (TIV, millions) delivered that year.

Writes (supplier)-[:TRANSFERS_ARMS_TO {year, tiv, deals, categories,
weapons}]->(recipient), aggregated per pair per year. `weapons` keeps the
top designations by TIV so the UI can show *what* was delivered.

This complements the customs-based TRADES_ARMS_WITH (BACI HS 93): customs
data misses most government-to-government deliveries of major platforms
(e.g. Russia -> India), which is exactly what SIPRI records.

Suppliers that no longer exist (Soviet Union, ...) are kept as
:HistoricalState; non-state actors and organisations ("African Union**",
"unknown supplier(s)", rebel groups) are skipped.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from common.db import Neo4jConnection  # noqa: E402
from common.entity_mapper import canonical_country, historical_state  # noqa: E402

logger = logging.getLogger(__name__)
PATH = ROOT / "data" / "raw" / "sipri_trade_register.csv"


def _resolve(name: str) -> tuple[str | None, str | None]:
    """-> (name, label) with label 'Country' or 'HistoricalState', or (None, None) to skip."""
    raw = str(name).strip().rstrip("*").strip()
    canon = canonical_country(raw)
    if canon:
        return canon, "Country"
    hist = historical_state(raw)
    if hist:
        return hist, "HistoricalState"
    return None, None


def build_edges() -> pd.DataFrame:
    df = pd.read_csv(PATH, skiprows=11, low_memory=False)
    df["tiv"] = pd.to_numeric(df["TIV delivery values"], errors="coerce").fillna(0.0)
    df["year"] = pd.to_numeric(df["Delivery year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    names = {n: _resolve(n) for n in set(df["Supplier"]) | set(df["Recipient"])}
    skipped = sorted(n for n, (c, _) in names.items() if c is None)
    logger.info("SIPRI: %d actor names, %d skipped as non-state/organisation: %s", len(names), len(skipped), skipped[:12])

    df["src"] = df["Supplier"].map(lambda n: names[n][0])
    df["src_label"] = df["Supplier"].map(lambda n: names[n][1])
    df["dst"] = df["Recipient"].map(lambda n: names[n][0])
    df["dst_label"] = df["Recipient"].map(lambda n: names[n][1])
    df = df.dropna(subset=["src", "dst"])
    df = df[df["src"] != df["dst"]]
    df["weapon"] = df["Designation"].astype(str).str.strip() + " (" + df["Description"].astype(str).str.strip() + ")"

    rows = []
    for (src, sl, dst, dl, year), g in df.groupby(["src", "src_label", "dst", "dst_label", "year"]):
        top = (
            g.groupby("weapon")["tiv"].sum().sort_values(ascending=False).head(5).index.tolist()
        )
        rows.append({
            "src": src, "src_label": sl, "dst": dst, "dst_label": dl, "year": int(year),
            "tiv": round(float(g["tiv"].sum()), 2),
            "deals": int(g["SIPRI AT Database ID"].nunique()),
            "categories": sorted(set(g["Armament category"].dropna().astype(str))),
            "weapons": top,
        })
    return pd.DataFrame(rows)


def load(edges: pd.DataFrame) -> int:
    conn = Neo4jConnection()
    rows = edges.to_dict("records")
    # Historical suppliers may not exist yet as nodes: create them under their own label.
    hist = sorted({r["src"] for r in rows if r["src_label"] == "HistoricalState"} |
                  {r["dst"] for r in rows if r["dst_label"] == "HistoricalState"})
    if hist:
        conn.run_query("UNWIND $names AS n MERGE (:HistoricalState {name: n})", {"names": hist})
    for i in range(0, len(rows), 5000):
        conn.run_query(
            """
            UNWIND $rows AS r
            MATCH (a {name: r.src}) WHERE (a:Country OR a:HistoricalState)
            MATCH (b {name: r.dst}) WHERE (b:Country OR b:HistoricalState)
            MERGE (a)-[e:TRANSFERS_ARMS_TO {year: r.year}]->(b)
            SET e.tiv = r.tiv, e.value = r.tiv, e.deals = r.deals,
                e.categories = r.categories, e.weapons = r.weapons,
                e.source = 'SIPRI Arms Transfers Database (TIV, millions)'
            """,
            {"rows": rows[i:i + 5000]},
        )
    return len(rows)


def run() -> dict[str, int]:
    edges = build_edges()
    n = load(edges)
    logger.info("TRANSFERS_ARMS_TO: %d pair-year edges, %d suppliers, %d recipients, years %d-%d",
                n, edges["src"].nunique(), edges["dst"].nunique(), edges["year"].min(), edges["year"].max())
    return {"edges": n, "suppliers": int(edges["src"].nunique()), "recipients": int(edges["dst"].nunique())}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    print(run())
