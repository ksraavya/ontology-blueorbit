"""
Bilateral arms trade from CEPII BACI customs data (already on disk).

    python -m modules.defense.arms_trade_baci

Products counted (HS 2017):
    93xxxx  arms and ammunition (military weapons, small arms, munitions, parts)
    871000  tanks and other armoured fighting vehicles
    890610  warships

Writes (:Country)-[:TRADES_ARMS_WITH {year, value, source}]->(:Country),
exporter -> importer, value in USD.

Caveat (documented on the edge and in the UI): customs data under-records
government-to-government transfers of major platforms (fighter jets,
missile systems), especially from exporters that do not report them
(e.g. Russia, China). It is dense and real for what it covers; SIPRI's
trade register, if loaded, complements it for major platforms.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from common.db import Neo4jConnection  # noqa: E402
from common.entity_mapper import normalize_entity  # noqa: E402

logger = logging.getLogger(__name__)
RAW = ROOT / "data" / "raw"
ARMS_HS6 = {"871000", "890610"}
MIN_VALUE_USD = 100_000          # drop sub-$100k noise


def _country_names() -> dict[int, str]:
    cc = pd.read_csv(RAW / "country_codes.csv")
    return {int(r.country_code): normalize_entity(r.country_name) for r in cc.itertuples()}


def build_edges() -> pd.DataFrame:
    names = _country_names()
    frames = []
    for path in sorted(RAW.glob("baci_trade_*.csv")):
        year = int(path.stem.split("_")[-1])
        df = pd.read_csv(path, usecols=["i", "j", "k", "v"], dtype={"k": str})
        k = df["k"].str.zfill(6)
        arms = df[k.str.startswith("93") | k.isin(ARMS_HS6)]
        pairs = arms.groupby(["i", "j"], as_index=False)["v"].sum()
        pairs["value"] = pairs["v"] * 1000          # BACI values are in thousand USD
        pairs = pairs[pairs["value"] >= MIN_VALUE_USD]
        pairs["year"] = year
        pairs["src"] = pairs["i"].map(names)
        pairs["dst"] = pairs["j"].map(names)
        frames.append(pairs.dropna(subset=["src", "dst"]))
        logger.info("BACI arms %d: %d country pairs, $%.1fB", year, len(pairs), pairs["value"].sum() / 1e9)
    out = pd.concat(frames, ignore_index=True)
    return out[out["src"] != out["dst"]][["src", "dst", "year", "value"]]


def load(edges: pd.DataFrame) -> int:
    conn = Neo4jConnection()
    rows = edges.to_dict("records")
    for i in range(0, len(rows), 5000):
        conn.run_query(
            """
            UNWIND $rows AS r
            MATCH (a:Country {name: r.src}) MATCH (b:Country {name: r.dst})
            MERGE (a)-[e:TRADES_ARMS_WITH {year: r.year}]->(b)
            SET e.value = r.value, e.source = 'CEPII BACI (HS 93, 871000, 890610)',
                e.note = 'customs-reported; major G2G platforms under-recorded'
            """,
            {"rows": rows[i:i + 5000]},
        )
    return len(rows)


def run() -> dict[str, int]:
    edges = build_edges()
    n = load(edges)
    logger.info("TRADES_ARMS_WITH: %d edges written", n)
    return {"edges": n, "pairs_latest_year": int((edges["year"] == edges["year"].max()).sum())}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    print(run())
