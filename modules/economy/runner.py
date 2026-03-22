from __future__ import annotations

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

print("runner.py starting...", flush=True)

from pathlib import Path

import pandas as pd

from common.ontology import EXPORTS_TO, IMPORTS_ENERGY_FROM

from modules.economy.ingest import (
    _baci_years_default,
    _load_baci_year,
    _load_country_codes,
    load_macro_data,
    load_org_data,
)
from modules.economy.transform import transform_macro_data, transform_trade_data
from modules.economy.compute import (
    compute_energy_metrics,
    compute_macro_metrics,
    compute_trade_metrics,
    compute_trade_volume,
    compute_trade_balance,
)
from modules.economy.load import load_to_graph

logger = logging.getLogger(__name__)


def run(years: list[int] | None = None) -> None:

    logger.info("Economy pipeline starting")

    years_list = _baci_years_default(years)
    logger.info("Years to process: %s", years_list)

    # ── Step 1: Macro (small, load once) ──────────────────────────────────
    logger.info("--- Stage: Macro ---")
    macro_raw = load_macro_data()
    macro_clean = transform_macro_data(macro_raw)
    macro_enriched = compute_macro_metrics(macro_clean)
    load_to_graph(macro_enriched)
    del macro_raw, macro_clean, macro_enriched
    logger.info("Macro stage complete")

    # ── Step 2: Trade + Energy, one year at a time ────────────────────────
    cc = _load_country_codes()

    all_trade_enriched: list[dict] = []
    all_energy_enriched: list[dict] = []

    for year in years_list:
        logger.info("--- Stage: BACI year %d ---", year)

        df = _load_baci_year(year)
        if df.empty:
            logger.warning("Skipping year %d — file not found or empty", year)
            continue

        # Join country names
        exp = (
            cc[["country_code", "country_name"]]
            .rename(columns={"country_name": "exporter_name"})
        )
        df = df.merge(
            exp, left_on="i", right_on="country_code", how="left"
        ).drop(columns=["country_code"])

        imp = (
            cc[["country_code", "country_name"]]
            .rename(columns={"country_name": "importer_name"})
        )
        df = df.merge(
            imp, left_on="j", right_on="country_code", how="left"
        ).drop(columns=["country_code"])

        df = df.dropna(subset=["exporter_name", "importer_name"])

        k_str = df["k"].astype(str)

        # Build trade rows
        trade_df = df.loc[~k_str.str.startswith("27")].copy()
        trade_df["value"] = trade_df["v"] * 1000
        trade_raw = pd.DataFrame({
            "source": trade_df["exporter_name"],
            "target": trade_df["importer_name"],
            "value":  trade_df["value"],
            "year":   trade_df["t"],
            "type":   "trade",
        }).to_dict("records")
        del trade_df

        # Build energy rows
        energy_df = df.loc[k_str.str.startswith("27")].copy()
        energy_df["value"] = energy_df["v"] * 1000
        energy_raw = pd.DataFrame({
            "source": energy_df["exporter_name"],
            "target": energy_df["importer_name"],
            "value":  energy_df["value"],
            "year":   energy_df["t"],
            "type":   "energy",
        }).to_dict("records")
        del energy_df, df, k_str

        # Transform
        trade_clean  = transform_trade_data(trade_raw)
        energy_clean = transform_trade_data(energy_raw)
        del trade_raw, energy_raw

        # Compute
        trade_enriched  = compute_trade_metrics(trade_clean)
        energy_enriched = compute_energy_metrics(energy_clean)
        del trade_clean, energy_clean

        # Load immediately — don't accumulate in memory
        load_to_graph(trade_enriched)
        load_to_graph(energy_enriched)

        # Keep post-threshold enriched data for volume computation
        # These are much smaller than raw rows (threshold filtered)
        all_trade_enriched.extend(trade_enriched)
        all_energy_enriched.extend(energy_enriched)
        del trade_enriched, energy_enriched

        logger.info("Year %d complete", year)

    # ── Step 3: Trade Volume (derived, needs all years) ───────────────────
    logger.info("--- Stage: Trade Volume ---")
    volume_enriched = compute_trade_volume(
        [r for r in all_trade_enriched if r["rel"] == EXPORTS_TO],
        [r for r in all_energy_enriched if r["rel"] == IMPORTS_ENERGY_FROM],
    )
    load_to_graph(volume_enriched)
    del volume_enriched
    logger.info("Trade volume stage complete")

    logger.info("--- Stage: Trade Balance ---")
    balance_enriched = compute_trade_balance(
        [r for r in all_trade_enriched if r["rel"] == EXPORTS_TO],
        [r for r in all_energy_enriched if r["rel"] == IMPORTS_ENERGY_FROM],
    )
    load_to_graph(balance_enriched)
    del balance_enriched, all_trade_enriched, all_energy_enriched
    logger.info("Trade balance stage complete")

    # ── Step 4: Orgs (captured, not loaded yet) ───────────────────────────
    orgs = load_org_data()
    logger.info(
        "Org data captured: %d records (not loaded — handled by geopolitics module)",
        len(orgs),
    )

    logger.info("Economy pipeline complete")


if __name__ == "__main__":
    run()