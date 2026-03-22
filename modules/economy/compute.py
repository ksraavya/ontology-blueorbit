from __future__ import annotations

import logging
from collections import defaultdict

from common.ontology import (
    EXPORTS_TO,
    IMPORTS_FROM,
    EXPORTS_ENERGY_TO,
    IMPORTS_ENERGY_FROM,
    HAS_GDP,
    HAS_INFLATION,
    HAS_TRADE_BALANCE,
    HAS_TRADE_VOLUME_WITH,
    get_relation_type,
)
from common.intelligence.aggregation import max_value, sum_values
from common.intelligence.dependency import (
    compute_dependency,
)
from common.intelligence.normalization import normalize_by_max
from modules.economy.constants import (
    COUNTRY_LABEL,
    ENERGY_MIN_VALUE_USD,
    GDP_METRIC_NAME,
    GDP_TYPE,
    INFLATION_METRIC_NAME,
    INFLATION_TYPE,
    METRIC_LABEL,
    MIN_DEPENDENCY,
    TRADE_BALANCE_METRIC_NAME,
    TRADE_BALANCE_TYPE,
    TRADE_MIN_VALUE_USD,
)

logger = logging.getLogger(__name__)


def _compute_flow_metrics(
    rows: list[dict],
    rel: str,
    min_value: float,
    label: str,
) -> list[dict]:
    """
    Shared flow metrics for trade (EXPORTS_TO) and energy (IMPORTS_ENERGY_FROM).

    For IMPORTS_ENERGY_FROM, output endpoints are reversed vs raw rows:
    raw (exporter, importer) → edge source=importer, target=exporter.
    Dependency totals are keyed by importer (raw ``target``).
    """
    n_in = len(rows)
    if get_relation_type(rel) != "flow":
        raise ValueError(
            f"Expected relation {rel} to have ontology type 'flow', "
            f"got {get_relation_type(rel)!r}"
        )

    filtered = [r for r in rows if r["value"] >= min_value]
    logger.info("%s rows after threshold filter: %d", label, len(filtered))

    # Aggregate to one row per (source, target, year)
    pair_year_values: dict[tuple, list[float]] = defaultdict(list)
    for r in filtered:
        key = (r["source"], r["target"], r["year"])
        pair_year_values[key].append(r["value"])

    aggregated = []
    for (src, tgt, yr), values in pair_year_values.items():
        aggregated.append(
            {
                "source": src,
                "target": tgt,
                "value": sum_values(values),
                "year": yr,
                "type": filtered[0]["type"] if filtered else "",
            }
        )
    filtered = aggregated
    logger.info("%s rows after aggregation: %d", label, len(filtered))

    if not filtered:
        log_name = (
            "compute_trade_metrics" if rel == EXPORTS_TO else "compute_energy_metrics"
        )
        logger.info("%s: input=%d filtered=%d output=%d", log_name, n_in, len(filtered), 0)
        return []

    rows_by_year: dict[int, list[dict]] = defaultdict(list)
    for r in filtered:
        rows_by_year[r["year"]].append(r)

    max_val_by_year: dict[int, float] = {}
    source_values_by_year: dict[int, dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )

    energy_mode = rel == IMPORTS_ENERGY_FROM
    agg_key = "target" if energy_mode else "source"

    for year, year_rows in rows_by_year.items():
        max_val_by_year[year] = max_value([r["value"] for r in year_rows])
        for r in year_rows:
            k = r[agg_key]
            source_values_by_year[year][k].append(r["value"])

    source_totals_by_year: dict[int, dict[str, float]] = {}
    for year, by_src in source_values_by_year.items():
        source_totals_by_year[year] = {
            src: sum_values(vals) for src, vals in by_src.items()
        }

    out: list[dict] = []
    for r in filtered:
        year = r["year"]
        total_src = source_totals_by_year[year][r[agg_key]]
        dep = compute_dependency(r["value"], total_src)
        if dep < MIN_DEPENDENCY:
            continue
        nw = normalize_by_max(r["value"], max_val_by_year[year])

        if energy_mode:
            out_source = r["target"]
            out_target = r["source"]
        else:
            out_source = r["source"]
            out_target = r["target"]

        out.append(
            {
                "source": out_source,
                "target": out_target,
                "rel": rel,
                "source_label": COUNTRY_LABEL,
                "target_label": COUNTRY_LABEL,
                "properties": {
                    "value": r["value"],
                    "normalized_weight": nw,
                    "dependency": dep,
                    "year": r["year"],
                },
            }
        )

        reverse_rel = IMPORTS_FROM if rel == EXPORTS_TO else EXPORTS_ENERGY_TO

        out.append(
            {
                "source": out_target,
                "target": out_source,
                "rel": reverse_rel,
                "source_label": COUNTRY_LABEL,
                "target_label": COUNTRY_LABEL,
                "properties": {
                    "value": r["value"],
                    "normalized_weight": nw,
                    "dependency": dep,
                    "year": r["year"],
                },
            }
        )

    log_name = "compute_trade_metrics" if rel == EXPORTS_TO else "compute_energy_metrics"
    logger.info(
        "%s: input=%d filtered=%d output=%d",
        log_name,
        n_in,
        len(filtered),
        len(out),
    )
    return out


def compute_trade_metrics(rows: list[dict]) -> list[dict]:
    return _compute_flow_metrics(rows, EXPORTS_TO, TRADE_MIN_VALUE_USD, "Trade")


def compute_energy_metrics(rows: list[dict]) -> list[dict]:
    return _compute_flow_metrics(rows, IMPORTS_ENERGY_FROM, ENERGY_MIN_VALUE_USD, "Energy")


def compute_trade_balance(
    trade_enriched: list[dict],
    energy_enriched: list[dict],
) -> list[dict]:
    if get_relation_type(HAS_TRADE_BALANCE) != "state":
        raise ValueError(
            f"Expected HAS_TRADE_BALANCE to be state, "
            f"got {get_relation_type(HAS_TRADE_BALANCE)!r}"
        )

    combined = trade_enriched + energy_enriched

    exports_lists: dict[tuple, list[float]] = defaultdict(list)
    imports_lists: dict[tuple, list[float]] = defaultdict(list)

    for r in combined:
        key = (r["source"], r["properties"]["year"])
        val = r["properties"]["value"]
        if r["rel"] in (EXPORTS_TO, EXPORTS_ENERGY_TO):
            exports_lists[key].append(val)
        elif r["rel"] in (IMPORTS_FROM, IMPORTS_ENERGY_FROM):
            imports_lists[key].append(val)
    exports_by = {k: sum_values(v) for k, v in exports_lists.items()}
    imports_by = {k: sum_values(v) for k, v in imports_lists.items()}

    all_keys = set(exports_by.keys()) | set(imports_by.keys())

    out = []
    for (country, year) in all_keys:
        exports = exports_by.get((country, year), 0.0)
        imports = imports_by.get((country, year), 0.0)
        balance = exports - imports
        out.append(
            {
                "source": country,
                "target": TRADE_BALANCE_METRIC_NAME,
                "rel": HAS_TRADE_BALANCE,
                "source_label": COUNTRY_LABEL,
                "target_label": METRIC_LABEL,
                "properties": {
                    "value": balance,
                    "exports": exports,
                    "imports": imports,
                    "normalized_weight": 0.0,
                    "year": year,
                    "currency": "USD",
                },
            }
        )

    logger.info("compute_trade_balance: output=%d", len(out))
    return out


def compute_macro_metrics(rows: list[dict]) -> list[dict]:
    n_in = len(rows)

    if get_relation_type(HAS_GDP) != "state":
        raise ValueError(
            f"Expected {HAS_GDP} to have ontology type 'state', "
            f"got {get_relation_type(HAS_GDP)!r}"
        )
    if get_relation_type(HAS_INFLATION) != "state":
        raise ValueError(
            f"Expected {HAS_INFLATION} to have ontology type 'state', "
            f"got {get_relation_type(HAS_INFLATION)!r}"
        )
    if get_relation_type(HAS_TRADE_BALANCE) != "state":
        raise ValueError(
            f"Expected {HAS_TRADE_BALANCE} to have ontology type 'state', "
            f"got {get_relation_type(HAS_TRADE_BALANCE)!r}"
        )

    gdp_rows = [r for r in rows if r["indicator"] == GDP_TYPE]
    max_gdp = max_value([r["value"] for r in gdp_rows])

    out: list[dict] = []
    for row in rows:
        ind = row["indicator"]
        if ind == GDP_TYPE:
            rel = HAS_GDP
            metric_name = GDP_METRIC_NAME
            nw = normalize_by_max(row["value"], max_gdp)
        elif ind == INFLATION_TYPE:
            rel = HAS_INFLATION
            metric_name = INFLATION_METRIC_NAME
            nw = 0.0
        elif ind == TRADE_BALANCE_TYPE:
            rel = HAS_TRADE_BALANCE
            metric_name = TRADE_BALANCE_METRIC_NAME
            nw = 0.0
        else:
            logger.warning(
                "Skipping macro row with unknown indicator: %r (country=%r)",
                ind,
                row.get("country"),
            )
            continue

        out.append(
            {
                "source": row["country"],
                "target": metric_name,
                "rel": rel,
                "source_label": COUNTRY_LABEL,
                "target_label": METRIC_LABEL,
                "properties": {
                    "value": row["value"],
                    "normalized_weight": nw,
                    "year": row["year"],
                    "currency": row["currency"],
                },
            }
        )

    logger.info("compute_macro_metrics: input=%d output=%d", n_in, len(out))
    return out


def compute_trade_volume(
    trade_rows: list[dict],
    energy_rows: list[dict],
) -> list[dict]:
    trade_primary = [r for r in trade_rows if r["rel"] == EXPORTS_TO]
    energy_primary = [r for r in energy_rows if r["rel"] == IMPORTS_ENERGY_FROM]

    combined = trade_primary + energy_primary
    vol_lists: dict[tuple[int, tuple[str, str]], list[float]] = defaultdict(list)

    for r in combined:
        y = r["properties"]["year"]
        pair = tuple(sorted([r["source"], r["target"]]))
        vol_lists[(y, pair)].append(r["properties"]["value"])

    year_pair_totals: dict[int, dict[tuple[str, str], float]] = defaultdict(dict)
    for (y, pair), vals in vol_lists.items():
        year_pair_totals[y][pair] = sum_values(vals)

    out: list[dict] = []
    for y, pair_dict in sorted(year_pair_totals.items()):
        max_vol = max_value(list(pair_dict.values()))
        for pair, total_volume in pair_dict.items():
            nw = normalize_by_max(total_volume, max_vol)
            out.append(
                {
                    "source": pair[0],
                    "target": pair[1],
                    "rel": HAS_TRADE_VOLUME_WITH,
                    "source_label": COUNTRY_LABEL,
                    "target_label": COUNTRY_LABEL,
                    "properties": {
                        "value": total_volume,
                        "normalized_weight": nw,
                        "year": y,
                    },
                }
            )

    n_pairs = len(out)
    logger.info("compute_trade_volume: pairs=%d output=%d", n_pairs, len(out))
    return out


def compute_all(data: dict[str, list[dict]]) -> dict[str, list[dict]]:
    trade_metrics = compute_trade_metrics(data["trade"])
    energy_metrics = compute_energy_metrics(data["energy"])
    macro_metrics = compute_macro_metrics(data["macro"])
    volume_metrics = compute_trade_volume(trade_metrics, energy_metrics)
    balance_metrics = compute_trade_balance(trade_metrics, energy_metrics)

    logger.info(
        "compute_all: trade=%d energy=%d macro=%d "
        "volume=%d balance=%d",
        len(trade_metrics),
        len(energy_metrics),
        len(macro_metrics),
        len(volume_metrics),
        len(balance_metrics),
    )
    return {
        "trade": trade_metrics,
        "energy": energy_metrics,
        "macro": macro_metrics,
        "volume": volume_metrics,
        "balance": balance_metrics,
        "orgs": data.get("orgs", []),
    }
