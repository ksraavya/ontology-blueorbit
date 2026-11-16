"""
Additional SIPRI datasets (data/raw/sipri/):

  top100_companies.xlsx          SIPRI Top 100 arms-producing companies, 2002-2024
  top100_total_revenues.xlsx     total Top-100 arms revenues per year (world)
  financial_values_arms_exports.xlsx  government/industry financial value of arms exports
  milex_1949_2025.xlsx           SIPRI Military Expenditure Database (v1.2, to 2025)

    python -m modules.defense.sipri_extra

Graph writes:
  (:ArmsCompany {name})-[:HEADQUARTERED_IN]->(:Country)
  (:ArmsCompany)-[:HAS_ARMS_REVENUE {year, value, total_revenue, arms_share, rank}]->(:Year)
  (:Country)-[:HAS_INDICATOR {year, value}]->(:Indicator) for:
      SIPRI_TOP100_REV    arms revenue of the country's Top-100 companies (USD m)
      SIPRI_FIN_EXPORTS   financial value of arms exports (current USD m)
      SIPRI_MILEX_GDP     military expenditure, % of GDP
      SIPRI_MILEX_PC      military expenditure per capita (current USD)
      SIPRI_MILEX_GOV     military expenditure, % of government spending
  (:Country)-[:SPENDS_ON_DEFENSE {year, value (USD m)}]->(:Year)  extended to 2025
  (:GlobalMetric {code: 'sipri_top100_total'}) with yearly world totals
"""
from __future__ import annotations

import logging
import re
import sys
from pathlib import Path
from typing import Optional

import openpyxl

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from common.db import Neo4jConnection  # noqa: E402
from common.entity_mapper import canonical_country  # noqa: E402

logger = logging.getLogger(__name__)
DIR = ROOT / "data" / "raw" / "sipri"


def _num(v) -> Optional[float]:
    """SIPRI cells: numbers, '<1', '. .', '...', 'xxx', footnote marks."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s.startswith("<"):
        return 0.5
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def _header_row(ws, must_contain: str, max_rows: int = 20) -> tuple[int, list[str]]:
    for i, row in enumerate(ws.iter_rows(min_row=1, max_row=max_rows, values_only=True), start=1):
        cells = [str(c).strip() if c is not None else "" for c in row]
        # a header cell is short and starts with the word; long note lines
        # ("Figures are in ... countries") must not match
        if any(c.lower().startswith(must_contain.lower()) and len(c) < 40 for c in cells):
            return i, cells
    raise ValueError(f"header with {must_contain!r} not found in sheet {ws.title}")


def _indicator(conn: Neo4jConnection, code: str, key: str, name: str, unit: str, direction: Optional[str],
               series: dict[str, dict[int, float]], source: str) -> int:
    conn.run_query(
        "MERGE (i:Indicator {code: $code}) SET i.key = $key, i.name = $name, i.unit = $unit, "
        "i.domain = 'defense', i.direction = $dir, i.source = $source",
        {"code": code, "key": key, "name": name, "unit": unit, "dir": direction, "source": source},
    )
    edges, latest = [], []
    for country, years in series.items():
        for y, v in years.items():
            edges.append({"name": country, "year": y, "value": v})
        if years:
            y = max(years)
            latest.append({"name": country, "year": y, "value": years[y]})
    for i in range(0, len(edges), 5000):
        conn.run_query(
            "UNWIND $rows AS r MATCH (c:Country {name: r.name}) MATCH (i:Indicator {code: $code}) "
            "MERGE (c)-[e:HAS_INDICATOR {year: r.year}]->(i) SET e.value = r.value, e.projected = false",
            {"rows": edges[i:i + 5000], "code": code},
        )
    conn.run_query(
        f"UNWIND $rows AS r MATCH (c:Country {{name: r.name}}) SET c.`{key}` = r.value, c.`{key}_year` = r.year",
        {"rows": latest},
    )
    return len(edges)


# ── Top 100 companies ────────────────────────────────────────────────────────

def load_top100(conn: Neo4jConnection) -> dict[str, int]:
    wb = openpyxl.load_workbook(DIR / "top100_companies.xlsx", read_only=True, data_only=True)
    rows = []
    for sheet in wb.sheetnames:
        if not sheet.isdigit():
            continue
        year = int(sheet)
        ws = wb[sheet]
        hi, header = _header_row(ws, "Company")
        lower = [h.lower() for h in header]
        c_company = next(i for i, h in enumerate(lower) if h.startswith("company"))
        c_country = next(i for i, h in enumerate(lower) if h.startswith("country"))
        c_arms = next(i for i, h in enumerate(lower) if h.startswith("arms revenues"))
        c_total = next((i for i, h in enumerate(lower) if h.startswith("total revenues")), None)
        c_rank = 0
        for r in ws.iter_rows(min_row=hi + 1, values_only=True):
            if r is None or len(r) <= max(c_company, c_country, c_arms):
                continue
            company = str(r[c_company] or "").strip()
            arms = _num(r[c_arms])
            if not company or arms is None:
                continue
            country_raw = str(r[c_country] or "").strip()
            country = canonical_country(country_raw)
            total = _num(r[c_total]) if c_total is not None else None
            rank = _num(r[c_rank])
            rows.append({
                "company": re.sub(r"\s+", " ", company), "country": country, "country_raw": country_raw,
                "year": year, "value": arms, "total": total,
                "share": (arms / total) if total else None, "rank": int(rank) if rank else None,
            })
    conn.run_query("UNWIND $ys AS y MERGE (:Year {year: y})", {"ys": sorted({r["year"] for r in rows})})
    for i in range(0, len(rows), 2000):
        conn.run_query(
            """
            UNWIND $rows AS r
            MERGE (co:ArmsCompany {name: r.company})
            SET co.country_label = r.country_raw
            WITH co, r
            MATCH (y:Year {year: r.year})
            MERGE (co)-[e:HAS_ARMS_REVENUE {year: r.year}]->(y)
            SET e.value = r.value, e.total_revenue = r.total, e.arms_share = r.share, e.rank = r.rank,
                e.unit = 'USD millions (current)', e.source = 'SIPRI Top 100'
            WITH co, r WHERE r.country IS NOT NULL
            MATCH (c:Country {name: r.country})
            MERGE (co)-[:HEADQUARTERED_IN]->(c)
            """,
            {"rows": rows[i:i + 2000]},
        )
    # Country-level series: sum of its Top-100 companies' arms revenue
    series: dict[str, dict[int, float]] = {}
    for r in rows:
        if r["country"]:
            series.setdefault(r["country"], {}).setdefault(r["year"], 0.0)
            series[r["country"]][r["year"]] += r["value"]
    n = _indicator(conn, "SIPRI_TOP100_REV", "arms_industry_revenue_usd_m",
                   "Arms revenue of national Top-100 companies", "USD millions", None, series, "SIPRI Top 100")
    return {"company_years": len(rows), "companies": len({r["company"] for r in rows}), "country_values": n}


def load_top100_total(conn: Neo4jConnection) -> int:
    wb = openpyxl.load_workbook(DIR / "top100_total_revenues.xlsx", read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]
    years, values = [], []
    grid = list(ws.iter_rows(values_only=True))
    for i, row in enumerate(grid):
        ints = [int(c) for c in row if isinstance(c, (int, float)) and 1990 < c < 2100 and float(c).is_integer()]
        if len(ints) >= 5:
            cols = [j for j, c in enumerate(row) if isinstance(c, (int, float)) and 1990 < c < 2100]
            years = [int(row[j]) for j in cols]
            val_row = next((g for g in grid[i + 1:] if g and str(g[0] or "").lower().startswith("current")), None)
            if val_row:
                values = [_num(val_row[j]) if j < len(val_row) else None for j in cols]
            break
    if not years:
        return 0
    conn.run_query(
        "MERGE (g:GlobalMetric {code: 'sipri_top100_total'}) SET g.name = 'Top-100 arms revenues (world)', "
        "g.unit = 'USD billions (current)', g.years = $years, g.values = $values, g.source = 'SIPRI Top 100'",
        {"years": years, "values": values},
    )
    return len(years)


# ── Financial value of arms exports ──────────────────────────────────────────

def load_financial_exports(conn: Neo4jConnection) -> int:
    wb = openpyxl.load_workbook(DIR / "financial_values_arms_exports.xlsx", read_only=True, data_only=True)
    ws = wb["Current US"]
    hi, header = _header_row(ws, "Country")
    year_cols = {i: int(h) for i, h in enumerate(header) if h.isdigit()}
    series: dict[str, dict[int, float]] = {}
    for r in ws.iter_rows(min_row=hi + 1, values_only=True):
        if not r or not r[0]:
            continue
        country = canonical_country(str(r[0]).strip())
        if not country:
            continue
        for i, y in year_cols.items():
            v = _num(r[i]) if i < len(r) else None
            if v is not None:
                series.setdefault(country, {})[y] = v
    return _indicator(conn, "SIPRI_FIN_EXPORTS", "arms_exports_financial_usd_m",
                      "Financial value of arms exports", "USD millions (current)", None, series,
                      "SIPRI (government and industry data)")


# ── Military expenditure database (to 2025) ──────────────────────────────────

def _milex_sheet(wb, sheet: str) -> dict[str, dict[int, float]]:
    ws = wb[sheet]
    hi, header = _header_row(ws, "Country")
    year_cols = {i: int(h) for i, h in enumerate(header) if h.isdigit()}
    out: dict[str, dict[int, float]] = {}
    for r in ws.iter_rows(min_row=hi + 1, values_only=True):
        if not r or not r[0]:
            continue
        country = canonical_country(str(r[0]).strip())
        if not country:
            continue   # region headings ("Africa", "North Africa") and footnotes
        for i, y in year_cols.items():
            v = _num(r[i]) if i < len(r) else None
            if v is not None:
                out.setdefault(country, {})[y] = v
    return out


def load_milex(conn: Neo4jConnection) -> dict[str, int]:
    wb = openpyxl.load_workbook(DIR / "milex_1949_2025.xlsx", read_only=True, data_only=True)
    res = {}
    current = _milex_sheet(wb, "Current US$")   # USD millions
    rows = [{"name": c, "year": y, "value": v} for c, ys in current.items() for y, v in ys.items() if y >= 1990]
    conn.run_query("UNWIND $ys AS y MERGE (:Year {year: y})", {"ys": sorted({r["year"] for r in rows})})
    for i in range(0, len(rows), 5000):
        conn.run_query(
            """
            UNWIND $rows AS r
            MATCH (c:Country {name: r.name}) MATCH (y:Year {year: r.year})
            MERGE (c)-[e:SPENDS_ON_DEFENSE]->(y)
            SET e.value = r.value, e.year = r.year, e.currency = 'USD millions (current)',
                e.source = 'SIPRI Milex 1949-2025 v1.2'
            """,
            {"rows": rows[i:i + 5000]},
        )
    res["spending_values"] = len(rows)
    gdp = _milex_sheet(wb, "Share of GDP")
    gdp = {c: {y: v * 100 if v < 1 else v for y, v in ys.items()} for c, ys in gdp.items()}  # fractions -> %
    res["share_gdp"] = _indicator(conn, "SIPRI_MILEX_GDP", "milex_pct_gdp", "Military expenditure (SIPRI)",
                                  "% of GDP", None, gdp, "SIPRI Milex")
    res["per_capita"] = _indicator(conn, "SIPRI_MILEX_PC", "milex_per_capita_usd", "Military expenditure per capita",
                                   "USD (current)", None, _milex_sheet(wb, "Per capita"), "SIPRI Milex")
    gov = _milex_sheet(wb, "Share of Govt. spending")
    gov = {c: {y: v * 100 if v < 1 else v for y, v in ys.items()} for c, ys in gov.items()}
    res["share_gov"] = _indicator(conn, "SIPRI_MILEX_GOV", "milex_pct_gov", "Military expenditure",
                                  "% of government spending", None, gov, "SIPRI Milex")
    return res


def run() -> dict:
    conn = Neo4jConnection()
    out = {}
    for name, fn in [("top100", load_top100), ("top100_total", load_top100_total),
                     ("financial_exports", load_financial_exports), ("milex", load_milex)]:
        path_ok = True
        try:
            out[name] = fn(conn)
        except FileNotFoundError as exc:
            path_ok = False
            out[name] = f"missing file: {exc}"
        except Exception as exc:   # one bad sheet must not stop the others
            logger.exception("%s failed", name)
            out[name] = f"FAILED: {exc}"
        logger.info("SIPRI %s: %s", name, out[name] if path_ok else "skipped")
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    print(run())
