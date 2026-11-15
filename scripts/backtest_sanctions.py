"""
Backtest: the 2022 sanctions on Russia vs. what the model predicts.

    python scripts/backtest_sanctions.py            # prints + writes docs/backtest_russia_2022.md

Model side (simulation/calibration.py), using PRE-shock data (2020):
    exposure = Russia's trade with the sanctioning coalition / Russia's GDP
    severity = shared GDP-exposure scale
    predicted direction: coalition trade collapses; partners re-route part of it

Reality side (CEPII BACI, same graph):
    change in Russia <-> coalition trade, 2020 -> 2023 / 2024
    change in Russia <-> China, India, Türkiye trade (re-routing)

Nothing here is fitted: the coefficients are the same ones the simulator
uses. The point is to check direction and order of magnitude, and to state
the model's known limits honestly in the report.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from common.db import Neo4jConnection  # noqa: E402
from simulation.calibration import K_INFLUENCE, K_RISK, K_TRADE, exposure_delta, severity_from_exposure  # noqa: E402

EU27 = ["Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czechia", "Denmark", "Estonia", "Finland",
        "France", "Germany", "Greece", "Hungary", "Ireland", "Italy", "Latvia", "Lithuania", "Luxembourg",
        "Malta", "Netherlands", "Poland", "Portugal", "Romania", "Slovakia", "Slovenia", "Spain", "Sweden"]
COALITION = EU27 + ["United States", "United Kingdom", "Japan", "Canada", "Australia", "Korea, Republic of",
                    "Switzerland", "Norway", "New Zealand"]
REROUTE = ["China", "India", "Türkiye", "Kazakhstan"]  # UAE omitted: BACI reporting gaps (zero rows)
RUSSIA = "Russian Federation"


def trade(conn, partners: list[str], year: int) -> float:
    q = """
        MATCH (r:Country {name: $ru})-[e:EXPORTS_TO]-(p:Country)
        WHERE p.name IN $partners AND e.year = $year
        RETURN sum(e.value) AS v"""
    return float(conn.run_query(q, {"ru": RUSSIA, "partners": partners, "year": year})[0]["v"] or 0.0)


def gdp(conn, year: int) -> float:
    r = conn.run_query("MATCH (c:Country {name: $ru})-[g:HAS_GDP]->() WHERE g.year = $y RETURN g.value AS v",
                       {"ru": RUSSIA, "y": year})
    return float(r[0]["v"]) if r and r[0]["v"] else 0.0


def main() -> int:
    conn = Neo4jConnection()
    years = [r["y"] for r in conn.run_query("MATCH ()-[e:EXPORTS_TO]->() RETURN DISTINCT e.year AS y ORDER BY y")]
    base = 2020 if 2020 in years else years[0]
    post = [y for y in years if y >= 2023]

    coal_base = trade(conn, COALITION, base)
    gdp_base = gdp(conn, base)
    exposure = coal_base / gdp_base if gdp_base else 0.0

    lines = []
    out = lines.append
    out("# Backtest: 2022 sanctions on Russia\n")
    out(f"Data: CEPII BACI bilateral trade in the graph (years available: {', '.join(map(str, years))}); "
        f"World Bank GDP. Coalition = EU27 + US, UK, Japan, Canada, Australia, South Korea, Switzerland, Norway, New Zealand.\n")
    out("## Model prediction (from pre-shock data, " + str(base) + ")\n")
    out(f"- Russia–coalition trade at risk: **${coal_base / 1e9:,.1f}B**")
    out(f"- Russia GDP {base}: ${gdp_base / 1e9:,.0f}B → exposure **{exposure * 100:.1f}% of GDP** → severity **{severity_from_exposure(exposure)}**")
    out(f"- Projected score changes at magnitude 1.0: trade vulnerability +{exposure_delta(exposure, K_TRADE, 1):.2f}, "
        f"economic influence −{exposure_delta(exposure, K_INFLUENCE, 1):.2f}, global risk +{exposure_delta(exposure, K_RISK, 1):.2f}")
    out("- Predicted direction: coalition trade falls sharply; part re-routes to non-sanctioning partners.\n")

    out("## What happened (BACI)\n")
    out("| Partner group | " + " | ".join(str(y) for y in [base] + post) + " | change " + str(base) + "→" + str(post[-1] if post else base) + " |")
    out("|---|" + "---|" * (len(post) + 2))
    rows = [("Sanctioning coalition", COALITION)] + [(p, [p]) for p in REROUTE]
    summary = {}
    for label, group in rows:
        vals = [trade(conn, group, y) for y in [base] + post]
        change = (vals[-1] - vals[0]) / vals[0] * 100 if vals[0] else float("nan")
        summary[label] = change
        out(f"| {label} | " + " | ".join(f"${v / 1e9:,.1f}B" for v in vals) + f" | {change:+.0f}% |")
    out("")

    coal_change = summary["Sanctioning coalition"]
    reroute_up = [k for k in REROUTE if summary.get(k, 0) > 0]
    out("## Verdict\n")
    out(f"- **Direction: {'correct' if coal_change < 0 else 'WRONG'}.** Coalition trade changed {coal_change:+.0f}%, "
        f"the model predicted a collapse (exposure {exposure * 100:.1f}% of GDP, severity {severity_from_exposure(exposure)}).")
    out(f"- **Re-routing: {'observed' if reroute_up else 'not observed'}.** Trade grew with: "
        f"{', '.join(f'{k} ({summary[k]:+.0f}%)' for k in reroute_up) or 'none'} — the model's cascade logic assumes "
        f"partial re-routing (CASCADE_DAMPING = 0.5) and does not yet model trade *gains* for third countries.")
    out(f"- **Magnitude:** the model treats sanctioned trade as fully at risk at magnitude 1.0; "
        f"the observed fall of {abs(coal_change):.0f}% corresponds to magnitude ≈ {abs(coal_change) / 100:.2f} "
        f"(sanctions exempted energy and some goods at first).")
    out("- **Limits:** coefficients are assumptions, not estimated; prices (2022 energy spike) inflate USD values; "
        "BACI lags 1–2 years. This is a direction and order-of-magnitude check, not a forecast accuracy score.")

    text = "\n".join(lines) + "\n"
    print(text)
    doc = ROOT / "docs" / "backtest_russia_2022.md"
    doc.parent.mkdir(exist_ok=True)
    doc.write_text(text, encoding="utf-8")
    print(f"written: {doc}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
