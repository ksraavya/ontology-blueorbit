"""
Country indicators pulled from public, key-free APIs.

Each entry becomes an (:Indicator) node; each country-year value a
(:Country)-[:HAS_INDICATOR {year, value}]->(:Indicator) edge; the latest
actual (non-forecast) value is also copied onto the Country node as
`<key>` + `<key>_year` so scores and simulations can read it in O(1).

`direction` says which way is "worse" for risk-style scores:
  "up"   = higher value means more risk/vulnerability
  "down" = higher value means less risk
  None   = descriptive only (size, population)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class IndicatorDef:
    code: str
    source: str            # "imf" | "wb"
    key: str               # property name on Country
    name: str
    unit: str
    domain: str            # economy | defense | geopolitics | climate
    direction: Optional[str] = None
    wb_source: int = 2     # World Bank database id (2 = WDI, 75 = Worldwide Governance Indicators)


INDICATORS: list[IndicatorDef] = [
    # ── Economy: IMF World Economic Outlook (DataMapper API) ──────────────
    IndicatorDef("NGDP_RPCH", "imf", "gdp_growth_pct", "Real GDP growth", "% per year", "economy", "down"),
    IndicatorDef("PCPIPCH", "imf", "inflation_pct", "Inflation, consumer prices", "% per year", "economy", "up"),
    IndicatorDef("GGXWDG_NGDP", "imf", "gov_debt_pct_gdp", "General government gross debt", "% of GDP", "economy", "up"),
    IndicatorDef("BCA_NGDPD", "imf", "current_account_pct_gdp", "Current account balance", "% of GDP", "economy", "down"),
    IndicatorDef("GGXCNL_NGDP", "imf", "fiscal_balance_pct_gdp", "Government net lending/borrowing", "% of GDP", "economy", "down"),
    IndicatorDef("LUR", "imf", "unemployment_pct", "Unemployment rate", "%", "economy", "up"),
    IndicatorDef("NGDPDPC", "imf", "gdp_per_capita_usd", "GDP per capita, current prices", "USD", "economy", "down"),
    IndicatorDef("NGDPD", "imf", "gdp_usd_bn", "GDP, current prices", "billion USD", "economy"),
    IndicatorDef("PPPSH", "imf", "world_gdp_share_ppp", "Share of world GDP (PPP)", "% of world", "economy"),
    IndicatorDef("LP", "imf", "population_m", "Population", "million people", "economy"),
    # ── Economy: World Bank WDI ───────────────────────────────────────────
    IndicatorDef("NE.TRD.GNFS.ZS", "wb", "trade_pct_gdp", "Trade (exports + imports)", "% of GDP", "economy", "up"),
    IndicatorDef("BX.KLT.DINV.WD.GD.ZS", "wb", "fdi_inflow_pct_gdp", "FDI, net inflows", "% of GDP", "economy"),
    IndicatorDef("FI.RES.TOTL.MO", "wb", "reserves_months_imports", "Total reserves", "months of imports", "economy", "down"),
    # ── Defence: World Bank WDI (SIPRI / IISS sourced) ────────────────────
    IndicatorDef("MS.MIL.XPND.GD.ZS", "wb", "military_exp_pct_gdp", "Military expenditure", "% of GDP", "defense"),
    IndicatorDef("MS.MIL.TOTL.P1", "wb", "armed_forces_personnel", "Armed forces personnel", "people", "defense"),
    IndicatorDef("MS.MIL.MPRT.KD", "wb", "arms_imports_tiv", "Arms imports", "SIPRI TIV", "defense"),
    IndicatorDef("MS.MIL.XPRT.KD", "wb", "arms_exports_tiv", "Arms exports", "SIPRI TIV", "defense"),
    # ── Geopolitics: Worldwide Governance Indicators (estimate, -2.5..2.5) ─
    IndicatorDef("GOV_WGI_PV.EST", "wb", "political_stability", "Political stability & absence of violence", "WGI estimate", "geopolitics", "down", 75),
    IndicatorDef("GOV_WGI_GE.EST", "wb", "gov_effectiveness", "Government effectiveness", "WGI estimate", "geopolitics", "down", 75),
    IndicatorDef("GOV_WGI_RL.EST", "wb", "rule_of_law", "Rule of law", "WGI estimate", "geopolitics", "down", 75),
    IndicatorDef("GOV_WGI_CC.EST", "wb", "control_of_corruption", "Control of corruption", "WGI estimate", "geopolitics", "down", 75),
    IndicatorDef("GOV_WGI_VA.EST", "wb", "voice_accountability", "Voice and accountability", "WGI estimate", "geopolitics", "down", 75),
    IndicatorDef("GOV_WGI_RQ.EST", "wb", "regulatory_quality", "Regulatory quality", "WGI estimate", "geopolitics", "down", 75),
    # ── Climate / energy: World Bank WDI ──────────────────────────────────
    IndicatorDef("EG.IMP.CONS.ZS", "wb", "energy_imports_pct_use", "Energy imports, net", "% of energy use", "climate", "up"),
    IndicatorDef("EG.FEC.RNEW.ZS", "wb", "renewable_share_pct", "Renewable energy consumption", "% of final energy", "climate", "down"),
    IndicatorDef("ER.H2O.FWST.ZS", "wb", "water_stress_pct", "Freshwater withdrawal vs available", "%", "climate", "up"),
    IndicatorDef("AG.LND.AGRI.ZS", "wb", "agricultural_land_pct", "Agricultural land", "% of land area", "climate"),
]

FIRST_YEAR = 2010
# IMF WEO includes forecasts; anything after this is flagged projected=true
# and never copied onto the Country node as a "latest actual" value.
LAST_ACTUAL_YEAR = 2025
