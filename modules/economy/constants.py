from __future__ import annotations
 
# Row type identifiers (used in ingest.py and transform.py)
TRADE_TYPE = "trade"
ENERGY_TYPE = "energy"
GDP_TYPE = "gdp"
INFLATION_TYPE = "inflation"
TRADE_BALANCE_TYPE = "trade_balance"
 
# Neo4j node labels
COUNTRY_LABEL = "Country"
METRIC_LABEL = "Metric"
 
# Entity type passed to normalize_entity()
ECONOMY_ENTITY_TYPE = "country"
 
# Indicator display names (used as Metric node names in the graph)
GDP_METRIC_NAME = "GDP"
INFLATION_METRIC_NAME = "Inflation"
TRADE_BALANCE_METRIC_NAME = "Trade Balance"

# Minimum value thresholds for graph insertion
TRADE_MIN_VALUE_USD = 1_000_000      # $1M
ENERGY_MIN_VALUE_USD = 500_000       # $500K

# Minimum dependency threshold for graph insertion
MIN_DEPENDENCY = 0.01

# Trade agreements
WTO_SOURCE = "WTO RTA"
TRADE_AGREEMENT_STATUS_ACTIVE = "In Force"
OFAC_SOURCE = "OFAC SDN"