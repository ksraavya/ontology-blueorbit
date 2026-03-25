import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

from modules.economy.ingest import load_sanctions_data, load_trade_agreements_data
from modules.economy.transform import transform_sanctions_data, transform_trade_agreements_data
from modules.economy.compute import compute_sanctions_metrics, compute_trade_agreement_metrics
from modules.economy.load import load_to_graph

# Sanctions
print("Loading sanctions...")
sanctions_raw = load_sanctions_data()
sanctions_clean = transform_sanctions_data(sanctions_raw)
sanctions_enriched = compute_sanctions_metrics(sanctions_clean)
load_to_graph(sanctions_enriched)
print(f"Sanctions done: {len(sanctions_enriched)} relationships")

# Trade agreements
print("Loading trade agreements...")
agreements_raw = load_trade_agreements_data()
agreements_clean = transform_trade_agreements_data(agreements_raw)
agreements_enriched = compute_trade_agreement_metrics(agreements_clean)
load_to_graph(agreements_enriched)
print(f"Trade agreements done: {len(agreements_enriched)} relationships")