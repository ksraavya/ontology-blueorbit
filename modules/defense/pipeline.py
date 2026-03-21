from __future__ import annotations

import sys
import time
from pathlib import Path


MILEX_FILE = "data/raw/sipri_milex.xlsx"
ARMS_1950_FILE = "data/raw/sipri_arms_1950_1980.csv"
ARMS_1981_FILE = "data/raw/sipri_arms_1981_2000.csv"

# Ensure `from modules...` works when executing this file directly:
# `python modules/defense/pipeline.py`
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def run_defense_pipeline() -> None:
    start = time.time()
    processed_dir = Path("data/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)

    # STEP 1 — SIPRI Milex Spending
    try:
        print("=== STEP 1: SIPRI Military Spending ===")
        from modules.defense.loaders.milex_loader import load_milex_spending
        from modules.defense.cleaner import clean_milex_data
        from modules.defense.inserter import (
            insert_defense_spending,
            verify_spending_insert,
        )

        df = load_milex_spending(MILEX_FILE)
        df = clean_milex_data(df)
        df.to_csv(processed_dir / "clean_milex.csv", index=False)
        insert_defense_spending(df)
        verify_spending_insert()
    except Exception as e:
        print(f"Step 1 failed: {e}")

    # STEP 2 — SIPRI Arms Exports
    try:
        print("=== STEP 2: SIPRI Arms Exports ===")
        from modules.defense.loaders.arms_loader import load_all_arms_exports
        from modules.defense.cleaner import clean_arms_data
        from modules.defense.inserter import (
            insert_arms_exports,
            verify_arms_insert,
        )

        df = load_all_arms_exports()
        df = clean_arms_data(df)
        df.to_csv(processed_dir / "clean_arms.csv", index=False)
        insert_arms_exports(df)
        verify_arms_insert()
    except Exception as e:
        print(f"Step 2 failed: {e}")

    # STEP 3 — ACLED Conflict Data
    try:
        print("=== STEP 3: ACLED Conflict Data ===")
        from modules.defense.loaders.acled_loader import load_acled_summary
        from modules.defense.cleaner import clean_acled_data
        from modules.defense.inserter import (
            insert_conflict_stats,
            verify_conflict_insert,
        )

        df = load_acled_summary()
        df = clean_acled_data(df)
        df.to_csv(processed_dir / "clean_acled.csv", index=False)
        insert_conflict_stats(df)
        verify_conflict_insert()
    except Exception as e:
        print(f"Step 3 failed: {e}")

    total = time.time() - start
    print(f"Total time taken: {total:.2f}s")


if __name__ == "__main__":
    run_defense_pipeline()

