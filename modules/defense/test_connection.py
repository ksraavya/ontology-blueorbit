from __future__ import annotations

import sys
from pathlib import Path


# When running `python modules/defense/test_connection.py`, Python sets
# `sys.path[0]` to `modules/defense`, so `import common` fails unless we
# explicitly add the repository root.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def main() -> None:
    conn = None
    try:
        # Uses common/db.py which loads NEO4J_* from .env automatically.
        from common.db import Neo4jConnection

        conn = Neo4jConnection()

        query = "RETURN 'Defence module connected' AS msg"
        result = conn.run_query(query)

        print(result)
    except Exception as exc:
        print(f"Neo4j connection test failed: {exc}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()

