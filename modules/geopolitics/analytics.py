import sys

sys.path.insert(0, ".")

from collections import defaultdict
from typing import Dict, List, Optional

import community
import networkx as nx

from common.db import Neo4jConnection
from common.intelligence.aggregation import average, weighted_sum
from common.intelligence.normalization import normalize_by_max
from common.intelligence.similarity import bounded_similarity


def _build_diplomacy_graph(conn: Neo4jConnection) -> nx.DiGraph:
    rows = conn.run_query(
        """
        MATCH (a)-[r:DIPLOMATIC_INTERACTION]->(b)
        RETURN a.name AS src, b.name AS dst, r.weight AS weight
        """
    )
    G = nx.DiGraph()
    for row in rows:
        w = row.get("weight")
        if w is None:
            w = 0.0
        try:
            fw = float(w)
        except (TypeError, ValueError):
            fw = 0.0
        src = row.get("src")
        dst = row.get("dst")
        if src is None or dst is None:
            continue
        G.add_edge(str(src), str(dst), weight=fw)
    return G


def _political_score_for_country(conn: Neo4jConnection, name: str) -> Optional[float]:
    rows = conn.run_query(
        """
        MATCH (c:Country {name: $name})-[r:HAS_POLITICAL_SYSTEM]->()
        RETURN r.score AS score
        """,
        {"name": name},
    )
    if not rows:
        return None
    scores: List[float] = []
    for row in rows:
        s = row.get("score")
        if s is None:
            continue
        try:
            scores.append(float(s))
        except (TypeError, ValueError):
            continue
    if not scores:
        return None
    return float(average(scores))


def compute_alignment_scores() -> None:
    conn: Optional[Neo4jConnection] = None
    try:
        try:
            conn = Neo4jConnection()
            results = conn.run_query(
                """
                MATCH (a:Country)-[r:DIPLOMATIC_INTERACTION]->(b:Country)
                RETURN a.name AS country1, b.name AS country2, SUM(r.weight) AS total_weight
                """
            )
            weights: List[float] = []
            for row in results:
                tw = row.get("total_weight")
                if tw is None:
                    continue
                try:
                    weights.append(float(tw))
                except (TypeError, ValueError):
                    continue
            max_weight = max(weights) if weights else 0.0

            pairs = [
                {
                    "c1": r["country1"],
                    "c2": r["country2"],
                    "score": normalize_by_max(float(r["total_weight"]), max_weight),
                }
                for r in results
                if r["total_weight"] is not None
            ]

            write_query = """
            UNWIND $pairs AS pair
            MATCH (a:Country {name: pair.c1})-[r:DIPLOMATIC_INTERACTION]->(b:Country {name: pair.c2})
            SET r.alignment_score = pair.score,
                r.value = pair.score,
                r.normalized_weight = pair.score,
                r.confidence = 0.8
            """

            conn.run_query(write_query, {"pairs": pairs})
            print(f"Alignment scores computed for {len(pairs)} pairs")
        except Exception as exc:
            print(f"compute_alignment_scores failed: {exc}")
            raise
    finally:
        if conn is not None:
            conn.close()


def compute_centrality() -> None:
    conn: Optional[Neo4jConnection] = None
    try:
        try:
            conn = Neo4jConnection()
            G = _build_diplomacy_graph(conn)
            if G.number_of_nodes() == 0:
                print("compute_centrality: no nodes in graph; skipping centrality.")
                return
            try:
                scores = nx.eigenvector_centrality_numpy(G, weight="weight")
            except nx.AmbiguousSolution:
                print(
                    "Note: graph is disconnected; using iterative eigenvector_centrality "
                    "instead of eigenvector_centrality_numpy."
                )
                scores = nx.eigenvector_centrality(G, weight="weight", max_iter=5000)

            nodes = [
                {"name": name, "score": float(score)}
                for name, score in scores.items()
            ]

            write_query = """
            UNWIND $nodes AS node
            MATCH (c:Country {name: node.name})
            SET c.centrality = node.score
            """

            conn.run_query(write_query, {"nodes": nodes})

            top_10 = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:10]
            print("Top 10 most central countries:")
            for name, score in top_10:
                print(f"  {name}: {score:.6f}")
        except Exception as exc:
            print(f"compute_centrality failed: {exc}")
            raise
    finally:
        if conn is not None:
            conn.close()


def detect_blocs() -> None:
    conn: Optional[Neo4jConnection] = None
    try:
        try:
            conn = Neo4jConnection()
            G = _build_diplomacy_graph(conn)
            G_undirected = G.to_undirected()
            if G_undirected.number_of_nodes() == 0:
                print("detect_blocs: no nodes in graph; no blocs detected.")
                return
            partition = community.best_partition(G_undirected, weight="weight")

            blocs = [
                {"name": name, "bloc": int(bloc_id)}
                for name, bloc_id in partition.items()
            ]

            write_query = """
            UNWIND $blocs AS item
            MATCH (c:Country {name: item.name})
            SET c.bloc_id = item.bloc
            """

            conn.run_query(write_query, {"blocs": blocs})

            num_blocs = len(set(partition.values()))
            print(f"Detected {num_blocs} bloc(s)")

            bloc_members: Dict[int, List[str]] = defaultdict(list)
            for name, bloc_id in partition.items():
                bloc_members[bloc_id].append(name)
            for bloc_id, members in sorted(bloc_members.items()):
                print(f"  Bloc {bloc_id} (sample): {', '.join(members[:3])}")
        except Exception as exc:
            print(f"detect_blocs failed: {exc}")
            raise
    finally:
        if conn is not None:
            conn.close()


def compute_political_similarity(country_a: str, country_b: str) -> float:
    conn: Optional[Neo4jConnection] = None
    try:
        try:
            conn = Neo4jConnection()
            score_a = _political_score_for_country(conn, country_a)
            score_b = _political_score_for_country(conn, country_b)
            if score_a is None or score_b is None:
                return 0.0
            return float(bounded_similarity(score_a, score_b))
        except Exception as exc:
            print(f"compute_political_similarity failed: {exc}")
            raise
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    compute_alignment_scores()
    compute_centrality()
    detect_blocs()
