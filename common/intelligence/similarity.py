from __future__ import annotations

from typing import Iterable, List, Dict
import math


"""
Similarity Utilities

Used for:
- Political similarity
- Alignment scoring
- Feature comparison
- Clustering support

Core:
    similarity = 1 - distance
"""


# =========================================================
# BASIC SCALAR SIMILARITY
# =========================================================

def similarity(a: float, b: float) -> float:
    """
    Compute similarity between two scalar values.

    Example:
        democracy score similarity
    """
    return 1.0 - abs(a - b)


# =========================================================
# CLAMPED SCALAR SIMILARITY
# =========================================================

def bounded_similarity(a: float, b: float) -> float:
    """
    Ensure similarity stays in [0,1]
    """
    val = similarity(a, b)
    return max(0.0, min(1.0, val))


# =========================================================
# EUCLIDEAN DISTANCE
# =========================================================

def euclidean_distance(vec1: Iterable[float], vec2: Iterable[float]) -> float:
    """
    Compute Euclidean distance between two vectors.
    """
    v1: List[float] = list(vec1)
    v2: List[float] = list(vec2)

    if len(v1) != len(v2):
        raise ValueError("Vectors must have same length")

    return math.sqrt(sum((a - b) ** 2 for a, b in zip(v1, v2)))


# =========================================================
# COSINE SIMILARITY
# =========================================================

def cosine_similarity(vec1: Iterable[float], vec2: Iterable[float]) -> float:
    """
    Compute cosine similarity between vectors.

    Range: [-1, 1]
    """
    v1: List[float] = list(vec1)
    v2: List[float] = list(vec2)

    if len(v1) != len(v2):
        raise ValueError("Vectors must have same length")

    dot_product = sum(a * b for a, b in zip(v1, v2))
    mag1 = math.sqrt(sum(a * a for a in v1))
    mag2 = math.sqrt(sum(b * b for b in v2))

    if mag1 == 0 or mag2 == 0:
        return 0.0

    return dot_product / (mag1 * mag2)


# =========================================================
# NORMALIZED COSINE (0–1)
# =========================================================

def normalized_cosine_similarity(vec1: Iterable[float], vec2: Iterable[float]) -> float:
    """
    Normalize cosine similarity to [0,1]
    """
    cos = cosine_similarity(vec1, vec2)
    return (cos + 1) / 2


# =========================================================
# DICTIONARY-BASED SIMILARITY
# =========================================================

def dict_similarity(
    d1: Dict[str, float],
    d2: Dict[str, float]
) -> float:
    """
    Compare two dictionaries (feature vectors).

    Missing keys treated as 0.
    """
    keys = set(d1.keys()).union(d2.keys())

    v1 = [d1.get(k, 0.0) for k in keys]
    v2 = [d2.get(k, 0.0) for k in keys]

    return normalized_cosine_similarity(v1, v2)
