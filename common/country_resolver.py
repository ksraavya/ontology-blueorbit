from __future__ import annotations

"""
Resolve free-text country names to the exact `Country.name` stored in Neo4j.

Why this exists: different loaders wrote names differently. The defense
enrichment writes "Iran", "Vietnam", "Syria"; entity_mapper writes
"Russian Federation"; pycountry would say "Iran, Islamic Republic of".
Every Cypher query in the simulation matches `{name: $target}` exactly, so
"Russia" vs "Russian Federation" silently returns zero rows and every
delta becomes 0. This module is the single place that bridges the gap:

    user text -> candidate spellings -> the spelling that exists in the graph

Also exposes `find_entities(text)` for the rule-based intent parser: it
scans a sentence and returns countries / alliances / regions in the order
they appear.
"""

import re
import threading
import time
from dataclasses import dataclass
from typing import Optional

import pycountry

from common.entity_mapper import normalize_entity

# Hand-written aliases for how people actually type countries.
# Values are *candidate* names; the graph decides which spelling wins.
_ALIASES: dict[str, list[str]] = {
    "us": ["United States"], "usa": ["United States"], "u.s.": ["United States"],
    "u.s.a.": ["United States"], "america": ["United States"],
    "united states of america": ["United States"], "the states": ["United States"],
    "uk": ["United Kingdom"], "u.k.": ["United Kingdom"], "britain": ["United Kingdom"],
    "great britain": ["United Kingdom"], "england": ["United Kingdom"],
    "russia": ["Russian Federation", "Russia"],
    "china": ["China"], "prc": ["China"], "mainland china": ["China"],
    "taiwan": ["Taiwan", "Taiwan, Province of China"],
    "south korea": ["South Korea", "Korea, Republic of"], "korea": ["South Korea", "Korea, Republic of"],
    "north korea": ["North Korea", "Korea, Democratic People's Republic of"], "dprk": ["North Korea"],
    "iran": ["Iran", "Iran, Islamic Republic of"],
    "syria": ["Syria", "Syrian Arab Republic"],
    "vietnam": ["Vietnam", "Viet Nam"],
    "laos": ["Laos", "Lao People's Democratic Republic"],
    "venezuela": ["Venezuela", "Venezuela, Bolivarian Republic of"],
    "bolivia": ["Bolivia", "Bolivia, Plurinational State of"],
    "tanzania": ["Tanzania", "Tanzania, United Republic of"],
    "moldova": ["Moldova", "Moldova, Republic of"],
    "turkey": ["Turkey", "Türkiye", "Turkiye"], "turkiye": ["Turkey", "Türkiye"],
    "czech republic": ["Czechia", "Czech Republic"],
    "uae": ["United Arab Emirates"], "emirates": ["United Arab Emirates"],
    "saudi": ["Saudi Arabia"], "ksa": ["Saudi Arabia"],
    "drc": ["Congo, The Democratic Republic of the", "Democratic Republic of the Congo"],
    "congo": ["Congo", "Congo, The Democratic Republic of the"],
    "ivory coast": ["Côte d'Ivoire", "Cote d'Ivoire"],
    "palestine": ["Palestine, State of", "Palestine"],
    "burma": ["Myanmar"],
}

ALLIANCES: dict[str, str] = {
    "nato": "NATO", "north atlantic treaty organization": "NATO",
    "brics": "BRICS", "sco": "SCO", "shanghai cooperation organisation": "SCO",
    "shanghai cooperation organization": "SCO",
    "quad": "QUAD", "asean": "ASEAN", "african union": "African Union",
    "arab league": "Arab League", "csto": "CSTO",
    # Trade blocs (used by trade_agreement scenarios as free text)
    "rcep": "RCEP", "cptpp": "CPTPP", "eu": "European Union",
    "european union": "European Union", "mercosur": "Mercosur",
    "usmca": "USMCA", "opec": "OPEC", "gcc": "GCC", "aukus": "AUKUS",
    "g7": "G7", "g20": "G20",
}

REGIONS: dict[str, str] = {
    "south asia": "South Asia", "middle east": "Middle East",
    "eastern europe": "Eastern Europe", "western europe": "Western Europe",
    "east asia": "East Asia", "southeast asia": "Southeast Asia",
    "north america": "North America", "south america": "South America",
    "latin america": "South America", "central asia": "Central Asia",
    "africa": "Africa", "sahel": "Africa", "horn of africa": "Africa",
    "europe": "Western Europe", "gulf": "Middle East",
}


def _key(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().casefold())


def _build_surface_forms() -> dict[str, list[str]]:
    """surface text (lowercase) -> candidate canonical names."""
    forms: dict[str, list[str]] = {k: list(v) for k, v in _ALIASES.items()}
    for c in pycountry.countries:
        names = [c.name]
        for attr in ("common_name", "official_name"):
            val = getattr(c, attr, None)
            if val:
                names.append(val)
        for n in names:
            forms.setdefault(_key(n), [])
            for cand in names:
                if cand not in forms[_key(n)]:
                    forms[_key(n)].append(cand)
    return forms


_SURFACE_FORMS = _build_surface_forms()


# =========================================================
# GRAPH NAME CACHE
# =========================================================

_cache_lock = threading.Lock()
_graph_names: dict[str, tuple[str, int]] = {}   # casefold name -> (exact name, degree)
_graph_loaded_at = 0.0
_TTL_S = 600


def _load_graph_names() -> dict[str, tuple[str, int]]:
    global _graph_names, _graph_loaded_at
    now = time.time()
    if _graph_names and now - _graph_loaded_at < _TTL_S:
        return _graph_names
    with _cache_lock:
        if _graph_names and now - _graph_loaded_at < _TTL_S:
            return _graph_names
        try:
            from common.db import Neo4jConnection
            rows = Neo4jConnection().run_query(
                "MATCH (c:Country) RETURN c.name AS name, COUNT { (c)--() } AS degree"
            )
            loaded = {}
            for r in rows:
                name = r.get("name")
                if not name:
                    continue
                deg = int(r.get("degree") or 0)
                prev = loaded.get(name.casefold())
                if prev is None or deg > prev[1]:
                    loaded[name.casefold()] = (name, deg)
            _graph_names = loaded
            _graph_loaded_at = now
        except Exception:
            # Graph unreachable: resolution falls back to canonical spellings.
            _graph_names = {}
        return _graph_names


def invalidate_cache() -> None:
    global _graph_loaded_at
    _graph_loaded_at = 0.0


def candidates_for(name: str) -> list[str]:
    """All plausible spellings for `name`, most likely first."""
    out: list[str] = []

    def add(n: Optional[str]) -> None:
        if n and n not in out:
            out.append(n)

    add(name.strip())
    for cand in _SURFACE_FORMS.get(_key(name), []):
        add(cand)
    add(normalize_entity(name))
    try:
        c = pycountry.countries.lookup(name.strip())
        add(c.name)
        add(getattr(c, "common_name", None))
        add(getattr(c, "official_name", None))
    except LookupError:
        pass
    return out


def resolve_country(name: Optional[str]) -> Optional[str]:
    """Return the graph's spelling of `name` (or the best canonical guess)."""
    if not name or not name.strip():
        return name
    cands = candidates_for(name)
    graph = _load_graph_names()
    if graph:
        hits = [graph[c.casefold()] for c in cands if c.casefold() in graph]
        if hits:
            # Several spellings present (e.g. "Iran" and "Iran, Islamic Republic of"):
            # the better-connected node is the one the pipelines actually used.
            return max(hits, key=lambda h: h[1])[0]
    # Graph empty or name absent: prefer the common spelling from our aliases.
    if _key(name) in _ALIASES:
        return _ALIASES[_key(name)][0]
    return normalize_entity(name) or name


def resolve_entity(name: Optional[str]) -> Optional[str]:
    """Like resolve_country, but leaves alliances / regions / blocs as their canonical label."""
    if not name or not name.strip():
        return name
    k = _key(name)
    if k in ALLIANCES:
        return ALLIANCES[k]
    if k in REGIONS:
        return REGIONS[k]
    return resolve_country(name)


# =========================================================
# ENTITY SCANNING (for rule-based parsing)
# =========================================================

@dataclass
class FoundEntity:
    kind: str        # "country" | "alliance" | "region"
    name: str        # canonical label (countries: resolved graph name)
    start: int       # char offset in the lowercased query
    surface: str     # the text that matched


def _scan_patterns() -> list[tuple[str, str, re.Pattern[str]]]:
    items: list[tuple[str, str]] = []
    items += [(k, "country") for k in _SURFACE_FORMS]
    items += [(k, "alliance") for k in ALLIANCES]
    items += [(k, "region") for k in REGIONS]
    # Longest first so "south korea" wins over "korea" and "north america" over "america".
    items.sort(key=lambda kv: -len(kv[0]))
    pats = []
    for surface, kind in items:
        if len(surface) < 2:
            continue
        pats.append((surface, kind, re.compile(r"(?<![\w])" + re.escape(surface) + r"(?![\w])")))
    return pats


_PATTERNS = _scan_patterns()

# Two-letter tokens that collide with English words; only match them in caps.
_CASE_SENSITIVE = {"us": "US", "uk": "UK", "eu": "EU"}


def find_entities(text: str) -> list[FoundEntity]:
    lowered = text.casefold()
    taken = [False] * len(lowered)
    found: list[FoundEntity] = []
    for surface, kind, pat in _PATTERNS:
        for m in pat.finditer(lowered):
            s, e = m.span()
            if any(taken[s:e]):
                continue
            if surface in _CASE_SENSITIVE and text[s:e] != _CASE_SENSITIVE[surface]:
                # "us" in "tell us" is not the United States; "US" or "the us" is.
                if not lowered[max(0, s - 4):s] == "the ":
                    continue
            for i in range(s, e):
                taken[i] = True
            if kind == "country":
                name = resolve_country(_SURFACE_FORMS[surface][0]) or _SURFACE_FORMS[surface][0]
            elif kind == "alliance":
                name = ALLIANCES[surface]
            else:
                name = REGIONS[surface]
            found.append(FoundEntity(kind=kind, name=name, start=s, surface=text[s:e]))
    found.sort(key=lambda f: f.start)
    # De-duplicate the same entity mentioned twice, keep first mention.
    seen: set[tuple[str, str]] = set()
    unique = []
    for f in found:
        if (f.kind, f.name) in seen:
            continue
        seen.add((f.kind, f.name))
        unique.append(f)
    return unique
