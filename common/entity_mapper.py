from __future__ import annotations

from typing import Dict, Optional
import re

import pycountry


"""
Entity normalization utility.

Supports:
- Countries (ISO-standard via pycountry)
- Organizations (custom mappings)
- General entities fallback

Goals:
- Ensure consistency across modules
- Avoid duplicate nodes in graph
- Handle messy real-world data gracefully
"""


# =========================================================
# CUSTOM MAPPINGS (CRITICAL FOR REAL DATA)
# =========================================================

_CUSTOM_MAPPINGS: Dict[str, str] = {

    # --------------------------
    # United States
    # --------------------------
    "usa": "United States",
    "us": "United States",
    "u.s.": "United States",
    "u.s.a": "United States",
    "america": "United States",

    # --------------------------
    # United Kingdom
    # --------------------------
    "uk": "United Kingdom",
    "u.k.": "United Kingdom",
    "britain": "United Kingdom",
    "great britain": "United Kingdom",

    # --------------------------
    # Korea
    # --------------------------
    "korea, south": "South Korea",
    "republic of korea": "South Korea",
    "korea, north": "North Korea",
    "dprk": "North Korea",

    # --------------------------
    # Russia
    # --------------------------
    "russia": "Russian Federation",

    # --------------------------
    # UAE
    # --------------------------
    "uae": "United Arab Emirates",

    # --------------------------
    # Organizations
    # --------------------------
    "eu": "European Union",
    "e.u.": "European Union",
    "un": "United Nations",
    "u.n.": "United Nations",
    "nato": "NATO",
    "wto": "World Trade Organization",

    # --------------------------
    # Common variants
    # --------------------------
    "ivory coast": "Côte d'Ivoire",
}


# =========================================================
# TEXT CLEANING
# =========================================================

def _clean_text(text: str) -> str:
    """
    Normalize text:
    - strip whitespace
    - collapse spaces
    - remove weird punctuation
    """
    text = text.strip()
    text = re.sub(r"\s+", " ", text)
    return text


def _normalize_key(text: str) -> str:
    """
    Normalize key for lookup:
    - lowercase
    - remove punctuation
    """
    text = text.casefold()
    text = re.sub(r"[^\w\s]", "", text)
    return text


# =========================================================
# ISO COUNTRY LOOKUP
# =========================================================

def _lookup_country(name: str) -> Optional[str]:
    """
    Resolve country using ISO standards via pycountry.
    """
    try:
        country = pycountry.countries.lookup(name)
        return country.name
    except LookupError:
        return None


# =========================================================
# MAIN NORMALIZATION FUNCTION
# =========================================================

def normalize_entity(
    name: Optional[str],
    entity_type: str = "country"
) -> Optional[str]:
    """
    Normalize an entity name.

    Steps:
    1. Handle None
    2. Clean input
    3. Apply custom mappings
    4. Use ISO lookup (for countries)
    5. Fallback to title case

    Args:
        name: raw entity name
        entity_type: "country", "organization", or "generic"

    Returns:
        Normalized entity name
    """

    if name is None:
        return None

    cleaned = _clean_text(name)

    if not cleaned:
        return cleaned

    key = _normalize_key(cleaned)

    # --------------------------
    # Step 1: custom mappings
    # --------------------------
    if key in _CUSTOM_MAPPINGS:
        return _CUSTOM_MAPPINGS[key]

    # --------------------------
    # Step 2: ISO lookup (countries)
    # --------------------------
    if entity_type == "country":
        iso_name = _lookup_country(cleaned)
        if iso_name:
            return iso_name

    # --------------------------
    # Step 3: fallback
    # --------------------------
    return cleaned.title()


# =========================================================
# BULK NORMALIZATION (UTILITY)
# =========================================================

def normalize_entities(
    names: list[str],
    entity_type: str = "country"
) -> list[str]:
    """
    Normalize a list of entity names.
    """
    return [
        normalize_entity(name, entity_type)
        for name in names
        if name is not None
    ]
