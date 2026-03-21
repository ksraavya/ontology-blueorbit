# ===== common/country_mapper.py =====

from __future__ import annotations

from typing import Dict, Optional

import pycountry


"""
Country normalization utility.

Strategy:
1. Clean input
2. Apply custom overrides (for messy real-world data)
3. Use ISO lookup via pycountry
4. Fallback to cleaned name

This ensures:
- Global consistency
- No duplicate country nodes
- Compatibility with real datasets
"""


# Custom overrides (VERY IMPORTANT for real-world messy data)
_CUSTOM_MAPPINGS: Dict[str, str] = {
    # United States
    "usa": "United States",
    "us": "United States",
    "u.s.": "United States",
    "america": "United States",

    # United Kingdom
    "uk": "United Kingdom",
    "britain": "United Kingdom",
    "great britain": "United Kingdom",

    # South Korea
    "korea, south": "South Korea",
    "republic of korea": "South Korea",

    # North Korea
    "korea, north": "North Korea",
    "dprk": "North Korea",

    # Russia
    "russia": "Russian Federation",

    # UAE
    "uae": "United Arab Emirates",

    # Dataset-specific messy names
    "soviet union": "Russian Federation",
    "congo, dr": "Congo, The Democratic Republic of the",
    "congo, republic": "Republic of the Congo",
    "cote d'ivoire": "Côte d'Ivoire",
    "turkiye": "Turkey",
    "korea, south": "South Korea",
    "korea, north": "North Korea",
    "cape verde": "Cabo Verde",
    "czech republic": "Czechia",
    "north macedonia": "North Macedonia",
    "myanmar": "Myanmar",
    "taiwan": "Taiwan",
    "kosovo": "Kosovo",

    # EU (not in pycountry)
    "eu": "European Union",
}


def _lookup_iso(name: str) -> Optional[str]:
    """
    Resolve country using pycountry (ISO standard).
    """
    try:
        country = pycountry.countries.lookup(name)
        return country.name
    except LookupError:
        return None


def normalize_country(name: Optional[str]) -> Optional[str]:
    """
    Normalize a country/entity name.

    Steps:
    - Handle None
    - Strip whitespace
    - Apply overrides
    - Use ISO lookup
    - Fallback to title case
    """

    if name is None:
        return None

    stripped = name.strip()
    if not stripped:
        return stripped

    key = stripped.casefold()

    # Step 1: custom overrides
    if key in _CUSTOM_MAPPINGS:
        return _CUSTOM_MAPPINGS[key]

    # Step 2: ISO lookup
    iso_name = _lookup_iso(stripped)
    if iso_name:
        return iso_name

    # Step 3: fallback
    return stripped.title()