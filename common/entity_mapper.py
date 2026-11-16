from __future__ import annotations

from functools import lru_cache
from typing import Dict, Optional
import re
import unicodedata

import pycountry


"""
Entity normalization utility.

Every loader passes raw names through normalize_entity() before writing a
Country node, so this is the single place that decides "which node is this
country". It must map every spelling of one country to ONE name, or a
country's data ends up split across several nodes (before this rewrite:
"Korea, Republic of" held trade, "Korea, Rep." held GDP, "Korea, South"
held defence spending, "South Korea" held alliances).

Canonical name = pycountry's `name` (ISO 3166 short name), e.g.
"Korea, Republic of", "Russian Federation", "Viet Nam", "Türkiye".
The frontend's COUNTRY_NAME_MAP already targets these names.

Resolution order for countries:
  1. hand-written aliases (source-specific spellings seen in our data)
  2. pycountry lookup of the cleaned name and of variants with source
     formatting stripped ("Egypt, Arab Rep." -> "Egypt", "Iran (Islamic
     Republic Of)" -> "Iran", "Gambia, The" -> "Gambia")
  3. historical states keep a fixed name (SIPRI arms data goes back to 1950)
  4. otherwise: cleaned title case (callers / the cleanup step decide)
"""


# =========================================================
# ALIASES: source spelling (casefolded, normalised) -> ISO alpha-3
# =========================================================

_COUNTRY_ALIASES: Dict[str, str] = {
    # common English
    "usa": "USA", "us": "USA", "u.s.": "USA", "u.s.a": "USA", "u.s.a.": "USA",
    "america": "USA", "united states of america": "USA",
    "uk": "GBR", "u.k.": "GBR", "britain": "GBR", "great britain": "GBR", "england": "GBR",
    "russia": "RUS", "soviet russia": "RUS",
    "south korea": "KOR", "korea, south": "KOR", "korea (south)": "KOR", "republic of korea": "KOR",
    "korea rep": "KOR", "korea": "KOR",
    "north korea": "PRK", "korea, north": "PRK", "korea (north)": "PRK", "dprk": "PRK",
    "korea dem peoples rep": "PRK", "korea, dem. people's rep.": "PRK",
    "iran": "IRN", "syria": "SYR", "vietnam": "VNM", "laos": "LAO", "lao pdr": "LAO",
    "venezuela": "VEN", "venezuela, rb": "VEN", "bolivia": "BOL", "tanzania": "TZA",
    "moldova": "MDA", "turkey": "TUR", "turkiye": "TUR", "czech republic": "CZE",
    "uae": "ARE", "emirates": "ARE", "taiwan": "TWN", "chinese taipei": "TWN",
    "palestine": "PSE", "state of palestine": "PSE", "west bank and gaza": "PSE",
    "palestine/west bank": "PSE", "palestine/gaza": "PSE", "gaza": "PSE",
    "burma": "MMR", "burma/myanmar": "MMR",
    "ivory coast": "CIV", "cote d'ivoire": "CIV", "côte d'ivoire": "CIV",
    "cape verde": "CPV", "east timor": "TLS", "timor leste": "TLS", "timor-leste": "TLS",
    "brunei": "BRN", "swaziland": "SWZ", "macedonia": "MKD", "north macedonia": "MKD",
    "vatican": "VAT", "vatican city": "VAT", "holy see": "VAT",
    "micronesia": "FSM", "micronesia, fed. sts.": "FSM",
    "naoero": "NRU", "curacao": "CUW", "reunion": "REU",
    "hong kong": "HKG", "hong kong sar, china": "HKG", "hong kong, china": "HKG",
    "china, hong kong special administrative region": "HKG",
    "macao": "MAC", "macau": "MAC", "macao sar, china": "MAC", "macao, china": "MAC",
    "the gambia": "GMB", "gambia, the": "GMB", "bahamas, the": "BHS", "the bahamas": "BHS",
    "kosovo": "XKX", "kosovo (under unsc res. 1244)": "XKX",
    "netherlands (kingdom of the)": "NLD", "holland": "NLD",
    "saint-martin": "MAF", "st. martin (french part)": "MAF", "sint maarten": "SXM",
    "saint-barthelemy": "BLM", "falkland islands": "FLK", "falkland islands (islas malvinas)": "FLK",
    "wallis and futuna islands": "WLF", "sao tome and principe": "STP", "são tomé and príncipe": "STP",
    "bailiwick of guernsey": "GGY", "bailiwick of jersey": "JEY",
    "caribbean netherlands": "BES", "french southern and antarctic lands": "ATF",
    "british overseas territory of saint helena, ascension and tristan da cunha": "SHN",
    "netherlands, kingdom of, with respect to aruba": "ABW",
    "virgin islands (u.s.)": "VIR", "puerto rico (us)": "PRI",
    # Congo: the two are easy to confuse
    "democratic republic of the congo": "COD", "democratic republic of congo": "COD",
    "dr congo": "COD", "drc": "COD", "congo, dr": "COD", "congo, dem. rep.": "COD",
    "congo-kinshasa": "COD", "drcongo": "COD",
    "republic of congo": "COG", "congo, rep.": "COG", "congo, republic": "COG",
    "congo-brazzaville": "COG", "congo": "COG",
}

# Kosovo has a user-assigned code (XK / XKX) that pycountry does not include.
_EXTRA_NAMES: Dict[str, str] = {"XKX": "Kosovo"}

# States that no longer exist. Kept as their own nodes (SIPRI 1950-2000 arms
# data); the cleanup step labels them :HistoricalState, not :Country.
HISTORICAL_STATES: Dict[str, str] = {
    "soviet union": "Soviet Union", "ussr": "Soviet Union",
    "yugoslavia": "Yugoslavia", "czechoslovakia": "Czechoslovakia",
    "east germany": "East Germany", "east germany (gdr)": "East Germany", "gdr": "East Germany",
    "south yemen": "South Yemen", "north yemen": "North Yemen",
    "south vietnam": "South Vietnam", "netherlands antilles": "Netherlands Antilles",
    "serbia and montenegro": "Serbia and Montenegro", "zaire": "Zaire",
}

# Non-country organisations (unchanged behaviour)
_ORG_MAPPINGS: Dict[str, str] = {
    "eu": "European Union", "e.u.": "European Union", "european union": "European Union",
    "un": "United Nations", "u.n.": "United Nations",
    "nato": "NATO", "wto": "World Trade Organization",
}


# =========================================================
# TEXT CLEANING
# =========================================================

def _clean_text(text: str) -> str:
    text = text.strip().replace("’", "'").replace("‘", "'").replace("`", "'")
    text = re.sub(r"\s+", " ", text)
    return text.rstrip("*").strip()


def _alias_key(text: str) -> str:
    return _clean_text(text).casefold()


def _ascii_key(text: str) -> str:
    """Casefold + strip accents + drop punctuation: 'Sao Tomé' == 'sao tome'."""
    t = unicodedata.normalize("NFKD", _alias_key(text))
    t = "".join(ch for ch in t if not unicodedata.combining(ch))
    return re.sub(r"[^\w\s]", "", t).strip()


def _normalize_key(text: str) -> str:
    text = text.casefold()
    return re.sub(r"[^\w\s]", "", text)


# Source formatting we strip before looking a name up:
#   World Bank:  "Egypt, Arab Rep."  "Yemen, Rep."  "Somalia, Fed. Rep."  "Gambia, The"
#   UN / EM-DAT: "Iran (Islamic Republic Of)"  "Netherlands (Kingdom Of The)"
#   WTO:         "Saudi Arabia, Kingdom Of"  "Kuwait, The State Of"
_SUFFIX_PATTERNS = [
    re.compile(r"\s*\(.*\)\s*$"),
    re.compile(r",\s*(the\s+)?(kingdom|state|republic|sultanate|emirate|principality|commonwealth|federation)\s+of(\s+the)?\s*$", re.I),
    re.compile(r",\s*(arab|islamic|fed\.?|dem\.?|people'?s)?\s*(rep\.?|republic)\s*$", re.I),
    re.compile(r",\s*the\s*$", re.I),
    re.compile(r",\s*(rb|sar.*|china|pdr)$", re.I),
]


def _variants(name: str) -> list[str]:
    out = [name]
    for pat in _SUFFIX_PATTERNS:
        v = pat.sub("", name).strip()
        if v and v not in out:
            out.append(v)
    if name.lower().startswith("the "):
        out.append(name[4:])
    if name.lower().startswith("st. "):
        out.append("Saint " + name[4:])
    return out


# =========================================================
# ISO LOOKUP
# =========================================================

def _iso_name(alpha3: str) -> Optional[str]:
    if alpha3 in _EXTRA_NAMES:
        return _EXTRA_NAMES[alpha3]
    c = pycountry.countries.get(alpha_3=alpha3)
    return c.name if c else None


@lru_cache(maxsize=8192)
def canonical_country(name: str) -> Optional[str]:
    """
    Canonical (ISO short) name for a country spelling, or None if it is not
    a current country. Historical states return None here (see
    historical_state()).
    """
    if not name or not name.strip():
        return None
    cleaned = _clean_text(name)
    for v in _variants(cleaned):
        for key in (_alias_key(v), _ascii_key(v)):
            if key in _COUNTRY_ALIASES:
                return _iso_name(_COUNTRY_ALIASES[key])
    for v in _variants(cleaned):
        # Two-letter / three-letter codes are only trusted in upper case:
        # "Car" (an OFAC program) must not become the Central African Republic.
        if len(v) <= 3 and not v.isupper():
            continue
        try:
            return pycountry.countries.lookup(v).name
        except LookupError:
            continue
    return None


def historical_state(name: str) -> Optional[str]:
    if not name:
        return None
    return HISTORICAL_STATES.get(_alias_key(name)) or HISTORICAL_STATES.get(_ascii_key(name))


def iso3_for(name: str) -> Optional[str]:
    canon = canonical_country(name)
    if not canon:
        return None
    if canon in _EXTRA_NAMES.values():
        return next(k for k, v in _EXTRA_NAMES.items() if v == canon)
    c = pycountry.countries.get(name=canon)
    return c.alpha_3 if c else None


# =========================================================
# MAIN NORMALIZATION FUNCTION
# =========================================================

def normalize_entity(
    name: Optional[str],
    entity_type: str = "country"
) -> Optional[str]:
    """
    Normalize an entity name.

    country:      canonical ISO short name when the name is a country;
                  historical state name for dissolved states; otherwise the
                  cleaned title-cased text (the rebuild cleanup step labels
                  such nodes :NonCountry so they never count as countries).
    organization: custom org mappings, else title case.
    """
    if name is None:
        return None
    cleaned = _clean_text(str(name))
    if not cleaned:
        return cleaned

    key = _normalize_key(cleaned)
    if key in _ORG_MAPPINGS:
        return _ORG_MAPPINGS[key]

    if entity_type == "country":
        canon = canonical_country(cleaned)
        if canon:
            return canon
        hist = historical_state(cleaned)
        if hist:
            return hist

    return cleaned.title()


def normalize_entities(
    names: list[str],
    entity_type: str = "country"
) -> list[str]:
    return [
        normalize_entity(name, entity_type)
        for name in names
        if name is not None
    ]
