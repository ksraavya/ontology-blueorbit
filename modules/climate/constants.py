from __future__ import annotations
import sys
sys.path.insert(0, '.')

# -- Node labels ---------------------------------------------------------------
COUNTRY_LABEL           = "Country"
CLIMATE_EVENT_LABEL     = "ClimateEvent"
EMISSIONS_PROFILE_LABEL = "EmissionsProfile"
LIVE_WEATHER_LABEL      = "LiveWeather"

# -- Entity type ---------------------------------------------------------------
CLIMATE_ENTITY_TYPE = "country"

# -- EM-DAT --------------------------------------------------------------------
EMDAT_FILENAME = "emdat_disasters.csv"
EMDAT_YEAR_MIN = 2000
EMDAT_YEAR_MAX = 2024

DISASTER_TYPES = [
    "Flood",
    "Drought",
    "Storm",
    "Earthquake",
    "Volcanic activity",
    "Extreme temperature",
    "Landslide",
    "Wildfire",
    "Tsunami",
]

EMDAT_COLUMN_MAP = {
    "Country":                              "country",
    "Year":                                 "year",
    "Disaster Type":                        "disaster_type",
    "Total Deaths":                         "deaths",
    "Total Damage, Adjusted ('000 US$)":    "damage_thousands_usd",
    "Total Affected":                       "affected",
}

# -- World Bank ----------------------------------------------------------------
WB_API_BASE         = "https://api.worldbank.org/v2"
WB_CO2_INDICATOR    = "EN.ATM.CO2E.PC"
WB_FOREST_INDICATOR = "AG.LND.FRST.ZS"
WB_YEARS_MRV        = 10
WB_SLEEP_S          = 0.2

# -- REST Countries ------------------------------------------------------------
REST_COUNTRIES_BASE      = "https://restcountries.com/v3.1"
REST_COUNTRIES_SLEEP_S   = 0.05

# -- NASA POWER ----------------------------------------------------------------
NASA_POWER_BASE = "https://power.larc.nasa.gov/api/temporal/climatology/point"
NASA_PARAMETER  = "T2M"
NASA_COMMUNITY  = "RE"
NASA_SLEEP_S    = 0.2

# -- USGS ----------------------------------------------------------------------
USGS_BASE          = "https://earthquake.usgs.gov/fdsnws/event/1/query"
USGS_MIN_MAGNITUDE = 5.0
USGS_LIMIT         = 20000
USGS_YEARS         = list(range(2018, 2025))
USGS_SLEEP_S       = 1.0

# -- Bounding box padding ------------------------------------------------------
BBOX_MIN_PADDING_DEG = 2.0
BBOX_MAX_PADDING_DEG = 15.0

# -- Risk classification thresholds --------------------------------------------
RISK_HIGH_THRESHOLD   = 0.66
RISK_MEDIUM_THRESHOLD = 0.33

RISK_SCORE_MAP = {
    "High":   1.0,
    "Medium": 0.5,
    "Low":    0.1,
}

# -- Composite weights ---------------------------------------------------------
EARTHQUAKE_RISK_WEIGHTS = {"count": 0.4, "magnitude": 0.6}

# -- Graph write ---------------------------------------------------------------
BATCH_SIZE = 500