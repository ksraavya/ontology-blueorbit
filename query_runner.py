import sys
sys.path.insert(0, '.')

from dotenv import load_dotenv
load_dotenv()

from common.db import Neo4jConnection

def run_query(conn, title, query, params=None):
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70)
    try:
        rows = conn.run_query(query, params or {})
        if not rows:
            print("  No results found.")
            return
        for i, row in enumerate(rows, 1):
            print(f"  [{i}]", end=" ")
            for k, v in row.items():
                if isinstance(v, float):
                    print(f"{k}: {round(v, 4)}", end="  ")
                else:
                    print(f"{k}: {v}", end="  ")
            print()
    except Exception as e:
        print(f"  ERROR: {e}")


def section(title):
    print("\n" + "#"*70)
    print(f"#  {title}")
    print("#"*70)


def main():
    conn = Neo4jConnection()

    # ──────────────────────────────────────────────────────────────────────────
    section("1. DATABASE INVENTORY")
    # ──────────────────────────────────────────────────────────────────────────

    run_query(conn, "Node Labels and Counts",
        """
        MATCH (n)
        RETURN labels(n) AS label, count(n) AS count
        ORDER BY count DESC
        """
    )

    run_query(conn, "Relationship Types and Counts",
        """
        MATCH ()-[r]->()
        RETURN type(r) AS relationship, count(r) AS count
        ORDER BY count DESC
        """
    )

    run_query(conn, "Country Nodes with Climate Scores",
        """
        MATCH (c:Country)
        WHERE c.climate_risk_score IS NOT NULL
        RETURN count(c) AS countries_with_climate_scores
        """
    )

    # ──────────────────────────────────────────────────────────────────────────
    section("2. INDIA — DISASTER STATISTICS 2018-2024")
    # ──────────────────────────────────────────────────────────────────────────

    run_query(conn, "Total Deaths in India by Natural Disaster (2018-2024)",
        """
        MATCH (c:Country {name: "India"})-[r:EXPERIENCED]->(e:ClimateEvent)
        WHERE e.year >= 2018 AND e.year <= 2024
        RETURN
            r.disaster_type                     AS disaster_type,
            count(e)                            AS event_count,
            toInteger(sum(r.deaths))            AS total_deaths,
            toInteger(max(r.deaths))            AS max_deaths_single_event,
            round(avg(r.deaths), 1)             AS avg_deaths_per_event
        ORDER BY total_deaths DESC
        """
    )

    run_query(conn, "Year-by-Year Death Toll in India (2018-2024)",
        """
        MATCH (c:Country {name: "India"})-[r:EXPERIENCED]->(e:ClimateEvent)
        WHERE e.year >= 2018 AND e.year <= 2024
        RETURN
            e.year                              AS year,
            count(e)                            AS total_events,
            toInteger(sum(r.deaths))            AS total_deaths,
            collect(DISTINCT r.disaster_type)   AS disaster_types
        ORDER BY e.year ASC
        """
    )

    run_query(conn, "Total Economic Damage in India (2018-2024)",
        """
        MATCH (e:ClimateEvent)-[r:CAUSED_DAMAGE]->(c:Country {name: "India"})
        WHERE e.year >= 2018 AND e.year <= 2024
        RETURN
            e.year                              AS year,
            round(sum(r.value) / 1000000, 2)    AS total_damage_million_usd,
            count(e)                            AS events_causing_damage
        ORDER BY e.year ASC
        """
    )

    run_query(conn, "People Affected in India (2018-2024)",
        """
        MATCH (c:Country {name: "India"})-[r:AFFECTED_BY]->(e:ClimateEvent)
        WHERE e.year >= 2018 AND e.year <= 2024
        RETURN
            e.year                              AS year,
            toInteger(sum(r.value))             AS total_affected,
            r.disaster_type                     AS disaster_type
        ORDER BY total_affected DESC
        LIMIT 10
        """
    )

    run_query(conn, "India Composite Climate Risk Score",
        """
        MATCH (c:Country {name: "India"})
        RETURN
            c.name                                          AS country,
            round(c.climate_risk_score, 4)                 AS climate_risk_score,
            round(c.disaster_frequency_score, 4)           AS disaster_frequency,
            round(c.climate_damage_score, 4)               AS damage_score,
            round(c.climate_deaths_score, 4)               AS deaths_score,
            round(coalesce(c.supply_chain_risk_score, 0), 4) AS supply_chain_risk
        """
    )

    # ──────────────────────────────────────────────────────────────────────────
    section("3. INDIA — IMPACT AND PROPAGATION EFFECTS")
    # ──────────────────────────────────────────────────────────────────────────

    run_query(conn, "What Hazard Risks is India High/Medium Risk For?",
        """
        MATCH (c:Country {name: "India"})-[r:IS_HIGH_RISK_FOR]->(e:ClimateEvent)
        RETURN
            e.name                  AS risk_category,
            r.hazard_type           AS hazard_type,
            r.risk_level            AS risk_level,
            round(r.value, 4)       AS risk_score
        ORDER BY r.value DESC
        """
    )

    run_query(conn, "India CO2 Emissions Profile (Latest Available)",
        """
        MATCH (c:Country {name: "India"})-[r:EMITS]->(e:EmissionsProfile)
        WITH r ORDER BY r.year DESC
        RETURN
            r.year                          AS year,
            round(r.co2_pc, 4)              AS co2_per_capita_tons,
            round(r.forest_pct, 2)          AS forest_coverage_pct,
            round(r.normalized_weight, 4)   AS emissions_score
        LIMIT 5
        """
    )

    run_query(conn, "India Temperature Stress",
        """
        MATCH (c:Country {name: "India"})-[r:HAS_RESOURCE_STRESS]->(w:LiveWeather)
        RETURN
            c.name                  AS country,
            round(r.nasa_temp, 2)   AS mean_temp_celsius,
            round(r.warming, 4)     AS warming_stress_score
        """
    )

    run_query(conn, "Countries India Could Disrupt (Supply Chain Risk)",
        """
        MATCH (c:Country {name: "India"})-[r:DISRUPTS_SUPPLY_CHAIN]->(target:Country)
        RETURN
            target.name             AS disrupted_country,
            round(r.value, 4)       AS disruption_score,
            r.source_event          AS trigger_event,
            r.disaster_type         AS disaster_type,
            r.confidence            AS confidence
        ORDER BY r.value DESC
        """
    )

    run_query(conn, "Countries India Increases Conflict Risk For",
        """
        MATCH (c:Country {name: "India"})-[r:INCREASES_CONFLICT_RISK]->(target:Country)
        RETURN
            target.name             AS at_risk_country,
            round(r.value, 4)       AS conflict_risk_score,
            r.source_event          AS trigger_event
        ORDER BY r.value DESC
        """
    )

    # ──────────────────────────────────────────────────────────────────────────
    section("4. JAPAN — HIGH RISK CLIMATE EVENTS")
    # ──────────────────────────────────────────────────────────────────────────

    run_query(conn, "Japan is High Risk For Which Climate Events?",
        """
        MATCH (c:Country {name: "Japan"})-[r:IS_HIGH_RISK_FOR]->(e:ClimateEvent)
        RETURN
            e.name                  AS climate_event,
            r.hazard_type           AS hazard_type,
            r.risk_level            AS risk_level,
            round(r.value, 4)       AS risk_score
        ORDER BY r.value DESC
        """
    )

    run_query(conn, "Japan Disaster History (All Time Top Events by Deaths)",
        """
        MATCH (c:Country {name: "Japan"})-[r:EXPERIENCED]->(e:ClimateEvent)
        WHERE r.deaths > 0
        RETURN
            e.name                      AS event,
            e.year                      AS year,
            r.disaster_type             AS disaster_type,
            toInteger(r.deaths)         AS deaths,
            round(r.risk, 4)            AS risk_score
        ORDER BY r.deaths DESC
        LIMIT 10
        """
    )

    run_query(conn, "Japan Earthquake Risk Summary (USGS Data)",
        """
        MATCH (c:Country {name: "Japan"})-[r:EXPERIENCED]->(e:ClimateEvent)
        WHERE r.disaster_type = "Earthquake"
        RETURN
            e.year                          AS year,
            toInteger(r.value)              AS quake_count,
            round(r.max_magnitude, 2)       AS max_magnitude,
            round(r.risk, 4)                AS risk_score
        ORDER BY e.year DESC
        """
    )

    run_query(conn, "Japan Climate Risk Scores",
        """
        MATCH (c:Country {name: "Japan"})
        RETURN
            c.name                                          AS country,
            round(c.climate_risk_score, 4)                 AS climate_risk,
            round(c.disaster_frequency_score, 4)           AS frequency,
            round(c.climate_damage_score, 4)               AS damage,
            round(c.climate_deaths_score, 4)               AS deaths_score
        """
    )

    # ──────────────────────────────────────────────────────────────────────────
    section("5. NEPAL — EARTHQUAKES AND NEIGHBOURING COUNTRY IMPACT")
    # ──────────────────────────────────────────────────────────────────────────

    run_query(conn, "How Earthquakes Affected Nepal (Full History)",
        """
        MATCH (c:Country {name: "Nepal"})-[r:EXPERIENCED]->(e:ClimateEvent)
        WHERE r.disaster_type = "Earthquake"
        RETURN
            e.year                          AS year,
            e.name                          AS event,
            toInteger(r.deaths)             AS deaths,
            round(r.risk, 4)                AS risk_score,
            round(r.max_magnitude, 2)       AS max_magnitude
        ORDER BY r.deaths DESC
        """
    )

    run_query(conn, "Nepal Fatalities from All Disasters",
        """
        MATCH (e:ClimateEvent)-[r:RESULTED_IN_FATALITIES]->(c:Country {name: "Nepal"})
        RETURN
            e.name                      AS event,
            e.year                      AS year,
            toInteger(r.value)          AS deaths,
            round(r.normalized_weight, 4) AS severity_score
        ORDER BY r.value DESC
        LIMIT 10
        """
    )

    run_query(conn, "Nepal Conflict Risk Impact on Neighbouring Countries",
        """
        MATCH (c:Country {name: "Nepal"})-[r:INCREASES_CONFLICT_RISK]->(target:Country)
        RETURN
            c.name                  AS source_country,
            target.name             AS neighbouring_country,
            round(r.value, 4)       AS conflict_risk_score,
            r.source_event          AS trigger_event
        ORDER BY r.value DESC
        """
    )

    run_query(conn, "Countries Sharing High Earthquake Risk with Nepal (Region)",
        """
        MATCH (c:Country)-[r:IS_HIGH_RISK_FOR]->(e:ClimateEvent)
        WHERE r.hazard_type = "Earthquake"
          AND r.risk_level = "High"
          AND c.name IN ["Nepal", "India", "China", "Bangladesh",
                         "Pakistan", "Afghanistan", "Myanmar"]
        RETURN
            c.name                  AS country,
            r.risk_level            AS risk_level,
            round(r.value, 4)       AS risk_score
        ORDER BY r.value DESC
        """
    )

    # ──────────────────────────────────────────────────────────────────────────
    section("6. COUNTRY → EVENT QUERIES")
    # ──────────────────────────────────────────────────────────────────────────

    run_query(conn, "Top 10 Deadliest Climate Events Globally (All Time)",
        """
        MATCH (c:Country)-[r:EXPERIENCED]->(e:ClimateEvent)
        WHERE r.deaths > 0
        RETURN
            c.name                      AS country,
            e.name                      AS event,
            e.year                      AS year,
            r.disaster_type             AS disaster_type,
            toInteger(r.deaths)         AS deaths,
            round(r.risk, 4)            AS risk_score
        ORDER BY r.deaths DESC
        LIMIT 10
        """
    )

    run_query(conn, "Countries with Most Climate Events (All Time)",
        """
        MATCH (c:Country)-[r:EXPERIENCED]->(e:ClimateEvent)
        RETURN
            c.name                          AS country,
            count(e)                        AS total_events,
            toInteger(sum(r.deaths))        AS total_deaths,
            collect(DISTINCT r.disaster_type) AS disaster_types
        ORDER BY total_events DESC
        LIMIT 15
        """
    )

    run_query(conn, "Flood Events — Top 10 Most Affected Countries",
        """
        MATCH (c:Country)-[r:EXPERIENCED]->(e:ClimateEvent)
        WHERE r.disaster_type = "Flood"
        RETURN
            c.name                      AS country,
            count(e)                    AS flood_events,
            toInteger(sum(r.deaths))    AS total_deaths,
            round(avg(r.risk), 4)       AS avg_risk_score
        ORDER BY total_deaths DESC
        LIMIT 10
        """
    )

    run_query(conn, "Countries at High Risk for Multiple Hazards",
        """
        MATCH (c:Country)-[r:IS_HIGH_RISK_FOR]->(e:ClimateEvent)
        WHERE r.risk_level = "High"
        RETURN
            c.name                              AS country,
            count(DISTINCT r.hazard_type)       AS high_risk_hazard_count,
            collect(DISTINCT r.hazard_type)     AS hazard_types,
            round(avg(r.value), 4)              AS avg_risk_score
        ORDER BY high_risk_hazard_count DESC, avg_risk_score DESC
        LIMIT 15
        """
    )

    run_query(conn, "Drought Events — Countries Most Affected",
        """
        MATCH (c:Country)-[r:EXPERIENCED]->(e:ClimateEvent)
        WHERE r.disaster_type = "Drought"
        RETURN
            c.name                      AS country,
            count(e)                    AS drought_events,
            toInteger(sum(r.deaths))    AS total_deaths
        ORDER BY drought_events DESC
        LIMIT 10
        """
    )

    run_query(conn, "CO2 Emissions — Top 15 Emitters (Latest Year)",
        """
        MATCH (c:Country)-[r:EMITS]->(e:EmissionsProfile)
        WHERE r.co2_pc IS NOT NULL
        WITH c, r ORDER BY r.year DESC
        WITH c, collect(r)[0] AS latest
        RETURN
            c.name                          AS country,
            latest.year                     AS year,
            round(latest.co2_pc, 3)         AS co2_per_capita_tons,
            round(latest.forest_pct, 2)     AS forest_coverage_pct
        ORDER BY latest.co2_pc DESC
        LIMIT 15
        """
    )

    run_query(conn, "Warmest Countries by NASA Temperature Data",
        """
        MATCH (c:Country)-[r:HAS_RESOURCE_STRESS]->(w:LiveWeather)
        RETURN
            c.name                  AS country,
            round(r.nasa_temp, 2)   AS mean_temp_celsius,
            round(r.warming, 4)     AS warming_stress_score
        ORDER BY r.nasa_temp DESC
        LIMIT 15
        """
    )

    # ──────────────────────────────────────────────────────────────────────────
    section("7. COUNTRY → COUNTRY QUERIES")
    # ──────────────────────────────────────────────────────────────────────────

    run_query(conn, "Top Supply Chain Disruption Pairs (Climate Triggered)",
        """
        MATCH (a:Country)-[r:DISRUPTS_SUPPLY_CHAIN]->(b:Country)
        WHERE r.source_event IS NOT NULL
        RETURN
            a.name                  AS source_country,
            b.name                  AS disrupted_country,
            round(r.value, 4)       AS disruption_score,
            r.source_event          AS trigger_event,
            r.disaster_type         AS disaster_type,
            r.confidence            AS confidence
        ORDER BY r.value DESC
        LIMIT 15
        """
    )

    run_query(conn, "Top Conflict Risk Pairs (Climate Triggered)",
        """
        MATCH (a:Country)-[r:INCREASES_CONFLICT_RISK]->(b:Country)
        WHERE r.source_event IS NOT NULL
        RETURN
            a.name                  AS source_country,
            b.name                  AS at_risk_country,
            round(r.value, 4)       AS conflict_risk_score,
            r.source_event          AS trigger_event,
            r.disaster_type         AS disaster_type
        ORDER BY r.value DESC
        LIMIT 15
        """
    )

    run_query(conn, "Countries Dependent on Deforested Nations (DEPENDS_ON_RESOURCE)",
        """
        MATCH (a:Country)-[r:DEPENDS_ON_RESOURCE]->(b:Country)
        RETURN
            a.name                  AS dependent_country,
            b.name                  AS resource_country,
            round(r.value, 4)       AS dependency_score,
            r.year                  AS year
        ORDER BY r.value DESC
        LIMIT 15
        """
    )

    run_query(conn, "Hazard Risk Pairs — Countries Sharing Climate Zone Risk",
        """
        MATCH (a:Country)-[r1:IS_HIGH_RISK_FOR]->(e1:ClimateEvent)
        MATCH (b:Country)-[r2:IS_HIGH_RISK_FOR]->(e2:ClimateEvent)
        WHERE r1.hazard_type = r2.hazard_type
          AND r1.risk_level = "High"
          AND r2.risk_level = "High"
          AND a.name < b.name
        RETURN
            a.name                  AS country_a,
            b.name                  AS country_b,
            r1.hazard_type          AS shared_hazard,
            round(r1.value, 3)      AS country_a_risk,
            round(r2.value, 3)      AS country_b_risk
        ORDER BY shared_hazard, country_a_risk DESC
        LIMIT 20
        """
    )

    # ──────────────────────────────────────────────────────────────────────────
    section("8. MATHEMATICAL FORMULA VERIFICATION")
    # ──────────────────────────────────────────────────────────────────────────

    run_query(conn, "Verify: climate_risk = 0.3*frequency + 0.4*damage + 0.3*deaths",
        """
        MATCH (c:Country)
        WHERE c.climate_risk_score IS NOT NULL
          AND c.disaster_frequency_score IS NOT NULL
          AND c.climate_damage_score IS NOT NULL
          AND c.climate_deaths_score IS NOT NULL
        WITH c,
             round(
                 0.3 * c.disaster_frequency_score +
                 0.4 * c.climate_damage_score +
                 0.3 * c.climate_deaths_score,
             4) AS expected_score,
             round(c.climate_risk_score, 4) AS stored_score
        WHERE abs(expected_score - stored_score) > 0.001
        RETURN count(*) AS countries_with_score_mismatch
        """
    )

    run_query(conn, "Score Distribution Statistics (Min / Max / Avg)",
        """
        MATCH (c:Country)
        WHERE c.climate_risk_score IS NOT NULL
        RETURN
            round(min(c.climate_risk_score), 4)             AS min_risk,
            round(max(c.climate_risk_score), 4)             AS max_risk,
            round(avg(c.climate_risk_score), 4)             AS avg_risk,
            round(stDev(c.climate_risk_score), 4)           AS std_dev,
            count(c)                                        AS total_countries
        """
    )

    run_query(conn, "Verify All Scores Are Clamped to [0.0, 1.0]",
        """
        MATCH (c:Country)
        WHERE c.climate_risk_score IS NOT NULL
        WITH
            sum(CASE WHEN c.climate_risk_score < 0 OR c.climate_risk_score > 1
                THEN 1 ELSE 0 END) AS risk_out_of_range,
            sum(CASE WHEN c.disaster_frequency_score < 0 OR c.disaster_frequency_score > 1
                THEN 1 ELSE 0 END) AS freq_out_of_range,
            sum(CASE WHEN c.climate_damage_score < 0 OR c.climate_damage_score > 1
                THEN 1 ELSE 0 END) AS damage_out_of_range,
            sum(CASE WHEN c.climate_deaths_score < 0 OR c.climate_deaths_score > 1
                THEN 1 ELSE 0 END) AS deaths_out_of_range
        RETURN
            risk_out_of_range,
            freq_out_of_range,
            damage_out_of_range,
            deaths_out_of_range
        """
    )

    run_query(conn, "Top 10 Countries by Overall Climate Risk Score",
        """
        MATCH (c:Country)
        WHERE c.climate_risk_score IS NOT NULL
        RETURN
            c.name                                          AS country,
            round(c.climate_risk_score, 4)                 AS climate_risk,
            round(c.disaster_frequency_score, 4)           AS frequency,
            round(c.climate_damage_score, 4)               AS damage,
            round(c.climate_deaths_score, 4)               AS deaths_score
        ORDER BY c.climate_risk_score DESC
        LIMIT 10
        """
    )

    run_query(conn, "Verify Normalized Weights Are in [0,1] for EXPERIENCED Rels",
        """
        MATCH ()-[r:EXPERIENCED]->()
        WITH
            min(r.normalized_weight)    AS min_nw,
            max(r.normalized_weight)    AS max_nw,
            avg(r.normalized_weight)    AS avg_nw,
            count(r)                    AS total_rels,
            sum(CASE WHEN r.normalized_weight < 0 OR r.normalized_weight > 1
                THEN 1 ELSE 0 END)      AS out_of_range_count
        RETURN
            round(min_nw, 4)            AS min_normalized_weight,
            round(max_nw, 4)            AS max_normalized_weight,
            round(avg_nw, 4)            AS avg_normalized_weight,
            total_rels,
            out_of_range_count
        """
    )

    # ──────────────────────────────────────────────────────────────────────────
    section("9. PROJECT WINNER QUERY")
    # ──────────────────────────────────────────────────────────────────────────

    run_query(conn,
        "Layer 1 + Layer 2: India Severe Events → Impact on Trade Partners",
        """
        MATCH (c:Country {name: "India"})-[r1:EXPERIENCED]->(e:ClimateEvent)
        WHERE r1.deaths >= 100
        WITH c, e, r1
        OPTIONAL MATCH (c)-[r2:DISRUPTS_SUPPLY_CHAIN]->(partner:Country)
        WHERE r2.source_event = e.name
        RETURN
            c.name                          AS affected_country,
            e.name                          AS event,
            e.year                          AS year,
            toInteger(r1.deaths)            AS deaths,
            r1.disaster_type                AS disaster_type,
            coalesce(partner.name, "none")  AS disrupted_partner,
            round(coalesce(r2.value, 0), 4) AS disruption_score,
            coalesce(r2.confidence, 0)      AS confidence
        ORDER BY r1.deaths DESC, r2.value DESC
        LIMIT 20
        """
    )

    conn.close()
    print("\n" + "="*70)
    print("  Query runner complete.")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()