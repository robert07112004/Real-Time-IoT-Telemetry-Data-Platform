from pyspark.sql import SparkSession 

spark = SparkSession \
    .builder \
    .appName("Gold Layer Processor") \
    .getOrCreate()

df_silver = spark.read \
    .format("parquet") \
    .load("./datalake/silver/flights")

df_silver.createOrReplaceTempView("silver_flight_telemetry")

clean_query_active_positions = """
    WITH RankedFlights AS (
        SELECT
            icao24,
            callsign,
            origin_country,
            longitude,
            latitude,
            velocity,
            baro_altitude,
            on_ground,
            CASE
                WHEN vertical_rate > 0 THEN 'Ascendiendo'
                WHEN vertical_rate < 0 THEN 'Descendiendo'
                ELSE 'Crucero'
            END AS flight_phase,
            ROW_NUMBER() OVER(PARTITION BY icao24 ORDER BY last_contact DESC) as rn
        FROM silver_flight_telemetry
    )
    SELECT 
        icao24, callsign, origin_country, longitude, latitude, 
        velocity, baro_altitude, on_ground, flight_phase
    FROM RankedFlights
    WHERE rn = 1;
"""

df_gold_active_positions = spark.sql(clean_query_active_positions)
df_gold_active_positions.createOrReplaceTempView("active_positions_flight_telemetry")

clean_query_traffic_summary = """
    SELECT 
        origin_country,
        COUNT(icao24) AS total_flights,
        SUM(CASE WHEN on_ground = true THEN 1 ELSE 0 END) AS grounded_flights,
        SUM(CASE WHEN on_ground = false THEN 1 ELSE 0 END) AS airborne_flights,
        AVG(velocity) AS average_speed,
        MAX(velocity) AS max_speed,
        AVG(baro_altitude) AS average_barometric_altitude
    FROM active_positions_flight_telemetry
    GROUP BY origin_country;
"""

df_gold_traffic_summary = spark.sql(clean_query_traffic_summary)

df_gold_active_positions.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("./datalake/gold/active_positions")

df_gold_traffic_summary.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("./datalake/gold/traffic_summary")

print("Capa Gold procesada con exito. Tablas listas para Power BI.")