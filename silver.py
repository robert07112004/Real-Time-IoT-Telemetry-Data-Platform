from pyspark.sql import SparkSession 

spark = SparkSession \
    .builder \
    .appName("Silver Layer Processor") \
    .getOrCreate()

df_bronze = spark.read \
    .format("parquet") \
    .load("./datalake/bronze/flights")

df_bronze.createOrReplaceTempView("bronze_flight_telemetry")

clean_query = """
    SELECT
        icao24, 
        callsign,
        date, 
        origin_country, 
        longitude, 
        latitude, 
        velocity, 
        true_track, 
        baro_altitude,
        on_ground,
        vertical_rate,
        last_contact
    FROM bronze_flight_telemetry
    WHERE longitude IS NOT NULL AND latitude IS NOT NULL AND last_contact IS NOT NULL;
"""

df_silver = spark.sql(clean_query)

df_silver.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("date") \
    .save("./datalake/silver/flights")

print("Datos procesados y guardados en la capa Silver con exito.")