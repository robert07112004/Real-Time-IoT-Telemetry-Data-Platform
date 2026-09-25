import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType, ArrayType
from pyspark.sql.functions import from_json, col, from_unixtime, to_date

spark_version = pyspark.__version__
paquete_kafka = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version}"

flight_schema = StructType([
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("time_position", IntegerType(), True),
    StructField("last_contact", IntegerType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("baro_altitude", DoubleType(), True),
    StructField("on_ground", BooleanType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("true_track", DoubleType(), True),
    StructField("vertical_rate", DoubleType(), True),
    StructField("sensors", ArrayType(IntegerType()), True),
    StructField("geo_altitude", DoubleType(), True),
    StructField("squawk", StringType(), True),
    StructField("spi", BooleanType(), True),
    StructField("position_source", IntegerType(), True),
])

spark = SparkSession \
    .builder \
    .appName("Spark consuming JSONs") \
    .config("spark.jars.packages", paquete_kafka) \
    .config("spark.sql.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

print(f"Spark inicializado v{spark_version}")
print(f"Descargando conector: {paquete_kafka}")

df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:19092") \
    .option("subscribe", "flight_telemetry") \
    .option("startingOffsets", "earliest") \
    .load()

df_json = df_kafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df_parsed = df_json.select(
    from_json(col("value"), flight_schema).alias("datos")
).select("datos.*")

df_with_date = df_parsed.withColumn("date", to_date(from_unixtime(col("time_position"))))

query = df_with_date \
    .writeStream \
    .format("parquet") \
    .partitionBy("date") \
    .outputMode("append") \
    .option("path", "./datalake/bronze/flights") \
    .option("checkpointLocation", "./checkpoint") \
    .start()

try:
    print("Escuchando datos... Pulsa Ctrl+C para detener el proceso de forma segura.")
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nSeñal de apagado recibida. Cerrando el streaming de forma segura...")
    query.stop()
    print("Streaming detenido correctamente. Checkpoints a salvo.")