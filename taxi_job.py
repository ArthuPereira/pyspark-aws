import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


bucket = sys.argv[1]

spark = (
    SparkSession.builder
    .appName("taxi-emr")
    .getOrCreate()
)

trips = spark.read.parquet(f"s3://{bucket}/raw/trips/")

zones = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"s3://{bucket}/raw/zones/")
)

print("Linhas brutas:", trips.count())

limpo = (
    trips
    .filter((F.col("trip_distance") > 0) & (F.col("fare_amount") > 0))
    .withColumn(
        "minutos", 
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
    )
    .filter((F.col("minutos") > 0) & (F.col("minutos") < 240))
    .withColumn("ano", F.year("tpep_pickup_datetime"))
    .withColumn("mes", F.month("tpep_pickup_datetime"))
    .filter(F.col("ano") == 2024)
    .join(
        F.broadcast(zones), 
        F.col("PULocationID") == F.col("LocationID")
    )
    .withColumnRenamed("Borough", "bairro")
    .withColumnRenamed("Zone", "zona")
    .drop("LocationID", "service_zone")
)

(
    limpo.write
    .mode("overwrite")
    .partitionBy("mes")
    .parquet(f"s3://{bucket}/curated/trips/")
)

resumo = (
    limpo
    .groupBy("mes", "bairro")
    .agg(
        F.count("*").alias("corridas"),
        F.round(F.sum("total_amount")).alias("receita")
    )
    .orderBy("mes", F.desc("receita"))
)

(
    resumo.write
    .mode("overwrite")
    .option("header", True)
    .csv(f"s3://{bucket}/curated/resumo/")
)

resumo.show(30, truncate=False)
spark.stop()
