from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Streaming Joins") \
    .master("local[2]") \
    .getOrCreate()

config = config_loader("/Users/sagarl/projects/spark-streaming-pyspark/src/config.json")
dataPath = config["dataPath"]

guitaristsDF = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{dataPath}/guitarPlayers")

guitarsDF = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{dataPath}/guitars")

bandsDF = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{dataPath}/bands")

guitaristsBandsDF = guitaristsDF.join(bandsDF, guitaristsDF.band == bandsDF.id, "inner")
bandsSchema = bandsDF.schema
guitaristsSchema = guitaristsDF.schema

# guitaristsBandsDF.show()
# print(bandsSchema)


def joinStreamedWithStatic():

    # restricted joins:
    # - stream joining with static: RIGHT outer join/full outer join/right_semi not permitted
    # - static joining with streaming: LEFT outer join/full/left_semi not permitted

    streamedBands = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12345) \
        .load()

    streamedBandsDF = streamedBands \
        .select(from_json(col("value"), bandsSchema).alias("band")) \
        .select(col("band.id").alias("id"),
                col("band.name").alias("name"),
                col("band.hometown").alias("hometown"),
                col("band.year").alias("year"))

    guitaristsBandsJoinCondition = guitaristsDF.band == streamedBandsDF.id

    guitaristsBandsDF = guitaristsDF.join(streamedBandsDF, guitaristsBandsJoinCondition, "inner")

    guitaristsBandsDF.writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()


def joinStreamWithStream():

    # - inner joins are supported
    # - left/right outer joins ARE supported, but MUST have watermarks
    # - full outer joins are NOT supported

    streamedBands = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12345) \
        .load()

    streamedBandsDF = streamedBands \
        .select(from_json(col("value"), bandsSchema).alias("band")) \
        .select(
            col("band.id").alias("id"),
            col("band.name").alias("name"),
            col("band.hometown").alias("hometown"),
            col("band.year").alias("year")
        )

    streamedGuitarists = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12346) \
        .load()

    streamedGuitaristsDF = streamedGuitarists \
        .select(from_json(col("value"), guitaristsSchema).alias("guitarist")) \
        .select(
            col("guitarist.id").alias("id"),
            col("guitarist.name").alias("name"),
            col("guitarist.guitars").alias("guitars"),
            col("guitarist.band").alias("band"),
        )

    guitaristsBandsJoinCondition = streamedGuitaristsDF.band == streamedBandsDF.id

    guitaristsBandsDF = streamedGuitaristsDF.join(streamedBandsDF, guitaristsBandsJoinCondition, "inner")

    guitaristsBandsDF.writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    # joinStreamedWithStatic()
    joinStreamWithStream()
