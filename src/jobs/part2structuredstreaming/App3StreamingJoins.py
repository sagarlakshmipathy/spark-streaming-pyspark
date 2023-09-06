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

# guitaristsBandsDF.show()
# print(bandsSchema)


def joinStreamedWithStatic():
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

    streamedGuitaristsDF = guitaristsDF.join(streamedBandsDF, guitaristsBandsJoinCondition, "inner")

    streamedGuitaristsDF.writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    joinStreamedWithStatic()
