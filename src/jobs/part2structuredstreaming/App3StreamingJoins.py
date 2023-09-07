from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Streaming Joins") \
    .master("local[2]") \
    .getOrCreate()

config = config_loader("/Users/sagarl/projects/spark-streaming-pyspark/src/config.json")
dataPath = config["dataPath"]

guitarists_df = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{dataPath}/guitarPlayers")

guitars_df = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{dataPath}/guitars")

bands_df = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .load(f"{dataPath}/bands")

guitaristsBandsDF = guitarists_df.join(bands_df, guitarists_df.band == bands_df.id, "inner")
bandsSchema = bands_df.schema
guitaristsSchema = guitarists_df.schema

# guitaristsBandsDF.show()
# print(bandsSchema)


def join_streamed_with_static():

    # restricted joins:
    # - stream joining with static: RIGHT outer join/full outer join/right_semi not permitted
    # - static joining with streaming: LEFT outer join/full/left_semi not permitted

    streamed_bands = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12345) \
        .load()

    streamed_bands_df = streamed_bands \
        .select(from_json(col("value"), bandsSchema).alias("band")) \
        .select(col("band.id").alias("id"),
                col("band.name").alias("name"),
                col("band.hometown").alias("hometown"),
                col("band.year").alias("year"))

    guitarists_bands_join_condition = guitarists_df.band == streamed_bands_df.id

    guitarists_bands_df = guitarists_df.join(streamed_bands_df, guitarists_bands_join_condition, "inner")

    guitarists_bands_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()


def join_stream_with_stream():

    # - inner joins are supported
    # - left/right outer joins ARE supported, but MUST have watermarks
    # - full outer joins are NOT supported

    streamed_bands = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12345) \
        .load()

    streamed_bands_df = streamed_bands \
        .select(from_json(col("value"), bandsSchema).alias("band")) \
        .select(
            col("band.id").alias("id"),
            col("band.name").alias("name"),
            col("band.hometown").alias("hometown"),
            col("band.year").alias("year")
        )

    streamed_guitarists = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 12346) \
        .load()

    streamed_guitarists_df = streamed_guitarists \
        .select(from_json(col("value"), guitaristsSchema).alias("guitarist")) \
        .select(
            col("guitarist.id").alias("id"),
            col("guitarist.name").alias("name"),
            col("guitarist.guitars").alias("guitars"),
            col("guitarist.band").alias("band"),
        )

    guitarists_bands_join_condition = streamed_guitarists_df.band == streamed_bands_df.id

    guitarists_bands_df = streamed_guitarists_df.join(streamed_bands_df, guitarists_bands_join_condition, "inner")

    guitarists_bands_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    # joinStreamedWithStatic()
    join_stream_with_stream()
