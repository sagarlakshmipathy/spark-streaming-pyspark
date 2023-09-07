from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.jobs.common.package import cars_schema
from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Integrating Cassandra") \
    .master("local[2]") \
    .config("spark.jars",
            "/Users/sagarl/dependencies/pyspark/spark-cassandra-connector_2.12-3.3.0.jar") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

cassandra_table = "cars"
cassandra_keyspace = "public"

config = config_loader("/Users/sagarl/projects/spark-streaming-pyspark/src/config.json")
dataPath = config["dataPath"]
checkpointPath = config["checkpointPath"]

cars_df = spark.readStream \
    .format("json") \
    .schema(cars_schema) \
    .load(f"{dataPath}/cars")


def for_each_batch(df, epoch_id):
    name_hp_df = df.select(col("Name"), col("Horsepower"))

    name_hp_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=cassandra_table, keyspace=cassandra_keyspace) \
        .mode("append") \
        .save()


def write_stream_to_cassandra_in_batches(df):
    df.writeStream \
        .foreachBatch(for_each_batch) \
        .start() \
        .awaitTermination()


# def write_stream_to_cassandra(df):
#     df.select(col("Name"), col("Horsepower")) \
#         .writeStream \
#         .format("org.apache.spark.sql.cassandra") \
#         .outputMode("Append") \
#         .option("checkpointLocation", checkpointPath) \
#         .option("table", cassandra_table) \
#         .option("keyspace", cassandra_keyspace) \
#         .start() \
#         .awaitTermination()


if __name__ == '__main__':
    write_stream_to_cassandra_in_batches(cars_df)
    # write_stream_to_cassandra(carsDF)
