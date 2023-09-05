from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.jobs.common.package import carsSchema
from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Integrating Cassandra") \
    .master("local[2]") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

config = config_loader("/Users/sagarl/projects/spark-streaming-pyspark/src/config.json")
dataPath = config["dataPath"]

carsDF = spark.readStream \
    .format("json") \
    .schema(carsSchema) \
    .load(f"{dataPath}/cars")


def for_each_batch(df, epoch_id):
    df.write \
        .select(col("Name"), col("Horsepower")) \
        .cassandraFormat("cars", "public") \
        .save()


def writeStreamToCassandraInBatches(df):
    df.writeStream \
        .foreachBatch(for_each_batch) \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    writeStreamToCassandraInBatches(carsDF)
