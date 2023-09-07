from pyspark.sql import SparkSession

from src.jobs.common.package import cars_schema
from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Integrating JDBC") \
    .config("spark.jars", "/Users/sagarl/dependencies/pyspark/postgresql-42.6.0.jar") \
    .master("local[2]") \
    .getOrCreate()

config = config_loader("/Users/sagarl/projects/spark-streaming-pyspark/src/config.json")
dataPath = config["dataPath"]
driver = config["driver"]
url = config["url"]
user = config["user"]
password = config["password"]

carsDF = spark.readStream \
    .format("json") \
    .schema(cars_schema) \
    .load(f"{dataPath}/cars")


def foreach_batch_function(df, epoch_id):
    return df.write \
        .format("jdbc") \
        .option("driver", driver) \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("dbtable", "public.cars") \
        .save()


def write_stream_to_postgres():
    carsDF.writeStream \
        .foreachBatch(foreach_batch_function) \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    write_stream_to_postgres()
