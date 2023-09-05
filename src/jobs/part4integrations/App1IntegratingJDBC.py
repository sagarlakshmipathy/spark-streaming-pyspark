from pyspark.sql import SparkSession

from src.jobs.common.package import carsSchema
from src.utils import config_loader

spark = SparkSession.builder \
    .appName("Integrating JDBC") \
    .config("spark.jars", "/Users/sagarl/projects/spark-essentials-pyspark/dependencies/postgresql-42.6.0.jar") \
    .master("local[2]") \
    .getOrCreate()

config = config_loader("/spark-streaming-pyspark/src/config.json")
dataPath = config["dataPath"]
driver = config["driver"]
url = config["url"]
user = config["user"]
password = config["password"]

carsDF = spark.readStream \
    .format("json") \
    .schema(carsSchema) \
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


def writeStreamToPostgres():
    carsDF.writeStream \
        .foreachBatch(foreach_batch_function) \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    writeStreamToPostgres()
