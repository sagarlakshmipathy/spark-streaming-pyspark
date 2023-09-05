from pyspark.sql.types import *

carsSchema = StructType([
    StructField("Name", StringType()),
    StructField("Miles_per_Gallon", DoubleType()),
    StructField("Cylinders", LongType()),
    StructField("Displacement", DoubleType()),
    StructField("Horsepower", LongType()),
    StructField("Weight_in_lbs", LongType()),
    StructField("Acceleration", DoubleType()),
    StructField("Year", StringType()),
    StructField("Origin", StringType())
])

stocksSchema = StructType([
    StructField("company", StringType()),
    StructField("date", DateType()),
    StructField("value", DoubleType())
])
