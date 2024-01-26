from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, row_number, when, count, desc
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, TimestampType, FloatType, DoubleType



spark = SparkSession \
        .builder \
        .appName("Qeury 2-RDD API") \
        .getOrCreate()


crime_schema = StructType([
    StructField("DR_NO", StringType()),
    StructField("Date Rptd", StringType()),
    StructField("DATE OCC", StringType()),
    StructField("TIME OCC", StringType()),
    StructField("AREA", StringType()),
    StructField("AREA NAME", StringType()),
    StructField("Rpt Dist No", StringType()),
    StructField("Part 1-2", StringType()),
    StructField("Crm Cd", StringType()),
    StructField("Crm Cd Desc", StringType()),
    StructField("Mocodes", StringType()),
    StructField("Vict Age", StringType()),
    StructField("Vict Sex", StringType()),
    StructField("Vict Descent", StringType()),
    StructField("Premis Cd", StringType()),
    StructField("Premis Desc", StringType()),
    StructField("Weapon Used Cd", StringType()),
    StructField("Weapon Desc", StringType()),
 StructField("Status", StringType()),
    StructField("Status Desc", StringType()),
    StructField("Crm Cd 1", StringType()),
    StructField("Crm Cd 2", StringType()),
    StructField("Crm Cd 3", StringType()),
    StructField("Crm Cd 4", StringType()),
    StructField("LOCATION", StringType()),
    StructField("Cross Street", StringType()),
    StructField("LAT", DoubleType()),
    StructField("LON", DoubleType())
    ])
crime1_rdd = spark.read.csv("hdfs://okeanos-master:54310/maindata/2020to2023.csv", header=True, schema=crime_schema)
crime2_rdd = spark.read.csv("hdfs://okeanos-master:54310/maindata/2010to2019.csv", header=True, schema=crime_schema)
# Union the DataFrames and convert to RDD
crime_rdd = crime2_rdd.union(crime1_rdd).rdd

# Filter rows
street_rdd = crime_rdd.filter(lambda x: x["Premis Desc"] == "STREET")

# Define time intervals
def get_time_interval(row):
    if 500 <= int(row["TIME OCC"]) <= 1159:
        return "Morning"
    elif 1200 <= int(row["TIME OCC"]) <= 1659:
        return "Afternoon"
    elif 1700 <= int(row["TIME OCC"]) <= 2059:
        return "Evening"
    elif 2100 <= int(row["TIME OCC"]) or int(row["TIME OCC"]) <= 359:
        return "Night"
    else:
        return None

# Create a new RDD with Year, Month, and Time Interval
time_interval_rdd = street_rdd \
    .map(lambda x: (get_time_interval(x), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    
# Sort the result by count in descending order
sorted_rdd = time_interval_rdd.sortBy(lambda x: x[1], ascending=False)

# Collect and print the result
result = sorted_rdd.collect()
for item in result:
    print(item)
spark.stop()
