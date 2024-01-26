from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, to_date

spark = SparkSession \
    .builder \
    .appName("Exercise 1: Dataframe combination") \
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
crime1_df = spark.read.csv("hdfs://okeanos-master:54310/maindata/2020to2023.csv", header=True, schema=crime_schema)
crime2_df = spark.read.csv("hdfs://okeanos-master:54310/maindata/2010to2019.csv", header=True, schema=crime_schema)
crime_df = crime2_df.union(crime1_df)
crime_df = crime_df.withColumn("Date Rptd",to_date("Date Rptd", "MM/dd/yyyy hh:mm:ss a"))
crime_df = crime_df.withColumn("Date OCC",to_date("Date OCC", "MM/dd/yyyy hh:mm:ss a"))
crime_df = crime_df.withColumn("Vict Age", crime_df["Vict Age"].cast(IntegerType()))
print("combined dataframe rows:", crime_df.count())
crime_df.printSchema()

local_directory="/home/user/code"

try:
    crime_df.write.parquet(f"{local_directory}/crime_df.parquet")
    print("Parquet file successfully written to local directory.")
except Exception as e:
    print(f"Error writing Parquet file: {e}")

