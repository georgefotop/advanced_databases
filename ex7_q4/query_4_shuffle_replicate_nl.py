from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, DoubleType, DateType, LongType
from pyspark.sql.functions import to_date, year, month, desc, rank, first, col, count, when, udf, sqrt, pow, mean, min, row_number
from pyspark.sql.window import Window
from math import radians, cos, sin, asin, sqrt
import pyspark.sql.functions as F

spark = SparkSession \
        .builder \
        .appName("SHUFFLE_REPLICATE_NL") \
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
crime_df = crime_df.withColumn("DR_NO", col("DR_NO").cast(LongType()))
crime_df = crime_df.withColumn('Year', year('DATE OCC'))
crime_df = crime_df.withColumn('Month', month('DATE OCC'))

lapd_schema = StructType([ StructField("X", DoubleType()), StructField("Y", DoubleType()), StructField("FID", StringType()),
                          StructField("Division", StringType()),StructField("Location", StringType()), StructField("PREC", StringType())
                        ])

lapd =  spark.read.csv("hdfs://okeanos-master:54310/LAPD_Police_Stations.csv", header=True, schema=lapd_schema)
lapd = lapd.withColumn("PREC", lapd["PREC"].cast(IntegerType()))
@udf(StringType())
def get_distance(lat1, lon1, lat2, lon2):

    lon1 = radians(lon1)
    lon2 = radians(lon2)
    lat1 = radians(lat1)
    lat2 = radians(lat2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of Earth in kilometers
    return (c * r)



spark.udf.register("get_distance", get_distance)

# Filter the DataFrame to keep only rows where Weapon Used Cd is in the range [100, 199]
filtered_df = crime_df.filter((col("Weapon Used Cd") >= 100) & (col("Weapon Used Cd") <= 199))

# Join police_df and filtered_df based on AREA and PREC columns
joined_df = lapd.hint('SHUFFLE_REPLICATE_NL').join(filtered_df, lapd["PREC"] == filtered_df["AREA"])
joined_df.explain()
# Keep specific columns in the result
result_df = joined_df.select("Y", "X", "LAT", "LON", "Year")

result_df = result_df.withColumn(
    "distance",
    get_distance(col("LAT"), col("LON"), col("Y"), col("X"))
)

# Group by "Year" and compute the mean distance and count
grouped_df = result_df.groupBy("Year").agg(
    mean("distance").alias("mean_distance"),
    count("Year").alias("count_per_year")
)

# Order the result by "Year"
grouped_df1 = grouped_df.orderBy("Year")

grouped_df1.show()




filtered_df2 = crime_df.filter(col("Weapon Used Cd").isNotNull())
joined_df = lapd.hint('SHUFFLE_REPLICATE_NL').join(filtered_df2, lapd["PREC"] == filtered_df["AREA"])
joined_df.explain()
# Keep specific columns in the result
result_df = joined_df.select("Y", "X", "LAT", "LON", "Division")

result_df = result_df.withColumn(
    "distance",
    get_distance(col("LAT"), col("LON"), col("Y"), col("X")))
# Group by "PREC" (police station) and compute the mean distance and count
grouped_df = result_df.groupBy("Division").agg(
    mean("distance").alias("mean_distance"),
    count("Division").alias("count_per_station")
)

# Order the result by number of incidents in descending order
grouped_df2 = grouped_df.orderBy("count_per_station", ascending=False)

grouped_df2.show()

joined_df = filtered_df.hint('SHUFFLE_REPLICATE_NL').crossJoin(lapd)
joined_df.explain()
joined_df = joined_df.na.drop(subset=["X", "Y"])
# Calculate the distance
joined_df = joined_df.withColumn(
    "distance",
    get_distance(col("LAT"), col("LON"), col("Y"), col("X"))
)

# Use window function to find the nearest police station for each crime
window_ = Window.partitionBy("DR_NO").orderBy("distance")


# Select the nearest police station for each crime
nearest_station_df = joined_df.groupBy("DR_NO").agg(
    min("distance").alias("min_distance"),
    first("Year").alias("Year"))
# Group by "Year" and compute the mean distance, count, and minimum distance
grouped_by_year_df = nearest_station_df.groupBy("Year").agg(
    mean("min_distance").alias("avg_min_distance"),
    count("DR_NO").alias("count_per_year")
    )




# Order the result by "Year" in ascending order
grouped_by_year_df3 = grouped_by_year_df.orderBy("Year")

# Show results for Query 4a
grouped_by_year_df3.show()


joined_df2 = filtered_df2.hint('SHUFFLE_REPLICATE_NL').crossJoin(lapd)
joined_df2.explain()
joined_df2 = joined_df2.withColumn(
    "distance",
    get_distance(col("LAT"), col("LON"), col("Y"), col("X"))
)
nearest_station_df2 = joined_df2.withColumn("row_num", row_number().over(window_)).filter(col("row_num") == 1)
# Group by "PREC" (police station) and compute the mean distance, count, and minimum distance
grouped_by_station_df = nearest_station_df2.groupBy("Division").agg(
    mean("distance").alias("avg_mean_distance"),
 count("Division").alias("count_per_station"),

)

# Order the result by number of incidents in descending order
grouped_by_station_df4 = grouped_by_station_df.orderBy("count_per_station", ascending=False)

# Show results for Query 4b
grouped_by_station_df4.show()
