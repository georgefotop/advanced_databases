from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, DoubleType, DateType
from pyspark.sql.functions import to_date, year, month, desc, rank, col, when, count
from pyspark.sql.window import Window


spark = SparkSession \
        .builder \
        .appName("Qeury 2-Dataframe API") \
        .getOrCreate()


crime_df = spark.read.parquet("hdfs://okeanos-master:54310/home/user/code/crime_df.parquet", header=True)

crime_df = crime_df.withColumn('Year', year('DATE OCC'))
crime_df = crime_df.withColumn('Month', month('DATE OCC'))




morning = (col("TIME OCC") >= 500) & (col("TIME OCC") <= 1159)
afternoon = (col("TIME OCC") >= 1200) & (col("TIME OCC") <= 1659)
evening = (col("TIME OCC") >= 1700) & (col("TIME OCC") <= 2059)
night = ((col("TIME OCC") >= 2100) | (col("TIME OCC") <= 359))

# Create a new column 'Time Intervals' based on the time segments
crime_df = crime_df.withColumn("Time_Interval", when(morning, "Morning")
                               .when(afternoon, "Afternoon")
                               .when(evening, "Evening")
                               .when(night, "Night"))


df_street = crime_df.filter(col("Premis Desc") == "STREET")

# Group by Year, Month, and Time Interval, then count occurrences
crimes_by_year_month_time = df_street.groupBy('Time_Interval').agg(count("*").alias("count"))

# Sort the result in descending order based on count
sorted_crimes = crimes_by_year_month_time.orderBy(desc('count'))

sorted_crimes.show()


