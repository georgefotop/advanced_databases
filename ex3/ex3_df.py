from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, DoubleType, DateType
from pyspark.sql.functions import to_date, year, month, desc, rank, col
from pyspark.sql.window import Window

spark = SparkSession \
        .builder \
        .appName("Qeury 1-Dataframe API") \
        .getOrCreate()


crime_df = spark.read.parquet("hdfs://okeanos-master:54310/home/user/code/crime_df.parquet", header=True)


crime_df = crime_df.withColumn('Year', year('DATE OCC'))
crime_df = crime_df.withColumn('Month', month('DATE OCC'))

crimes_by_year_month = crime_df.groupBy(['Year', 'Month']).count()

window_spec = Window.partitionBy('Year').orderBy(desc('count'))
ranked_crimes = crimes_by_year_month.withColumn('rank', rank().over(window_spec))
top_3_months_per_year = ranked_crimes.filter(col('rank') <= 3)

top_3_months_per_year.show(42)
