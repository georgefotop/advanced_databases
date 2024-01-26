from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, DoubleType, DateType
from pyspark.sql.functions import to_date, year, month, desc, rank, col
from pyspark.sql.window import Window

spark = SparkSession \
        .builder \
        .appName("Qeury 1-SQL API") \
        .getOrCreate()


crime_df = spark.read.parquet("hdfs://okeanos-master:54310/home/user/code/crime_df.parquet", header=True)

crime_df.createOrReplaceTempView("crime")

id_query = "WITH tempTable as (SELECT Year, Month, crime_total, RANK() OVER (PARTITION BY Year ORDER BY crime_total DESC) as rank FROM \
        (SELECT Year, Month, crime_total \
        FROM ( SELECT YEAR(`Date OCC`) AS Year, MONTH(`Date OCC`) AS Month, COUNT(*) as crime_total FROM crime GROUP BY Year,Month))) \
        SELECT * FROM tempTable WHERE rank<=3 \
        ORDER BY Year ASC, crime_total DESC"

result = spark.sql(id_query)
result.show(42)
