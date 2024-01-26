from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, DoubleType, DateType
from pyspark.sql.functions import to_date, year, month, desc, rank, col, when
from pyspark.sql.window import Window


spark = SparkSession \
        .builder \
        .appName("Qeury 2-Sql API") \
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
crime_df = crime_df.withColumn("Date Rptd",to_date("Date Rptd", "MMa/dd/yyyy hh:mm:ss a"))
crime_df = crime_df.withColumn("Date OCC",to_date("Date OCC", "MM/dd/yyyy hh:mm:ss a"))
crime_df = crime_df.withColumn("Vict Age", crime_df["Vict Age"].cast(IntegerType()))


crime_df.createOrReplaceTempView("crime")

id_query = """
    WITH TimeIntervals AS (
        SELECT
            CASE
                WHEN `TIME OCC` BETWEEN 500 AND 1159 THEN 'Morning'
                WHEN `TIME OCC` BETWEEN 1200 AND 1659 THEN 'Afternoon'
                WHEN `TIME OCC` BETWEEN 1700 AND 2059 THEN 'Evening'
                WHEN `TIME OCC` >= 2100 OR `TIME OCC` <= 359 THEN 'Night'
                ELSE NULL
            END AS Time_Interval,
            *
        FROM
            crime
    )

    SELECT
        Time_Interval,
        COUNT(*) AS count
    FROM
        TimeIntervals
    WHERE
        `Premis Desc` = 'STREET'
    GROUP BY
        Time_Interval
    ORDER BY
        count DESC;
"""


result = spark.sql(id_query)
result.show(42)
