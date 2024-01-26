from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, DoubleType, DateType
from pyspark.sql.functions import to_date, year, month, desc, rank, col, when, count, collect_list
from pyspark.sql.window import Window


spark = SparkSession \
        .builder \
        .appName("Qeury 3-SQL API") \
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

crime_df = crime_df.withColumn('Year', year('DATE OCC'))
crime_df = crime_df.withColumn('Month', month('DATE OCC'))
revgeocoding_schema= StructType([  StructField("LAT", StringType()), StructField("LON", StringType()),StructField("ZIPcode", StringType())])
Income_schema= StructType([  StructField("ZIP Code", StringType()), StructField("Community", StringType()),StructField("Estimated Median Income", StringType())])
revgecoding = spark.read.csv("hdfs://okeanos-master:54310/revgeocoding", header=True, schema=revgeocoding_schema)
la_income = spark.read.csv("hdfs://okeanos-master:54310/LA_income", header=True, schema=Income_schema)
crime_df.createOrReplaceTempView("crime")
revgecoding.createOrReplaceTempView("rev")
la_income.createOrReplaceTempView("income")

# 2. Write SQL query
sql_query1= """  SELECT
    `Vict Descent`,
    SUM(VictimCount) AS Victim_Count,
    RANK() OVER (ORDER BY SUM(VictimCount) DESC) AS Rank
FROM (
    SELECT
        c.`Vict Descent`,
        r.ZIPcode,
        COUNT(*) AS VictimCount
    FROM
        crime c
        JOIN rev r ON c.LAT = r.LAT AND c.LON = r.LON
        JOIN income i ON r.ZIPcode = i.`ZIP Code`
    WHERE
        YEAR(c.`DATE OCC`) = 2015
    GROUP BY
        c.`Vict Descent`, r.ZIPcode
) AS grouped_data
GROUP BY
    `Vict Descent`
ORDER BY
    Rank;


 """      
sql_query2 = """
    -- Create a common table expression (CTE) for the crime data
WITH crime_view AS (
    SELECT
        c.`Vict Descent`,
        r.LAT,
        r.LON,
        r.ZIPcode,
        i.`Estimated Median Income`
    FROM
        crime c
    JOIN
        rev r ON c.LAT = r.LAT AND c.LON = r.LON
    JOIN
        income i ON r.ZIPcode = i.`ZIP Code`
    WHERE
        YEAR(c.`DATE OCC`) = 2015
)

-- Create a common table expression (CTE) for the aggregated victim counts
, victim_counts AS (
    SELECT
        `Vict Descent`,
        ZIPcode,
        COUNT(*) AS VictimCount,
        RANK() OVER (PARTITION BY ZIPcode ORDER BY COUNT(*) DESC) AS Rank
    FROM
        crime_view
    GROUP BY
        `Vict Descent`,
        ZIPcode
)

-- Retrieve the top 3 and bottom 3 ZIP codes for each racial group
SELECT
    `Vict Descent`,
    ZIPcode,
    VictimCount,
    Rank
FROM
    victim_counts
WHERE
    Rank <= 3 OR Rank >= (SELECT MAX(Rank) FROM victim_counts) - 2;

"""

# 3. Execute SQL query
result_df = spark.sql(sql_query1)

# 4. Show the results
result_df.show()

# 5. Stop Spark session
spark.stop()

