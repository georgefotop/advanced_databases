from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, LongType, FloatType, StringType, DoubleType, DateType
from pyspark.sql.functions import to_date, regexp_replace, to_timestamp, udf, expr, when,   year, month, desc, rank, col, when, count, collect_list
from pyspark.sql.window import Window


spark = SparkSession \
        .builder \
        .appName("Qeury 3-Shuffle Hash") \
        .getOrCreate()


crime_df = spark.read.parquet("hdfs://okeanos-master:54310/home/user/code/crime_df.parquet", header=True)
crime_df = crime_df.withColumn('Year', year('DATE OCC'))
crime_df = crime_df.withColumn('Month', month('DATE OCC'))
revgeocoding_schema= StructType([  StructField("LAT", StringType()), StructField("LON", StringType()),StructField("ZIPcode", StringType())])
Income_schema= StructType([  StructField("Zip Code", StringType()), StructField("Community", StringType()),StructField("Estimated Median Income", StringType())])
rev = spark.read.csv("hdfs://okeanos-master:54310/revgeocoding", header=True, schema=revgeocoding_schema)
la_income = spark.read.csv("hdfs://okeanos-master:54310/LA_income", header=True, schema=Income_schema)
la_income = la_income.withColumn("Zip Code", col("Zip Code").cast(LongType()))
la_income = la_income.withColumn("Estimated Median Income", regexp_replace("Estimated Median Income", "[\$,]", ""))
la_income = la_income.withColumn("Estimated Median Income", col("Estimated Median Income").cast(IntegerType()))
#########################################################

rev = rev.withColumn("ZIPcode", col("ZIPcode").cast(LongType()))
rev = rev.withColumn("LON", col("LON").cast(DoubleType()))
rev = rev.withColumn("LAT", col("LAT").cast(DoubleType()))
#########################################################
# Convert the Integer type columns
crime_df = crime_df.withColumn("DR_NO", col("DR_NO").cast(LongType()))  #Int 64
crime_df = crime_df.withColumn("TIME OCC", col("TIME OCC").cast(IntegerType()))
crime_df = crime_df.withColumn("AREA", col("AREA").cast(IntegerType()))
crime_df = crime_df.withColumn("Rpt Dist No", col("Rpt Dist No").cast(IntegerType()))
crime_df = crime_df.withColumn("Part 1-2", col("Part 1-2").cast(IntegerType()))
crime_df = crime_df.withColumn("Crm Cd", col("Crm Cd").cast(IntegerType()))
crime_df = crime_df.withColumn("Vict Age", col("Vict Age").cast(IntegerType()))

##
crime_df = crime_df.withColumn("Date Rptd", to_timestamp(col("Date Rptd"), "MM/dd/yyyy hh:mm:ss a"))
crime_df = crime_df.withColumn("DATE OCC", to_timestamp(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))


#Convert the Float type columns
crime_df = crime_df.withColumn("Premis Cd", col("Premis Cd").cast(FloatType()))
crime_df = crime_df.withColumn("Weapon Used Cd", col("Weapon Used Cd").cast(FloatType()))
crime_df = crime_df.withColumn("Crm Cd 1", col("Crm Cd 1").cast(FloatType()))
crime_df = crime_df.withColumn("Crm Cd 2", col("Crm Cd 2").cast(FloatType()))
crime_df = crime_df.withColumn("Crm Cd 3", col("Crm Cd 3").cast(FloatType()))
crime_df = crime_df.withColumn("Crm Cd 4", col("Crm Cd 4").cast(FloatType()))

#Convert the Double type columns
crime_df = crime_df.withColumn("LAT", col("LAT").cast(DoubleType()))
crime_df = crime_df.withColumn("LON", col("LON").cast(DoubleType()))
# We add column year
#########################################  Mporei na thelei Date Rptd (SOS)
crime_df = crime_df.withColumn("Year", year(col("DATE OCC")))


# We keep only the rows that Vict Descent in not null
crime_df = crime_df.filter(col("Vict Descent").isNotNull())

crime_df = crime_df.filter(col("Year") == 2015)

##
# Mapping Descent Codes to full names
descent_mapping = {
    'A': 'Other Asian',
    'B': 'Black',
    'C': 'Chinese',
    'D': 'Cambodian',
    'F': 'Filipino',
    'G': 'Guamanian',
    'H': 'Hispanic/Latin/Mexican',
    'I': 'American Indian/Alaskan Native',
    'J': 'Japanese',
    'K': 'Korean',
    'L': 'Laotian',
    'O': 'Other',
    'P': 'Pacific Islander',
    'S': 'Samoan',
 'U': 'Hawaiian',
    'V': 'Vietnamese',
    'W': 'White',
    'X': 'Unknown',
    'Z': 'Asian Indian'
}

# Define a UDF (User Defined Function) to apply the mapping
@udf(StringType())
def map_descent_code(descent_code):
    return descent_mapping.get(descent_code, descent_code)

# Register the UDF
spark.udf.register("map_descent_code", map_descent_code)

# Apply the mapping to the DataFrame
crime_df = crime_df.withColumn("Full Descent", when(col("Vict Descent").isNotNull(), map_descent_code(col("Vict Descent"))))

la_income = la_income.filter(col("Estimated Median Income").isNotNull())

la_income = la_income.filter(~((col("Zip Code") == 91046) | (col("Zip Code") == 91210)))
#unique_values = income_df.select("Estimated Median Income").distinct().show()
sorted_income = la_income.orderBy("Estimated Median Income")

first_3_rows = sorted_income.limit(3)
first_3_rows = first_3_rows.orderBy("Estimated Median Income", ascending=False)
last_3_rows = sorted_income.orderBy("Estimated Median Income", ascending=False).limit(3)

# Concatenate the DataFrames
df = last_3_rows.union(first_3_rows)
"""
# Select the first row
for i in range(5):
    print("----------------------------------------")
    row = la_income.head(i+1)[i]

    # Iterate over the columns and print the values
    for col_name, value in zip(la_income.columns, row):
        print(f"{col_name}: {value}")

"""

# Inner join of reverse_df and new_df on zip code
joined_df = rev.hint('SHUFFLE_HASH').join(df.select("Zip Code"), rev["ZIPcode"] == df["Zip Code"], "inner")
joined_df.explain()
# Drop the extra col zip code
joined_df.drop("ZIPcode")

# Show the resulting DataFrame
#joined_df.show()

#new_df.show()
unique_zipcodes = joined_df.select("Zip Code").distinct()
# Show the unique values
#unique_zipcodes.show()
unique_lat_lon_pairs = joined_df.select("LAT", "LON").dropDuplicates()
joined_df.drop("ZIPcode")

# Show the resulting DataFrame
#joined_df.show()

#new_df.show()
unique_zipcodes = joined_df.select("Zip Code").distinct()
# Show the unique values
#unique_zipcodes.show()
unique_lat_lon_pairs = joined_df.select("LAT", "LON").dropDuplicates()

# Show the unique pairs of LAT and LON
#unique_lat_lon_pairs.show()


#############################
# Join df with unique_lat_lon_pairs on LAT and LON columns
filtered_df = crime_df.hint('SHUFFLE_HASH').join(unique_lat_lon_pairs, ["LAT", "LON"], "inner")
filtered_df.explain()
# Show the resulting DataFrame
#filtered_df.show()

# Group by "Vict Descent" and count the rows for each group
vict_descent_counts = filtered_df.groupBy("Full Descent").count()
#
vict_descent_counts = vict_descent_counts.orderBy("count", ascending=False)
#
# Show the resulting DataFrame
vict_descent_counts.show()

