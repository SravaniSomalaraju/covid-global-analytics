from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time


print("=========spark session started=========")
spark = SparkSession.builder \
    .appName("Covid-Data-Ingestion") \
    .getOrCreate()

RAW = "hdfs:///data/covid/raw/"
STAGING = "hdfs:///data/covid/staging/"

print("==========created struct schema============")
full_grouped_schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Country_Region", StringType(), True),
    StructField("Confirmed", IntegerType(), True),
    StructField("Deaths", IntegerType(), True),
    StructField("Recovered", IntegerType(), True),
    StructField("Active", IntegerType(), True),
    StructField("New_cases", IntegerType(), True),
    StructField("New_deaths", IntegerType(), True),
    StructField("New_recovered", IntegerType(), True),
    StructField("WHO_Region", StringType(), True)
])

covid_clean_schema = StructType([
    StructField("Province_State", StringType(), True),
    StructField("Country_Region", StringType(), True),
    StructField("Lat", DoubleType(), True),
    StructField("Long", DoubleType(), True),
    StructField("Date", StringType(), True),
    StructField("Confirmed", IntegerType(), True),
    StructField("Deaths", IntegerType(), True),
    StructField("Recovered", IntegerType(), True),
    StructField("Active", IntegerType(), True),
    StructField("WHO_Region", StringType(), True)
])

country_latest_schema = StructType([
    StructField("Country_Region", StringType(), True),
    StructField("Confirmed", IntegerType(), True),
    StructField("Deaths", IntegerType(), True),
    StructField("Recovered", IntegerType(), True),
    StructField("Active", IntegerType(), True),
    StructField("New_cases", IntegerType(), True),
    StructField("New_deaths", IntegerType(), True),
    StructField("New_recovered", IntegerType(), True),
    StructField("Deaths_per_100_Cases", DoubleType(), True),
    StructField("Recovered_per_100_Cases", DoubleType(), True)
])

day_wise_schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Confirmed", IntegerType(), True),
    StructField("Deaths", IntegerType(), True),
    StructField("Recovered", IntegerType(), True),
    StructField("Active", IntegerType(), True),
    StructField("New_cases", IntegerType(), True),
    StructField("New_deaths", IntegerType(), True),
    StructField("New_recovered", IntegerType(), True),
    StructField("Deaths_per_100_Cases", DoubleType(), True),
    StructField("Recovered_per_100_Cases", DoubleType(), True)
])

usa_county_schema = StructType([
    StructField("UID", LongType(), True),
    StructField("iso2", StringType(), True),
    StructField("iso3", StringType(), True),
    StructField("code3", IntegerType(), True),
    StructField("FIPS", DoubleType(), True),
    StructField("Admin2", StringType(), True),
    StructField("Province_State", StringType(), True),
    StructField("Country_Region", StringType(), True),
    StructField("Lat", DoubleType(), True),
    StructField("Long_", DoubleType(), True)
])

worldometer_schema = StructType([
    StructField("Country_Region", StringType(), True),
    StructField("Continent", StringType(), True),
    StructField("Population", LongType(), True),
    StructField("TotalCases", LongType(), True),
    StructField("NewCases", LongType(), True),
    StructField("TotalDeaths", LongType(), True),
    StructField("NewDeaths", LongType(), True),
    StructField("TotalRecovered", LongType(), True),
    StructField("NewRecovered", LongType(), True),
    StructField("ActiveCases", LongType(), True)
])

df_full_grouped = spark.read.csv(RAW + "full_grouped.csv",
                         header=True,
                         schema=full_grouped_schema)

df_clean_complete = spark.read.csv(RAW + "covid_19_clean_complete.csv",
                        header=True,
                        schema=covid_clean_schema)

df_country_wise = spark.read.csv(RAW + "country_wise_latest.csv",
                          header=True,
                          schema=country_latest_schema)

df_day_wise = spark.read.csv(RAW + "day_wise.csv",
                          header=True,
                          schema=day_wise_schema)

df_usa_country = spark.read.csv(RAW + "usa_county_wise.csv",
                          header=True,
                          schema=usa_county_schema)

df_worldometer = spark.read.csv(RAW + "worldometer_data.csv",
                          header=True,
                          schema=worldometer_schema)

def handle_nulls(df):
    fill_values = {}

    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            fill_values[field.name] = "Unknown"
        elif isinstance(field.dataType, (IntegerType, LongType, DoubleType)):
            fill_values[field.name] = 0

    return df.fillna(fill_values)

print("=================Handling null values(numeric to 0, string to Unknown)===================")

df_full_grouped = handle_nulls(df_full_grouped)
df_clean_complete = handle_nulls(df_clean_complete)
df_country_wise = handle_nulls(df_country_wise)
df_day_wise = handle_nulls(df_day_wise)
df_usa_country = handle_nulls(df_usa_country)
df_worldometer = handle_nulls(df_worldometer)

print("================Null handling completed=================")

print("===============Writing Parquet files to HDFS staging==========")

df_full_grouped.write.mode("overwrite").parquet(STAGING + "full_grouped")
df_clean_complete.write.mode("overwrite").parquet(STAGING + "covid_clean_complete")
df_country_wise.write.mode("overwrite").parquet(STAGING + "country_wise_latest")
df_day_wise.write.mode("overwrite").parquet(STAGING + "day_wise")
df_usa_country.write.mode("overwrite").parquet(STAGING + "usa_county_wise")
df_worldometer.write.mode("overwrite").parquet(STAGING + "worldometer_data")

print("=======Parquet write completed========")


print("=======Comparing CSV vs Parquet read performance====")

start = time.time()
spark.read.csv(RAW + "full_grouped.csv", header=True).count()
csv_time = time.time() - start

start = time.time()
spark.read.parquet(STAGING + "full_grouped").count()
parquet_time = time.time() - start

print(f"CSV Read Time: {csv_time:.2f} sec")
print(f"Parquet Read Time: {parquet_time:.2f} sec")

print("=======compasrision completed=======")

print("============CSV Execution Plan==========")
spark.read.csv(RAW + "full_grouped.csv", header=True).explain()

print("========Parquet Execution Plan=======")
spark.read.parquet(STAGING + "full_grouped").explain()
print("======parquet execution plan completed==========")

spark.stop()