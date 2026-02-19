# Task 3: Death Percentage Analysis
# Using full_grouped.csv:
# Compute daily death percentage per country:
#Deaths / Confirmed * 100
# Compute global daily death percentage.
# Compute continent-wise death percentage (join with worldometer_data).
# Identify:
# Country with highest death percentage
# Top 10 countries by deaths per capita
# All results must be written to HDFS under /data/covid/analytics(hdfs dfs -ls /data/covid/analytics).

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

print("================Task3 Started============")

spark = SparkSession.builder \
    .appName("Covid-Death-Analysis") \
    .getOrCreate()

ANALYTICS = "hdfs:///data/covid/analytics/"
STAGING   = "hdfs:///data/covid/staging/"

df_full_grouped = spark.read.parquet(STAGING + "full_grouped")
df_worldometer = spark.read.parquet(STAGING + "worldometer_data")


# Compute daily death percentage per country
print("==========computing daily death percentage per country=======")

df_daily_country_death = df_full_grouped.withColumn(
    "Death_Percentage",
    when(col("Confirmed") > 0,
         (col("Deaths") / col("Confirmed")) * 100
    ).otherwise(0)
)

df_daily_country_death.show(20)

df_daily_country_death.write.mode("overwrite").parquet(
    ANALYTICS + "daily_country_death_percentage"
)

print("=======completed computing of daily death percentage=====")


# Compute global daily death percentage
print("=======computing global daily death percentage======")

df_global_daily_death = df_full_grouped.groupBy("Date").agg(
    sum("Deaths").alias("Total_Deaths"),
    sum("Confirmed").alias("Total_Confirmed")
).withColumn(
    "Global_Death_Percentage",
    (col("Total_Deaths") / col("Total_Confirmed")) * 100
)

df_global_daily_death.show()

df_global_daily_death.write.mode("overwrite").parquet(
    ANALYTICS + "global_daily_death_percentage"
)

print("======completed computing global daily death percentage======")


#Compute continent-wise death percentage (join with worldometer_data)
print("=========Computing coninent wise death========")

df_country_totals = df_full_grouped.groupBy("Country_Region").agg(
    sum("Deaths").alias("Total_Deaths"),
    sum("Confirmed").alias("Total_Confirmed")
)

df_joined = df_country_totals.join(
    df_worldometer.select("Country_Region","Continent","Population"),
    "Country_Region",
    "inner"
)

df_continent_death = df_joined.groupBy("Continent").agg(
    sum("Total_Deaths").alias("Deaths"),
    sum("Total_Confirmed").alias("Confirmed")
).withColumn(
    "Continent_Death_Percentage",
    (col("Deaths") / col("Confirmed")) * 100
)

df_continent_death.show()

df_continent_death.write.mode("overwrite").parquet(
    ANALYTICS + "continent_death_percentage"
)

print("====completed computing continent death percentage=======")


# Country with highest death percentage
print("=======finding country with highest death percentage======")

df_country_death_percentage = df_country_totals.withColumn(
    "Death_Percentage",
    (col("Total_Deaths") / col("Total_Confirmed")) * 100
)

df_highest_death_country = df_country_death_percentage.orderBy(
    desc("Death_Percentage")
).limit(1)

df_highest_death_country.show()

df_highest_death_country.write.mode("overwrite").parquet(
    ANALYTICS + "highest_death_percentage_country"
)

print("=====completed computing country with highest death %========")


# Top 10 countries by deaths per capita
print("========finding top 10 cuntries by death per capita=======")

df_deaths_per_capita = df_country_death_percentage.join(
    df_worldometer.select("Country_Region", "Population"),
    "Country_Region",
    "inner"
).withColumn(
    "Deaths_Per_Capita",
    col("Total_Deaths") / col("Population")
)

df_top10_deaths_per_capita = df_deaths_per_capita.orderBy(
    desc("Deaths_Per_Capita")
).limit(10)

df_top10_deaths_per_capita.show()

df_top10_deaths_per_capita.write.mode("overwrite").parquet(
    ANALYTICS + "top10_deaths_per_capita"
)

print("========completed computing top 10 deaths per capita=======")