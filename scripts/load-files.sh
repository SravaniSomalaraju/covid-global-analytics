#!/bin/bash

echo "============================================="

echo "===== creating directories ====="

hdfs dfs -mkdir -p /data/covid/raw
hdfs dfs -mkdir -p /data/covid/staging
hdfs dfs -mkdir -p /data/covid/curated
hdfs dfs -mkdir -p /data/covid/analytics

echo "===== verifying directory creation ====="

hdfs dfs -ls /data/covid

echo "===== upload csv files to hdfs ====="

hdfs dfs -put -f /mnt/d/covid_project/datasets/full_grouped.csv /data/covid/raw
hdfs dfs -put -f /mnt/d/covid_project/datasets/covid_19_clean_complete.csv /data/covid/raw
hdfs dfs -put -f /mnt/d/covid_project/datasets/country_wise_latest.csv /data/covid/raw
hdfs dfs -put -f /mnt/d/covid_project/datasets/day_wise.csv /data/covid/raw
hdfs dfs -put -f /mnt/d/covid_project/datasets/usa_county_wise.csv /data/covid/raw
hdfs dfs -put -f /mnt/d/covid_project/datasets/worldometer_data.csv /data/covid/raw

echo "===== verifying files uploaded ====="

hdfs dfs -ls /data/covid/raw


echo "=====Identifying block allocation====="

hdfs fsck /data/covid/raw/full_grouped.csv -files -blocks
hdfs fsck /data/covid/raw/covid_19_clean_complete.csv -files -blocks
hdfs fsck /data/covid/raw/country_wise_latest.csv -files -blocks
hdfs fsck /data/covid/raw/day_wise.csv -files -blocks
hdfs fsck /data/covid/raw/usa_county_wise.csv -files -blocks
hdfs fsck /data/covid/raw/worldometer_data.csv -files -blocks

echo "=====completed hadoop integration====="

