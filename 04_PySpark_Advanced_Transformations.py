# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, lag, lead

# 1. Read Silver Table Data
df_silver = spark.table("workspace.default.silver_organizations")
# 2. Define Window Specification
# ప్రతీ Industry లో employee_count బట్టి ఫిల్టర్/ర్యాంక్ చేయడానికి Window డెఫినిషన్
windowSpec = Window.partitionBy("industry").orderBy(col("employee_count").desc())

# 3. Apply Window Functions

df_ranked = df_silver.withColumn("row_num",row_number().over(windowSpec))\
.withColumn("rank", rank().over(windowSpec))\
    .withColumn("dense_rank",dense_rank().over(windowSpec))
# 4. Get Top 2 Companies with Highest Employees in Each Industry
df_top_per_industry=df_ranked.filter(col("row_num")<=2)
display(df_top_per_industry.select("industry", "company_name", "employee_count", "row_num", "rank", "dense_rank"))




# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, lead

# 1. Define Window ordered by founded_year
window_year_spec = Window.orderBy("founded_year")

# 2. Apply lag (previous record) and lead (next record)
df_lag_lead = df_silver.select("company_name", "founded_year", "employee_count") \
    .withColumn("prev_company_employees", lag("employee_count", 1).over(window_year_spec)) \
    .withColumn("next_company_employees", lead("employee_count", 1).over(window_year_spec))

display(df_lag_lead.limit(10))