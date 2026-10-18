# Databricks notebook source
from pyspark.sql.functions import count, sum, avg, round, desc

# 1. Read Cleaned Data from Silver Delta Table
df_silver = spark.table("workspace.default.silver_organizations")

# 2. Aggregation 1: Country-wise Summary (Business Metric)
df_gold_country = (df_silver
    .groupBy("country")
    .agg(
        count("organization_id").alias("total_organizations"),
        sum("employee_count").alias("total_employees")
    )
    .sort(desc("total_organizations"))
)

# 3. Aggregation 2: Industry-wise Summary
df_gold_industry = (df_silver
    .groupBy("industry")
    .agg(
        count("organization_id").alias("total_organizations"),
        round(avg("employee_count"), 2).alias("avg_employees")
    )
    .sort(desc("total_organizations"))
)

# 4. Save to Gold Delta Tables
df_gold_country.write.format("delta").mode("overwrite").saveAsTable("workspace.default.gold_country_summary")
df_gold_industry.write.format("delta").mode("overwrite").saveAsTable("workspace.default.gold_industry_summary")

# 5. Preview Gold Tables
print("--- Country Summary ---")
display(spark.table("workspace.default.gold_country_summary"))

print("--- Industry Summary ---")
display(spark.table("workspace.default.gold_industry_summary"))