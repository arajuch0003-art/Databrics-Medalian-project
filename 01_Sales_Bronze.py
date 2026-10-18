# Databricks notebook source
# 1. Read Raw CSV
input_path = "/Volumes/workspace/default/raw_data/organizations-100.csv"

df_raw = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path))

# 2. Clean column names (Spaces ని Underscores గా మార్చడం)
for col_name in df_raw.columns:
    clean_col = col_name.strip().replace(" ", "_").replace("(", "").replace(")", "")
    df_raw = df_raw.withColumnRenamed(col_name, clean_col)

# 3. Save as Bronze Delta Table
df_raw.write.format("delta").mode("overwrite").saveAsTable("workspace.default.bronze_organizations")

# 4. Preview
display(spark.table("workspace.default.bronze_organizations"))