# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# 1. Volume Paths Setup
input_directory = "/Volumes/workspace/default/raw_data"
checkpoint_directory = "/Volumes/workspace/default/raw_data/_checkpoints/sream_demo"
# 2. Read Stream using Databricks Auto Loader (cloudFiles)
df_stream_raw=spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("header", "true").option("cloudFiles.schemaLocation", f"{checkpoint_directory}/schema").load(input_directory)
# 3. Simple Transformation (Ingestion Timestamp Add చేయడం)
#df_stream_bronze = df_stream_raw.withColumn("ingested_at", current_timestamp())

# 3. Clean Column Names (Spaces తీసేసి Lowercase లోకి మార్చడం & Timestamp Add చేయడం)
df_cleaned_stream = df_stream_raw
for col_name in df_stream_raw.columns:
    clean_name = col_name.strip().lower().replace(" ", "_").replace("-", "_")
    df_cleaned_stream = df_cleaned_stream.withColumnRenamed(col_name, clean_name)
    df_stream_bronze = df_cleaned_stream.withColumn("ingested_at", current_timestamp())
# 4. Write Stream to Delta Table (Continuous Processing)
query = (df_stream_bronze.writeStream
   .format("delta")
   .outputMode("append")
    .option("checkpointLocation", checkpoint_directory)
    .trigger(availableNow=True)
    .toTable("workspace.default.stream_bronze_organizations"))


# COMMAND ----------

# Streaming ద్వారా Table లోకి వచ్చిన డేటాను చూడటానికి
display(spark.table("workspace.default.stream_bronze_organizations"))