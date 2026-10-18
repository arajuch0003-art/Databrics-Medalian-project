# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, upper
# 1. Checkpoint Path for Silver Stream
silver_checkpoint = "/Volumes/workspace/default/raw_data/_checkpoints/silver_streams"
# 2. Read Stream from Bronze Delta Table
df_bronze_stream = (spark.readStream.table("workspace.default.stream_bronze_organizations"))
# 3. Silver Transformations (Cleaning, Null handling, Uppercase conversion)
#df_silver_stream = (df_bronze_stream.filter(col("Organisation_id").isNotNull())).withColumn("compny_name", upper(col("company_name"))).withColumn("processed_at", current_timestamp)
df_silver_stream = (df_bronze_stream
    .filter(col("organization_id").isNotNull())
    .withColumn("company_name", upper(col("name")))
    .withColumn("processed_at", current_timestamp()))
# 4. Write Stream to Silver Delta Table using Trigger AvailableNow
query_silver = (df_silver_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", silver_checkpoint)
    .trigger(availableNow=True)
    .toTable("workspace.default.stream_silver_organizations"))

query_silver.awaitTermination()

# COMMAND ----------

from pyspark.sql.functions import col, count, current_timestamp

# 1. Checkpoint Path for Gold Stream
gold_checkpoint = "/Volumes/workspace/default/raw_data/_checkpoints/gold_stream"

# 2. Read Stream from Silver Delta Table
df_silver_stream = (spark.readStream
    .table("workspace.default.stream_silver_organizations"))

# 3. Gold Aggregations (Industry-wise Company Count)
df_gold_stream = (df_silver_stream
    .groupBy("industry")
    .agg(count("organization_id").alias("total_companies"))
    .withColumn("updated_at", current_timestamp()))

# 4. Write Stream to Gold Delta Table using Complete Output Mode
query_gold = (df_gold_stream.writeStream
    .format("delta")
    .outputMode("complete") # Aggregations కి "complete" వాడతాం
    .option("checkpointLocation", gold_checkpoint)
    .trigger(availableNow=True)
    .toTable("workspace.default.stream_gold_industry_summary"))

query_gold.awaitTermination()

# COMMAND ----------

# Streaming ద్వారా Table లోకి వచ్చిన డేటాను చూడటానికి
display(spark.table("workspace.default.stream_silver_organizations"))
display(spark.table("workspace.default.stream_gold_industry_summary"))