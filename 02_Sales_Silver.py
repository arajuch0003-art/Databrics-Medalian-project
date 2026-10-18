# Databricks notebook source
from pyspark.sql.functions import col

# 1. Read Data from Bronze Delta Table
df_bronze = spark.table("workspace.default.bronze_organizations")

# 2. Clean & Transform Columns
df_silver = (df_bronze
    .select(
        col("Index").cast("int"),
        col("Organization_Id").alias("organization_id"),
        col("Name").alias("company_name"),
        col("Website").alias("website"),
        col("Country").alias("country"),
        col("Description").alias("description"),
        col("Founded").cast("int").alias("founded_year"),
        col("Industry").alias("industry"),
        col("Number_of_employees").cast("int").alias("employee_count")
    )
    .dropDuplicates(["organization_id"])
)

# 3. Save as Silver Delta Table
df_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_organizations")

# 4. Preview Silver Data
display(spark.table("workspace.default.silver_organizations"))

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY workspace.default.silver_organizations"))

def_ver0=spark.read.option("versionAsOf", 0).table("workspace.default.silver_organizations")
display(def_ver0)

# COMMAND ----------

# Create a temporary view for demo update
spark.sql("""
MERGE INTO workspace.default.silver_organizations AS target
USING (
  SELECT 'FAB0d41d5b5d22c' as organization_id, 'Updated Company Name' as company_name
) AS source
ON target.organization_id = source.organization_id
WHEN MATCHED THEN
  UPDATE SET target.company_name = source.company_name
""")
display(spark.table("workspace.default.silver_organizations").filter("organization_id = 'FAB0d41d5b5d22c'"))

# COMMAND ----------

# టేబుల్ లోని ఒక valid organization_id ని చూడటానికి
display(spark.sql("SELECT organization_id, company_name FROM workspace.default.silver_organizations LIMIT 5"))

# COMMAND ----------

# MERGE ki mundu version 0 lo pātā company name em undo choodu:
display(spark.read.option("versionAsOf", 0)
        .table("workspace.default.silver_organizations")
        .filter("company_name != 'Updated Company Name'"))

# Present latest version lo updated name choodu:
display(spark.table("workspace.default.silver_organizations")
        .filter("company_name = 'Updated Company Name'"))