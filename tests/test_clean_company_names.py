import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

# 1. Spark Session Setup
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("PySpark-Unit-Tests").getOrCreate()

# 2. Transformation Logic
def clean_company_names(df):
    return df.withColumn("company_name", upper(col("name")))

# 3. Unit Test
def test_clean_company_names(spark):
    data = [("org_1", "tata motors"), ("org_2", "infosys")]
    schema = ["organization_id", "name"]
    input_df = spark.createDataFrame(data, schema)
    
    output_df = clean_company_names(input_df)
    results = [row.company_name for row in output_df.collect()]
    
    assert results == ["TATA MOTORS", "INFOSYS"]
