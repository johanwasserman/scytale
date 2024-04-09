from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, TimestampType

# Define the schema for pull_requests array element
pull_request_schema = StructType([
    StructField("title", StringType(), True),
    StructField("state", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("closed_at", StringType(), True),
    StructField("merged_at", StringType(), True),
    StructField("base_branch", StringType(), True),
    StructField("head_branch", StringType(), True)
])

# Define the main schema for the JSON data
schema = StructType([
    StructField("full_name", StringType(), True),
    StructField("name", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("owner", StringType(), True),
    StructField("pull_requests", ArrayType(pull_request_schema), True)
])
