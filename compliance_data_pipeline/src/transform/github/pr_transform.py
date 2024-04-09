import os
import yaml
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # Import pyspark.sql.functions as F
from pyspark.sql.window import Window
from schema import schema

def find_project_root(current_directory):
    """
    Find the root directory of the project by iteratively checking for the existence of the specified project folder name in the current directory and its parent directories.

    Args:
        current_directory (str): The starting directory to begin the search.

    Returns:
        str: The path to the project's root directory, or None if the project folder name is not found.
    """
    project_folder_name = 'compliance_data_pipeline'
    while True:
        # Check if the specified project folder name exists in the current directory
        if project_folder_name in os.listdir(current_directory):
            return os.path.join(current_directory, project_folder_name)  # Found the project root folder
        # Move up one directory level
        current_directory = os.path.dirname(current_directory)
        # Stop if we've reached the root directory (i.e., cannot move up further)
        if current_directory == os.path.dirname(current_directory):
            break
    return None  # Unable to determine the project root


# Get the path to the project's root folder
current_dir = os.path.abspath(os.getcwd())
project_root = find_project_root(current_dir)

# Specify the path to the log file (github_transform.log) in the log folder
log_file_path = os.path.join(project_root, 'logs', 'github_pr_transform.log')

# Configure logging to write to the log file in the project log folder
logging.basicConfig(filename=log_file_path, level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

config_file_path = os.path.join(project_root, 'github_profiles.yml')
with open(config_file_path, 'r') as file:
    profiles = yaml.safe_load(file)
    
    
# Initialize Spark session
spark = SparkSession.builder \
    .appName("GitHub Data Processing") \
    .getOrCreate()


for org_key, profiles in profiles.items():

    for profile_name in profiles:

        base_path = os.path.join(project_root, 'data_lake', 'github', 'pull_requests')
        partition_path = os.path.join(base_path, '*', org_key, profile_name, '*.json')
        output_path = os.path.join(project_root, 'data_warehouse', 'github', 'pull_requests', org_key, profile_name)
        os.makedirs(output_path, exist_ok=True)

        try:

            # Read data from a partitioned directory structure using 'date' as the partition
            df = spark.read \
                    .schema(schema) \
                    .option("basePath", base_path) \
                    .option("multiLine", "true") \
                    .json(partition_path) \
                    .withColumn("date", F.regexp_extract(F.input_file_name(), r"/(\d{4}-\d{2}-\d{2})/", 1))

            # Define a window specification partitioned by 'id' and ordered by 'date' in descending order
            window_spec = Window.partitionBy("id").orderBy(F.col("date").desc())

            #Filter the data to keep only the latest record per partition
            df = df.withColumn("rank", F.dense_rank().over(window_spec)) \
                .filter("rank == 1") \
                .drop("rank")

            # Derive the DataFrame with required transformations and aggregations
            df = df.withColumn("organization_name", F.split("full_name", "/")[0]) \
                .withColumnRenamed("id", "repository_id") \
                .withColumnRenamed("name", "repository_name") \
                .withColumnRenamed("owner", "repository_owner") \
                .withColumn("num_prs", F.size(F.expr("filter(pull_requests, pr -> pr.state != 'closed')"))) \
                .withColumn("num_prs_merged", F.size(F.expr("filter(pull_requests, pr -> pr.state != 'closed' AND pr.merged_at IS NOT NULL)"))) \
                .withColumn("merged_at", F.expr("aggregate(filter(pull_requests, pr -> pr.state != 'closed' AND pr.merged_at IS NOT NULL), cast(null as string), (acc, pr) -> IF(acc IS NULL OR pr.merged_at > acc, pr.merged_at, acc))")) \
                .withColumn("is_compliant", (F.col("num_prs") == F.col("num_prs_merged")) & (F.lower(F.col("repository_owner")).contains("scytale"))) \
                .select(
                    "organization_name",
                    "repository_id",
                    "repository_name",
                    "repository_owner",
                    "num_prs",
                    "num_prs_merged",
                    "merged_at",
                    "is_compliant"
                )
                
            # Save DataFrame to Parquet files
            df.write \
                .mode("overwrite") \
                .parquet(output_path)  
                
        except Exception as e:
                # Log the error to the log file
                logging.error(f"{org_key}:{profile_name} - Error occurred during data processing:\n{str(e)}")

# Stop Spark session
spark.stop()


