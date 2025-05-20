import sys
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from awsglue.job import Job


# Initialize Spark session and Glue context and read the job arguments
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()
args = getResolvedOptions(sys.argv,
                          ['primary_keys',
                           'input_s3_path',
                           'output_s3_path',
                           'database_name',
                           'table_name'])

# Read Parquet files
primary_keys = args["primary_keys"].split(",") # Primary keys are passed as a comma-separated string
input_s3_path = args['input_s3_path']
output_s3_path = args['output_s3_path']
df = spark.read.option("mergeSchema", "true").parquet(input_s3_path)

# Deduplicate using the primary keys
dedup_columns = primary_keys
window_spec = Window.partitionBy(*dedup_columns).orderBy(col(dedup_columns[0]).desc())
df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))
dedup_df = df_with_row_num.filter(col("row_num") == 1).drop("row_num")
table = "glue_catalog." + args["database_name"] + "." + args["table_name"]

try:
    # Try to append the data to the table
    dedup_df.writeTo(table) \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .tableProperty("format-version", "2") \
    .option("mergeSchema", "true") \
    .option("useGlueParquetWriter", "true") \
    .append()
except Exception as e:
    # If the table does not exist, create it and write the data
    dedup_df.writeTo(table) \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .tableProperty("write.spark.accept-any-schema", "true") \
    .tableProperty("format-version", "2") \
    .option("useGlueParquetWriter", "true") \
    .create()

job.commit()