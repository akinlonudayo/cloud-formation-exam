import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import lit

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_BUCKET', 'SOURCE_KEY', 'TARGET_BUCKET', 'TARGET_KEY'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read CSV from S3
source_path = f"s3://{args['SOURCE_BUCKET']}/{args['SOURCE_KEY']}"
df = spark.read.option("header", "true").csv(source_path)

# Simple transformation: add a new column
df_transformed = df.withColumn("processed_flag", lit(True))

# Write transformed data back to S3
target_path = f"s3://{args['TARGET_BUCKET']}/{args['TARGET_KEY']}"
df_transformed.write.mode("overwrite").csv(target_path, header=True)

job.commit()
