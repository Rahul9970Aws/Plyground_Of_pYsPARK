import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging

logger=logging.getLogger()
logger.setLevel(logging.INFO)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [
    'SOURCE_S3_BUCKET',
    'TARGET_S3_PATH',
    'JOB_NAME',
    'PROCESS_DATE'
    
])
bucket = args['SOURCE_S3_BUCKET']
target_path = args['TARGET_S3_PATH']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info(f"I am in the glue job.... with arguments {bucket} and {target_path}")


job.commit()
