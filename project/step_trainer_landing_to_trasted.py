import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tkoyama-data-lake/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1686685864299 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tkoyama-data-lake/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1686685864299",
)

# Script generated for node Join
Join_node1686685948663 = Join.apply(
    frame1=step_trainer_landing_node1686685864299,
    frame2=accelerometer_trusted_node1,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="Join_node1686685948663",
)

# Script generated for node Drop Fields
DropFields_node1686698387896 = DropFields.apply(
    frame=Join_node1686685948663,
    paths=["z", "timeStamp", "user", "y", "x"],
    transformation_ctx="DropFields_node1686698387896",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1686698387896,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tkoyama-data-lake/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
