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

# Script generated for node customer_trusted
customer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tkoyama-data-lake/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1686659688398 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tkoyama-data-lake/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1686659688398",
)

# Script generated for node Join
Join_node1686659772841 = Join.apply(
    frame1=customer_trusted_node1,
    frame2=accelerometer_landing_node1686659688398,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1686659772841",
)

# Script generated for node Drop Fields
DropFields_node1686659825708 = DropFields.apply(
    frame=Join_node1686659772841,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1686659825708",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1686659825708,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tkoyama-data-lake/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
