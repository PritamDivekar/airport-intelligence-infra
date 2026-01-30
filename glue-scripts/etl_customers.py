import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    year, month, col, avg, count, when, to_timestamp
)

# --------------------------------------------------
# JOB INITIALIZATION
# --------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'RAW_BUCKET', 'OUTPUT_BUCKET']
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --------------------------------------------------
# S3 PATHS (ONLY CHANGE)
# --------------------------------------------------
raw_path = f"s3://{args['RAW_BUCKET']}/raw/"
reference_path = f"s3://{args['RAW_BUCKET']}/reference/airlines/"
output_path = f"s3://{args['OUTPUT_BUCKET']}/transformed/customers/"

# --------------------------------------------------
# READ RAW FLIGHT DATA (CSV FROM S3)
# --------------------------------------------------
raw_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [raw_path],
        "recurse": True
    },
    format="csv",
    format_options={
        "withHeader": True,
        "separator": ",",
        "quoteChar": '"'
    }
).toDF()

# --------------------------------------------------
# CAST DATE (VERY IMPORTANT)
# --------------------------------------------------
df = raw_df.withColumn(
    "FL_DATE",
    to_timestamp(col("FL_DATE"))
)

# --------------------------------------------------
# DERIVED COLUMNS
# --------------------------------------------------
df = (
    df
    .withColumn("year", year("FL_DATE"))
    .withColumn("month", month("FL_DATE"))
    .withColumn(
        "on_time_dep",
        when(col("DEP_DELAY").isNotNull() & (col("DEP_DELAY") <= 15), 1).otherwise(0)
    )
    .withColumn(
        "on_time_arr",
        when(col("ARR_DELAY").isNotNull() & (col("ARR_DELAY") <= 15), 1).otherwise(0)
    )
)

# --------------------------------------------------
# CORE AGGREGATION
# FINAL GRAIN:
# year + month + ORIGIN + DEST + OP_CARRIER
# --------------------------------------------------
agg_df = (
    df.groupBy(
        "year",
        "month",
        "ORIGIN",
        "DEST",
        "OP_CARRIER"
    )
    .agg(
        count("*").alias("total_flights"),
        avg("DEP_DELAY").alias("avg_dep_delay"),
        avg("ARR_DELAY").alias("avg_arr_delay"),
        (avg("on_time_dep") * 100).alias("on_time_dep_pct"),
        (avg("on_time_arr") * 100).alias("on_time_arr_pct"),
        avg("TAXI_OUT").alias("avg_taxi_out"),
        avg("TAXI_IN").alias("avg_taxi_in"),
        avg("AIR_TIME").alias("avg_air_time"),
        avg("O_TEMP").alias("avg_o_temp"),
        avg("O_PRCP").alias("avg_o_prcp"),
        avg("O_WSPD").alias("avg_o_wspd"),
        avg("D_TEMP").alias("avg_d_temp"),
        avg("D_PRCP").alias("avg_d_prcp"),
        avg("D_WSPD").alias("avg_d_wspd")
    )
)

# --------------------------------------------------
# READ LOOKUP: ORIGIN AIRPORT FULL NAME
# --------------------------------------------------
origin_lkp = spark.read.option("header", True) \
    .csv(f"{reference_path}ORIGIN .csv") \
    .select(
        col("Code").alias("ORIGIN"),
        col("Airport Name").alias("origin_airport_name")
    )

# --------------------------------------------------
# READ LOOKUP: DEST AIRPORT FULL NAME
# --------------------------------------------------
dest_lkp = spark.read.option("header", True) \
    .csv(f"{reference_path}DEST Airport.csv") \
    .select(
        col("Code").alias("DEST"),
        col("Airport Name").alias("dest_airport_name")
    )

# --------------------------------------------------
# READ LOOKUP: OP_CARRIER FULL NAME
# --------------------------------------------------
carrier_lkp = spark.read.option("header", True) \
    .csv(f"{reference_path}OP_Carrier code to full airline name.csv") \
    .select(
        col("Code").alias("OP_CARRIER"),
        col("Airline Name").alias("carrier_full_name")
    )

# --------------------------------------------------
# JOIN LOOKUPS (LEFT JOINS)
# --------------------------------------------------
final_df = (
    agg_df
    .join(origin_lkp, on="ORIGIN", how="left")
    .join(dest_lkp, on="DEST", how="left")
    .join(carrier_lkp, on="OP_CARRIER", how="left")
)

# --------------------------------------------------
# WRITE FINAL TABLE (UNCHANGED STRUCTURE)
# --------------------------------------------------
final_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(output_path)

# --------------------------------------------------
# JOB COMMIT
# --------------------------------------------------
job.commit()
