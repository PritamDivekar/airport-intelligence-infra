import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    col, year, month, hour, when,
    avg, count, expr
)


# --------------------------------------------------
# JOB INIT
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
output_path = f"s3://{args['OUTPUT_BUCKET']}/transformed/departure_congestion/"

# --------------------------------------------------
# READ RAW CSV FLIGHT DATA
# --------------------------------------------------
df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [raw_path],
        "recurse": True
    },
    format="csv",
    format_options={
        "withHeader": True,
        "separator": ","
    }
).toDF()

# --------------------------------------------------
# DERIVE DATE & HOUR
# --------------------------------------------------
df = (
    df
    .withColumn("year", year("FL_DATE"))
    .withColumn("month", month("FL_DATE"))
    .withColumn("dep_hour", hour("CRS_DEP_TIME"))
)

# --------------------------------------------------
# TIME-OF-DAY BUCKETS
# --------------------------------------------------
df = df.withColumn(
    "dep_time_bucket",
    when(col("dep_hour").between(5, 8), "Early Morning")
    .when(col("dep_hour").between(9, 12), "Morning")
    .when(col("dep_hour").between(13, 16), "Afternoon")
    .when(col("dep_hour").between(17, 20), "Evening (Peak)")
    .when(col("dep_hour").between(21, 23), "Night")
    .otherwise("Late Night")
)

# --------------------------------------------------
# ON-TIME FLAG
# --------------------------------------------------
df = df.withColumn(
    "on_time_dep",
    when(col("DEP_DELAY") <= 15, 1).otherwise(0)
)

# --------------------------------------------------
# AGGREGATE
# --------------------------------------------------
agg = (
    df.groupBy(
        "ORIGIN",
        "year",
        "month",
        "dep_time_bucket"
    )
    .agg(
        count("*").alias("flights_count"),
        avg("DEP_DELAY").alias("avg_dep_delay"),
        avg("TAXI_OUT").alias("avg_taxi_out"),
        avg("on_time_dep").alias("on_time_dep_pct")
    )
)

# --------------------------------------------------
# NORMALIZATION
# --------------------------------------------------
window_expr = "PARTITION BY ORIGIN, year, month"

agg = (
    agg
    .withColumn(
        "norm_flights",
        expr(f"""
            (flights_count - min(flights_count) OVER ({window_expr})) /
            NULLIF(max(flights_count) OVER ({window_expr}) - min(flights_count) OVER ({window_expr}), 0)
        """)
    )
    .withColumn(
        "norm_taxi",
        expr(f"""
            (avg_taxi_out - min(avg_taxi_out) OVER ({window_expr})) /
            NULLIF(max(avg_taxi_out) OVER ({window_expr}) - min(avg_taxi_out) OVER ({window_expr}), 0)
        """)
    )
    .withColumn(
        "norm_delay",
        expr(f"""
            (avg_dep_delay - min(avg_dep_delay) OVER ({window_expr})) /
            NULLIF(max(avg_dep_delay) OVER ({window_expr}) - min(avg_dep_delay) OVER ({window_expr}), 0)
        """)
    )
)

# --------------------------------------------------
# SCORES
# --------------------------------------------------
agg = agg.withColumn(
    "congestion_score",
    expr("ROUND(100 * (0.4*norm_flights + 0.3*norm_taxi + 0.3*norm_delay), 1)")
)

agg = agg.withColumn(
    "efficiency_score",
    expr("ROUND(100 * (0.5*on_time_dep_pct + 0.3*(1-norm_delay) + 0.2*(1-norm_taxi)), 1)")
)

# --------------------------------------------------
# WRITE OUTPUT (UNCHANGED STRUCTURE)
# --------------------------------------------------
agg.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(output_path)

# --------------------------------------------------
# COMMIT
# --------------------------------------------------
job.commit()
