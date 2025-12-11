#!/usr/bin/env python3
"""
Docstring for problem1

Problem 1: log level distribution

this script:
    analyzes the distribution of log levels (INFO, WARN, ERROR, DEBUG) 
        across all log files

outputs (paths in ec2 instance, "/home/ubuntu/spark-cluster/" on master):
1. data/output/problem1_counts.csv
2. data/output/problem1_sample.csv
3. data/output/problem1_summary.txt
"""

import os
import logging
import sys
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# logging setup
logging.basicConfig(
    level = logging.INFO,
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)
logger = logging.getLogger(__name__)

# constants
OUTPUT_DIR = "/home/ubuntu/spark-cluster" # on the master node
COUNTS_PATH = os.path.join(OUTPUT_DIR, "problem1_counts.csv")
SAMPLE_PATH = os.path.join(OUTPUT_DIR, "problem1_sample.csv")
SUMMARY_PATH = os.path.join(OUTPUT_DIR, "problem1_summary.txt")

# spark session
def create_spark_session(master_url: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("AO6P1-Cluster")
        .master(master_url)

        # memory configuration
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")

        # executor configuration
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")  # Across cluster

        # S3 configuration
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider",
        )

        # performance settings
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        # serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # arrow optimization 
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created for cluster at %s", master_url)
    return spark

def read_logs_from_s3(spark: SparkSession):
    '''
    read all log files from s3 logs bucket under /data/
    '''
    bucket = os.getenv("SPARK_LOGS_BUCKET", "s3a://dvf6-assignment-spark-cluster-logs")
    input_path = f"{bucket}/data/"

    logger.info("reading input logs from %s", input_path)

    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .text(input_path)
    )

    logger.info("loaded %d rows of raw logs", df.count())
    return df

# computations

def get_log_level_counts(df):
    '''
    log level counts
    output format (csv):
        log_level, count
        INFO,125430
        WARN,342
        ERROR,89
        DEBUG,12
    '''

    pattern = r'\b(INFO|WARN|ERROR|DEBUG|FATAL|TRACE)\b'

    df_with_level = df.withColumn(
        "log_level",
        F.regexp_extract(F.col("value"), pattern, 1)
    )

    counts_df = (
        df_with_level
        .filter(F.col("log_level") != "")
        .groupBy("log_level")
        .agg(F.count("*").alias("count"))
        .orderBy(F.desc("count"))
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    counts_df.toPandas().to_csv(COUNTS_PATH, index = False)
    logger.info("saved log level counts to %s", COUNTS_PATH)

    return counts_df, df_with_level

def get_log_sample(df_with_level):
    '''
    10 randomly sampled logs
    output format (csv):
        log_entry,log_level
        "17/03/29 10:04:41 INFO ApplicationMaster: Registered signal handlers",INFO
        "17/03/29 10:04:42 WARN YarnAllocator: Container request...",WARN
        ...
    '''

    sample_10_df = (
        df_with_level
        .filter(F.col("log_level") != "")
        .orderBy(F.rand())
        .limit(10)
        .select(F.col("value").alias("log_entry"), "log_level")
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    sample_10_df.toPandas().to_csv(SAMPLE_PATH, index = False)
    logger.info("saved 10 randomly sampled log entries to %s", SAMPLE_PATH)

    return sample_10_df

def get_sum_stats(df_raw, df_with_level, counts_df):
    '''
    summary statistics in txt format
        Total log lines processed: 3,234,567
        Total lines with log levels: 3,100,234
        Unique log levels found: 4

        Log level distribution:
        INFO  :    125,430 (40.45%)
        WARN  :        342 ( 0.01%)
        ...
    '''

    # total rows in raw df
    total_lines = df_raw.count()

    # lines where log level detected
    lines_with_levels = df_with_level.filter(F.col("log_level") != "").count()

    # num of distinct log lvls
    unique_levels = counts_df.count()

    # total across all counted lvls
    total_detected = counts_df.agg(F.sum("count")).first()[0]
    counts = counts_df.collect()

    summary_lines = [
        f"Total log lines processed: {total_lines:,}",
        f"Total lines with log levels: {lines_with_levels:,}",
        f"Unique log levels found: {unique_levels}",
        "",
        "Log level distribution:",
    ]

    for row in counts:
        pct = (row['count'] / total_detected) * 100 if total_detected else 0.0
        summary_lines.append(
            f"{row['log_level']:6}: {row['count']:10,} ({pct:6.2f}%)"
        )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(SUMMARY_PATH, "w") as f:
        f.write("\n".join(summary_lines))

    logger.info("saved summary statistics to %s", SUMMARY_PATH)
    return summary_lines

# main!
def main() -> int:
    # master url
    if len(sys.argv) > 1:
        master_url = sys.argv[1]
    else:
        master_private_ip = os.getenv("MASTER_PRIVATE_IP")
        if master_private_ip:
            master_url = f"spark://{master_private_ip}:7077"
        else:
            print("ERROR: master URL not provided")
            print("usage: python problem1.py spark://MASTER_IP:7077")
            print("or export MASTER_PRIVATE_IP=... and python problem1.py")
            return 1
        
    logger.info("initializing spark session with master %s", master_url)
    spark = create_spark_session(master_url)
    success = False

    try:
        # read all logs from s3
        raw_df = read_logs_from_s3(spark)

        # log lvl counts
        logger.info("computing log level counts")
        counts_df, df_with_level = get_log_level_counts(raw_df)

        # random sample of 10 logs
        logger.info("computing random 10 log sample")
        get_log_sample(df_with_level)

        # summary stats
        logger.info("computing summary statistics")
        get_sum_stats(raw_df, df_with_level, counts_df)

        success = True
    except Exception as e:
        logger.exception("error during problem 1 processing: %s", str(e))
        success = False
    finally:
        logger.info("stopping spark session")
        spark.stop()

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())