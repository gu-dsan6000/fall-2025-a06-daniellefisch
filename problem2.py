#!/usr/bin/env python3
"""
Docstring for problem2

Problem 2: cluster usage analysis

this script:
    analyzes cluster usage patterns to understand which clusters are most heavily used over time
    extracts cluster IDs, application IDs, and application start/ end times
    creates visualizations with seaborn

answers:
    how many unique clusters are in the dataset?
    how many applications ran on each cluster?
    which clusters are most heavily used?
    what is the timeline of application execution across clusters?

outputs:
1. data/output/problem2_timeline.csv
2. data/output/problem2_cluster_summary.csv
3. data/output/problem2_stats.txt
4. data/output/problem2_bar_chart.png
5. data/output/problem2_desnity_plot.png
"""

import os
import sys
import logging
import argparse

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    input_file_name,
    regexp_extract,
    col,
)

# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)
logger = logging.getLogger(__name__)

# constants
OUTPUT_DIR = "data/output"
TIMELINE_PATH = os.path.join(OUTPUT_DIR, "problem2_timeline.csv")
CLUSTER_SUMMARY_PATH = os.path.join(OUTPUT_DIR, "problem2_cluster_summary.csv")
STATS_PATH = os.path.join(OUTPUT_DIR, "problem2_stats.txt")
BAR_CHART_PATH = os.path.join(OUTPUT_DIR, "problem2_bar_chart.png")
DENSITY_PLOT_PATH = os.path.join(OUTPUT_DIR, "problem2_density_plot.png")

# arg parsing
def parse_args():
    parser = argparse.ArgumentParser(description="problem 2: cluster usage analysis")

    # master url only needed when not using skip spark, so making it opt
    parser.add_argument(
        "master_url",
        nargs="?",
        default=None,
        help="spark master url, if omitted, will use master private ip env var"
    )
    parser.add_argument(
        "--net-id",
        type=str,
        default=None,
        help="netID for logging and identifiaction",
    )
    parser.add_argument(
        "--skip-spark",
        action="store_true",
        help="skip spark process, just generate from existing csvs"
    )

    return parser.parse_args()

# spark 
def create_spark_session(master_url: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("AO6P2-ClusterUsage")
        .master(master_url)

        # memory configuration
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")

        # executor configuration
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")

        # S3A configuration
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
    read all log files from s3 log bucket under /data/
    '''
    bucket = os.getenv("SPARK_LOGS_BUCKET", "s3a://dvf6-assignment-spark-cluster-logs")
    input_path = f"{bucket}/data/"
    logger.info("reading input logs from %s", input_path)

    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .text(input_path)
        .withColumn("file_path", input_file_name())
    )
    logger.info("loaded %d rows of raw logs", df.count())
    return df 

# processing
def extract_application_timeline(df):
    '''
    from raw logs extract:
        cluster_id, application_id, app_number, start_time, end_time
    '''

    # filepath and application/cluster ids
    df = (
        df.withColumn("file_path", input_file_name())
          .withColumn(
              "application_id",
              regexp_extract(col("file_path"), r"(application_\d+_\d+)", 1),
          )
          .withColumn(
              "cluster_id",
              regexp_extract(col("application_id"), r"application_(\d+)_", 1),
          )
    )

    # extract timestamp substring from logline
    df = df.withColumn(
        "timestamp_str",
        regexp_extract(col("value"), r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1),
    )

    # timestamp parsing
    df = df.withColumn(
        "timestamp",
        F.coalesce(
            F.expr("try_to_timestamp(timestamp_str, 'yy/MM/dd HH:mm:ss')"),
            F.expr("try_to_timestamp(timestamp_str, 'yyyy-MM-dd HH:mm:ss')"),
            F.expr("try_to_timestamp(timestamp_str, 'MM/dd/yyyy HH:mm:ss')")
        ),
    )

    # filter out rows missing fields
    df = df.where(
        (col("application_id") != "") &
        (col("cluster_id") != "") &
        (col("timestamp").isNotNull())
    )

    logger.info(
        "filtered to %d rows with application_id, cluster_id, and timestamp",
        df.count(),
    )

    # agg start/end times (cluster_id, application_id)
    apps = (
        df.groupBy("cluster_id", "application_id")
          .agg(
              F.min("timestamp").alias("start_time"),
              F.max("timestamp").alias("end_time"),
          )
          .withColumn(
              "app_number",
              F.lpad(F.split("application_id", "_").getItem(2), 4, "0"),
          )
          .orderBy("cluster_id", "app_number")
    )

    logger.info("extracted %d application timelines", apps.count())
    return apps

def write_timeline_csv(apps_df):
    '''
    write problem2_timeline.csv: cluster_id, application_id, app_number, start_time, end_time
    '''

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    timeline = (
        apps_df.select(
            "cluster_id",
            "application_id",
            "app_number",
            "start_time",
            "end_time",
        )
        .orderBy("cluster_id", "start_time")
    )

    timeline.toPandas().to_csv(TIMELINE_PATH, index = False)
    logger.info("wrote application timeline to %s", TIMELINE_PATH)

def write_cluster_summary_and_stats(apps_df):
    '''
    write problem2_cluster_summary.csv and problem2_stats.txt
    '''

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # cluster level agg
    cluster_summary = (
        apps_df.groupBy("cluster_id")
        .agg(
            F.count("*").alias("num_applications"),
            F.min("start_time").alias("cluster_first_app"),
            F.max("end_time").alias("cluster_last_app"),
        )
        .orderBy(F.desc("num_applications"))
    )

    cluster_summary_pd = cluster_summary.toPandas()
    cluster_summary_pd.to_csv(CLUSTER_SUMMARY_PATH, index = False)
    logger.info("wrote cluster summary to %s", CLUSTER_SUMMARY_PATH)

    # overall stats
    num_clusters = cluster_summary_pd.shape[0]
    total_apps = apps_df.count()
    avg_apps_per_cluster = total_apps / num_clusters if num_clusters > 0 else 0.0

    # top clusters
    cluster_summary_pd_sorted = cluster_summary_pd.sort_values(
        "num_applications", ascending = False
    )

    lines = [
        f"Total unique clusters: {num_clusters}",
        f"Total applications: {total_apps}",
        f"Average applications per cluster: {avg_apps_per_cluster:.2f}",
        "",
        "Most heavily used clusters:",
    ]

    for _, row in cluster_summary_pd_sorted.iterrows():
        lines.append(
            f" Cluster {row['cluster_id']}: {int(row['num_applications'])} applications"
        )

    with open(STATS_PATH, "w") as f:
        f.write("\n".join(lines))

    logger.info("wrote overall stats to %s", STATS_PATH)

# visualization helpers
def generate_bar_chart(cluster_summary_pd: pd.DataFrame):
    '''
    bar chart of applications per cluster w value labels, problem2_bar_chart.png
    '''
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # sort clusters by num_applications desc
    cluster_summary_pd = cluster_summary_pd.sort_values(
        "num_applications", ascending=False
    )

    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(10, 6))

    ax = sns.barplot(
        data=cluster_summary_pd,
        x="cluster_id",
        y="num_applications",
    )

    # ad value labels on top of bars
    for p in ax.patches:
        height = p.get_height()
        ax.annotate(
            f"{int(height)}",
            (p.get_x() + p.get_width() / 2.0, height),
            ha="center",
            va="bottom",
            fontsize=9,
        )

    ax.set_xlabel("Cluster ID")
    ax.set_ylabel("Number of Applications")
    ax.set_title("Applications per Cluster")

    plt.tight_layout()
    plt.savefig(BAR_CHART_PATH)
    plt.close()
    logger.info("saved bar chart to %s", BAR_CHART_PATH)

def generate_density_plot(timeline_pd: pd.DataFrame, cluster_summary_pd: pd.DataFrame):
    '''
    density plot of job durations (secs) for largest cluster
    histogram + kde, log scale on x axis
    problem2_density_plot.png
    '''
    timeline_pd["cluster_id"] = timeline_pd["cluster_id"].astype(str)
    cluster_summary_pd["cluster_id"] = cluster_summary_pd["cluster_id"].astype(str)

    timeline_pd["start_time"] = pd.to_datetime(timeline_pd["start_time"], errors="coerce")
    timeline_pd["end_time"] = pd.to_datetime(timeline_pd["end_time"], errors="coerce")

    # duration in secs
    timeline_pd["duration_sec"] = (
        timeline_pd["end_time"] - timeline_pd["start_time"]
    ).dt.total_seconds()

    # largest cluster
    top_cluster = (
        cluster_summary_pd.sort_values("num_applications", ascending=False)
        .iloc[0]["cluster_id"]
    )

    # filter for cluster
    cluster_df = timeline_pd[timeline_pd["cluster_id"] == top_cluster].copy()

    # keep pos durations
    cluster_df = cluster_df[
        cluster_df["duration_sec"].notna() & (cluster_df["duration_sec"] > 0)
    ]

    # ff empty, warn and exit
    if cluster_df.empty:
        logger.warning(
            f"No valid durations for largest cluster {top_cluster}; skipping density plot."
        )
        return

    n = len(cluster_df)

    # plot
    plt.figure(figsize=(8, 5))
    sns.histplot(cluster_df["duration_sec"], bins=30, kde=True, color="royalblue")
    plt.xscale("log")
    plt.xlabel("Job Duration (seconds, log scale)")
    plt.ylabel("Frequency")
    plt.title(f"Duration Distribution – Cluster {top_cluster} (n={n})")
    plt.tight_layout()

    out_path = "/home/ubuntu/data/output/problem2_density_plot.png"
    plt.savefig(out_path)
    plt.close()

    logger.info(f"saved density plot to {out_path}")

# main
def main() -> int:
    args = parse_args()
    if args.skip_spark:
        # skip spark, generate vis only
        logger.info("running in --skipe-spark mode")

        if not (os.path.exists(TIMELINE_PATH) and os.path.exists(CLUSTER_SUMMARY_PATH)):
            logger.error(
                "timeline or cluster summary csv not found in %s, make sure processing run first",
                OUTPUT_DIR
            )
            return 1
        
        timeline_pd = pd.read_csv(TIMELINE_PATH)
        cluster_summary_pd = pd.read_csv(CLUSTER_SUMMARY_PATH)

        generate_bar_chart(cluster_summary_pd)
        generate_density_plot(timeline_pd, cluster_summary_pd)

        return 0
    
    # full spark
    if args.master_url:
        master_url = args.master_url
    else:
        master_private_ip = os.getenv("MASTER_PRIVATE_IP")
        if master_private_ip:
            master_url = f"spark://{master_private_ip}:7077"
        else:
            print("ERROR: master url not provided")
            return 1
    
    if args.net_id:
        logger.info("net ID: %s", args.net_id)

    logger.info("initializing spark session with master %s", master_url)
    spark = create_spark_session(master_url)
    success = False

    try:
        raw_df = read_logs_from_s3(spark)

        logger.info("extracting application timelines")
        apps_df = extract_application_timeline(raw_df)

        logger.info("writing timeline csv")
        write_timeline_csv(apps_df)

        logger.info("writing cluster summary and stats")
        write_cluster_summary_and_stats(apps_df)

        logger.info("generating visualizations from csvs")
        timeline_pd = pd.read_csv(TIMELINE_PATH)
        cluster_summary_pd = pd.read_csv(CLUSTER_SUMMARY_PATH)
        generate_bar_chart(cluster_summary_pd)
        generate_density_plot(timeline_pd, cluster_summary_pd)

        success = True
    
    except Exception as e:
        logger.exception("error during problem 2 processing: %s", str(e))
        success = False
    finally:
        logger.info("stopping spark session")
        spark.stop()

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
    