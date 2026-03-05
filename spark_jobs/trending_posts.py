"""
Trending Posts Detection (MapReduce-Style)
==========================================
Identifies trending posts by counting total interactions per post.

MapReduce Logic:
    MAP Phase:
        (post_id, action) → (post_id, 1)
        Each interaction is mapped to a count of 1.

    REDUCE Phase:
        (post_id → total_interactions)
        Aggregate counts per post using groupBy + count.

    SORT:
        Sort descending to find top trending posts.

Distributed Processing:
    - MAP runs independently on each of the 4 partitions/nodes
    - REDUCE triggers a shuffle to co-locate all records for each post_id
    - RDD-level reduceByKey is used alongside DataFrame API to demonstrate
      both classic MapReduce and modern Spark patterns

Output:
    results/trending_posts.csv — columns: post_id, total_interactions, topic
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F

from utils.spark_session import create_spark_session
from utils.helpers import (
    INTERACTIONS_CSV, POSTS_CSV, TRENDING_POSTS_CSV,
    load_csv, save_csv, ensure_directories
)


def run_trending_detection_rdd(spark):
    """
    Classic MapReduce approach using RDD API.
    
    This demonstrates the traditional MapReduce paradigm:
        - map: transform each record to (key, value)
        - reduceByKey: aggregate values by key across all nodes
    """
    print("\n  --- RDD-Based MapReduce Approach ---")
    
    interactions_df = load_csv(spark, INTERACTIONS_CSV)
    
    # Convert DataFrame to RDD for classic MapReduce operations
    interactions_rdd = interactions_df.rdd
    
    num_partitions = interactions_rdd.getNumPartitions()
    print(f"      Data distributed across {num_partitions} partitions")
    
    # -----------------------------------------------------------------------
    # MAP PHASE (RDD): Each record → (post_id, 1)
    # This runs in parallel on all 4 nodes without any data shuffling.
    # -----------------------------------------------------------------------
    mapped_rdd = interactions_rdd.map(
        lambda row: (row["post_id"], 1)  # Emit (post_id, count=1) for each interaction
    )
    
    # -----------------------------------------------------------------------
    # REDUCE PHASE (RDD): reduceByKey — aggregate counts per post_id
    # Spark shuffles data so all records with the same post_id are on the
    # same node, then reduces them using the addition function.
    # This is the classic MapReduce reduceByKey operation.
    # -----------------------------------------------------------------------
    reduced_rdd = mapped_rdd.reduceByKey(
        lambda a, b: a + b  # Sum up counts for each post_id
    )
    
    # Sort by count descending (Spark sorts across all partitions)
    sorted_rdd = reduced_rdd.sortBy(lambda x: x[1], ascending=False)
    
    # Show top results from RDD approach
    print("      Top 10 trending posts (RDD MapReduce):")
    for post_id, count in sorted_rdd.take(10):
        print(f"        Post {post_id}: {count} interactions")
    
    return sorted_rdd


def run_trending_detection_df(spark):
    """
    Modern DataFrame approach — functionally equivalent but optimized
    by Spark's Catalyst optimizer and Tungsten execution engine.
    
    This approach is preferred in production as Spark can optimize the
    execution plan across the 4-node cluster.
    """
    print("\n  --- DataFrame API Approach ---")
    
    # Load datasets
    # Load datasets on Driver
    import pandas as pd
    interactions_pdf = pd.read_csv(INTERACTIONS_CSV)
    posts_pdf = pd.read_csv(POSTS_CSV)
    interactions_df = spark.createDataFrame(interactions_pdf)
    posts_df = spark.createDataFrame(posts_pdf)
    
    # -----------------------------------------------------------------------
    # MAP + REDUCE (DataFrame): groupBy + count
    # Spark automatically maps and reduces across all 4 partitions.
    # The Catalyst optimizer may combine the map and reduce steps.
    # -----------------------------------------------------------------------
    trending = (
        interactions_df
        .groupBy("post_id")                                    # Group by post_id (key)
        .agg(
            F.count("*").alias("total_interactions"),          # Count interactions (reduce)
            F.countDistinct("user_id").alias("unique_users"),  # Bonus: unique user count
            F.sum(
                F.when(F.col("action") == "like", 1).otherwise(0)
            ).alias("total_likes"),                             # Count likes specifically
        )
        .orderBy(F.desc("total_interactions"))                  # Sort descending
    )
    
    # -----------------------------------------------------------------------
    # DISTRIBUTED JOIN: Enrich with post metadata
    # This join is distributed — each node joins its partition with posts data.
    # -----------------------------------------------------------------------
    trending_enriched = (
        trending
        .join(posts_df.select("post_id", "topic", "creator_id", "timestamp"), "post_id", "inner")
        .orderBy(F.desc("total_interactions"))
    )
    
    print("      Top 20 trending posts (DataFrame API):")
    trending_enriched.show(20, truncate=False)
    
    return trending_enriched


def run_trending_posts(spark):
    """Execute both trending detection approaches."""
    print("=" * 60)
    print("  TRENDING POSTS DETECTION - MapReduce Pipeline")
    print("=" * 60)
    
    # Approach 1: Classic RDD MapReduce (for demonstration)
    run_trending_detection_rdd(spark)
    
    # Approach 2: Modern DataFrame API (for the final output)
    trending_df = run_trending_detection_df(spark)
    
    # Save results (Driver-side collect)
    print("\n[*] Saving trending posts (Driver)...")
    ensure_directories()
    results_pdf = trending_df.toPandas()
    results_pdf.to_csv(TRENDING_POSTS_CSV, index=False)
    
    print("\n[OK] Trending Posts Detection complete!")
    return trending_df


def main():
    """Entry point for spark-submit."""
    spark = create_spark_session("TrendingPosts")
    try:
        run_trending_posts(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
