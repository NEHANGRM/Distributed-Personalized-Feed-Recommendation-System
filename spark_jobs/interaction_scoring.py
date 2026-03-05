"""
Interaction Scoring Algorithm (MapReduce-Style)
================================================
Converts raw user interactions into numerical scores using a weighted mapping.

MapReduce Logic:
    MAP Phase:
        (user_id, post_id, action) → (user_id, post_id, score)
        Each interaction action is mapped to a numerical weight:
            view=1, like=3, comment=4, share=5

    REDUCE Phase:
        Aggregate scores per (user_id, post_id) pair using groupBy + sum.
        This computes the total interaction intensity between each user-post pair.

Distributed Processing:
    - Uses Spark DataFrames distributed across 4 worker threads (nodes).
    - The map operation runs in parallel across all partitions.
    - The groupBy + agg triggers a shuffle operation, redistributing data
      across nodes to compute aggregated scores.

Output:
    results/user_post_scores.csv — columns: user_id, post_id, total_score
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from utils.spark_session import create_spark_session
from utils.helpers import (
    ACTION_WEIGHTS, INTERACTIONS_CSV,
    USER_POST_SCORES_CSV, load_csv, save_csv, ensure_directories
)


def map_action_to_score(action_col):
    """
    MAP PHASE: Convert action strings to numerical scores.
    
    This creates a Spark Column expression that maps each action
    to its corresponding weight using a CASE/WHEN construct.
    Equivalent to the MAP step in MapReduce:
        (user_id, post_id, action) → (user_id, post_id, score)
    """
    mapping_expr = F.lit(0)  # Default score for unknown actions
    for action, weight in ACTION_WEIGHTS.items():
        mapping_expr = F.when(action_col == action, weight).otherwise(mapping_expr)
    return mapping_expr


def run_interaction_scoring(spark):
    """
    Execute the full interaction scoring pipeline.
    
    Steps:
        1. Load interactions data (distributed across 4 partitions)
        2. MAP: Convert actions to scores in parallel
        3. REDUCE: GroupBy (user_id, post_id) and aggregate scores
        4. Save results
    """
    print("=" * 60)
    print("  INTERACTION SCORING - MapReduce Pipeline")
    print("=" * 60)
    
    # Load interactions dataset — Spark distributes this across 4 partitions
    print("\n[1/4] Loading interactions data (distributed across 4 nodes)...")
    interactions_df = load_csv(spark, INTERACTIONS_CSV)
    
    total_records = interactions_df.count()
    num_partitions = interactions_df.rdd.getNumPartitions()
    print(f"      Loaded {total_records} interactions across {num_partitions} partitions")
    
    # -----------------------------------------------------------------------
    # MAP PHASE: Transform each interaction into a score
    # This operation runs in parallel across all 4 partitions/nodes.
    # Each partition independently maps action → score without communication.
    # -----------------------------------------------------------------------
    print("\n[2/4] MAP Phase: Mapping actions to scores across all nodes...")
    scored_df = interactions_df.withColumn(
        "score",
        map_action_to_score(F.col("action")).cast(IntegerType())
    )
    
    print("      Sample mapped records:")
    scored_df.show(5, truncate=False)
    
    # -----------------------------------------------------------------------
    # REDUCE PHASE: Aggregate scores per (user_id, post_id) pair
    # This triggers a SHUFFLE operation — data with the same key is sent
    # to the same node, then aggregated using SUM.
    # Equivalent to reduceByKey in classic MapReduce.
    # -----------------------------------------------------------------------
    print("[3/4] REDUCE Phase: Aggregating scores per (user_id, post_id)...")
    user_post_scores = (
        scored_df
        .groupBy("user_id", "post_id")           # Group by composite key
        .agg(F.sum("score").alias("total_score"))  # Reduce: sum scores
        .orderBy("user_id", F.desc("total_score")) # Sort for readability
    )
    
    result_count = user_post_scores.count()
    print(f"      Reduced {total_records} interactions → {result_count} user-post score pairs")
    
    print("\n      Top scored user-post pairs:")
    user_post_scores.show(10, truncate=False)
    
    # -----------------------------------------------------------------------
    # Save results
    # -----------------------------------------------------------------------
    print("[4/4] Saving user-post scores...")
    ensure_directories()
    save_csv(user_post_scores, USER_POST_SCORES_CSV)
    
    print("\n[✓] Interaction Scoring complete!")
    return user_post_scores


def main():
    """Entry point for spark-submit."""
    spark = create_spark_session("InteractionScoring")
    try:
        run_interaction_scoring(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
