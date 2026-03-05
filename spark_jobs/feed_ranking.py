"""
Feed Ranking Algorithm (Distributed)
======================================
Computes the final personalized feed by combining multiple signals.

Feed Score Formula:
    FeedScore = 0.5 × InteractionScore + 0.3 × Popularity + 0.2 × Recency

Where:
    - InteractionScore: How much the user interacted with this post (from scoring)
    - Popularity: How popular the post is overall (total interactions from all users)
    - Recency: How recent the post is (higher for newer posts)

Distributed Processing:
    - Multiple distributed JOINs to combine data from different sources
    - Window functions for per-user ranking across partitions
    - Normalized scoring using distributed min/max aggregations

Output:
    results/feeds/user_feed.csv — columns: user_id, post_id, feed_score
    (Top 10 posts per user)
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.spark_session import create_spark_session
from utils.helpers import (
    POSTS_CSV, INTERACTIONS_CSV, USER_POST_SCORES_CSV,
    COLLAB_RECOMMENDATIONS_CSV, USER_FEED_CSV,
    W_INTERACTION, W_POPULARITY, W_RECENCY, TOP_N_FEED,
    load_csv, save_csv, ensure_directories
)


def compute_popularity_scores(spark):
    """
    Compute popularity score for each post.

    Popularity = total number of interactions across all users.
    Normalized to [0, 1] range using min-max scaling.

    This uses a distributed groupBy + count operation across all 4 nodes.
    """
    print("\n[1/6] Computing post popularity scores (distributed aggregation)...")

    interactions_df = load_csv(spark, INTERACTIONS_CSV)

    # -----------------------------------------------------------------------
    # DISTRIBUTED AGGREGATION: Count interactions per post across all nodes
    # -----------------------------------------------------------------------
    popularity = (
        interactions_df
        .groupBy("post_id")
        .agg(F.count("*").alias("raw_popularity"))
    )

    # -----------------------------------------------------------------------
    # NORMALIZE: Min-max scaling to [0, 1] (distributed min/max computation)
    # -----------------------------------------------------------------------
    stats = popularity.agg(
        F.min("raw_popularity").alias("min_pop"),
        F.max("raw_popularity").alias("max_pop")
    ).collect()[0]

    min_pop = stats["min_pop"]
    max_pop = stats["max_pop"]
    pop_range = max_pop - min_pop if max_pop != min_pop else 1

    popularity = popularity.withColumn(
        "popularity_score",
        F.round((F.col("raw_popularity") - min_pop) / pop_range, 4)
    )

    print(f"      Popularity range: {min_pop} to {max_pop}")
    return popularity


def compute_recency_scores(spark):
    """
    Compute recency score for each post.

    Recency is based on the number of days since the post was created.
    Newer posts get higher scores. Normalized to [0, 1].

    Uses Spark's datediff function for distributed date arithmetic.
    """
    print("\n[2/6] Computing post recency scores (distributed date computation)...")

    posts_df = load_csv(spark, POSTS_CSV)

    # -----------------------------------------------------------------------
    # DISTRIBUTED DATE COMPUTATION: Calculate days since post creation
    # Each node processes its partition of posts independently.
    # -----------------------------------------------------------------------
    reference_date = "2025-12-31"  # Reference date for recency calculation

    recency = (
        posts_df
        .withColumn(
            "days_old",
            F.datediff(F.lit(reference_date), F.col("timestamp"))
        )
    )

    # Normalize: newer posts (fewer days old) get HIGHER scores
    stats = recency.agg(
        F.min("days_old").alias("min_days"),
        F.max("days_old").alias("max_days")
    ).collect()[0]

    min_days = stats["min_days"]
    max_days = stats["max_days"]
    days_range = max_days - min_days if max_days != min_days else 1

    recency = recency.withColumn(
        "recency_score",
        F.round(1.0 - (F.col("days_old") - min_days) / days_range, 4)
    ).select("post_id", "topic", "recency_score")

    return recency


def get_user_interaction_scores(spark):
    """
    Load or compute user-post interaction scores.
    
    These are the per-user personalized scores from the interaction scoring job.
    Normalized to [0, 1] range.
    """
    print("\n[3/6] Loading user interaction scores...")

    try:
        scores_df = load_csv(spark, USER_POST_SCORES_CSV)
        print("      Loaded precomputed interaction scores.")
    except Exception:
        print("      Running interaction scoring first...")
        from spark_jobs.interaction_scoring import run_interaction_scoring
        scores_df = run_interaction_scoring(spark)

    # Normalize interaction scores per user (each user's max score → 1.0)
    window_spec = Window.partitionBy("user_id")

    scores_df = (
        scores_df
        .withColumn("max_score", F.max("total_score").over(window_spec))
        .withColumn(
            "interaction_score",
            F.round(F.col("total_score") / F.col("max_score"), 4)
        )
        .select("user_id", "post_id", "interaction_score")
    )

    return scores_df


def get_collaborative_recommendations(spark):
    """
    Load collaborative filtering recommendations if available.
    These are used to add posts the user hasn't seen but similar users liked.
    """
    print("\n[4/6] Loading collaborative filtering recommendations...")

    try:
        collab_df = load_csv(spark, COLLAB_RECOMMENDATIONS_CSV)
        print(f"      Loaded {collab_df.count()} collaborative recommendations.")
        return collab_df
    except Exception:
        print("      No collaborative recommendations found. Skipping.")
        return None


def compute_final_feed(spark, interaction_scores, popularity, recency, collab_recs):
    """
    Compute the final feed ranking for all users.

    FeedScore = 0.5 × InteractionScore + 0.3 × Popularity + 0.2 × Recency

    Uses multiple distributed JOINs and Window functions:
        1. JOIN interaction scores with popularity (distributed)
        2. JOIN with recency scores (distributed)
        3. UNION with collaborative recommendations (distributed)
        4. Window ranking: partition by user_id, order by feed_score DESC
        5. Select top N posts per user
    """
    print(f"\n[5/6] Computing final feed scores (distributed joins + Window ranking)...")
    print(f"      Formula: FeedScore = {W_INTERACTION}×Interaction + {W_POPULARITY}×Popularity + {W_RECENCY}×Recency")

    # -----------------------------------------------------------------------
    # DISTRIBUTED JOIN 1: Combine interaction scores with popularity
    # Data is shuffled across 4 nodes to co-locate matching post_ids.
    # -----------------------------------------------------------------------
    feed = (
        interaction_scores
        .join(
            popularity.select("post_id", "popularity_score"),
            "post_id",
            "left"
        )
        .fillna(0, subset=["popularity_score"])
    )

    # -----------------------------------------------------------------------
    # DISTRIBUTED JOIN 2: Combine with recency scores
    # Another shuffle operation distributes data by post_id.
    # -----------------------------------------------------------------------
    feed = (
        feed
        .join(
            recency.select("post_id", "recency_score"),
            "post_id",
            "left"
        )
        .fillna(0, subset=["recency_score"])
    )

    # -----------------------------------------------------------------------
    # If collaborative recommendations exist, add them as bonus candidates
    # -----------------------------------------------------------------------
    if collab_recs is not None:
        # Add collaborative recommendation posts with a base interaction score
        collab_posts = (
            collab_recs
            .select(
                F.col("user_id"),
                F.col("recommended_post_id").alias("post_id"),
                F.lit(0.5).alias("interaction_score")  # Base score for recommended posts
            )
            .join(
                popularity.select("post_id", "popularity_score"),
                "post_id", "left"
            )
            .fillna(0, subset=["popularity_score"])
            .join(
                recency.select("post_id", "recency_score"),
                "post_id", "left"
            )
            .fillna(0, subset=["recency_score"])
        )

        # Union direct interactions with collaborative recommendations
        feed = feed.union(collab_posts)

    # -----------------------------------------------------------------------
    # COMPUTE FEED SCORE: Weighted combination of all signals
    # This computation runs in parallel across all 4 nodes.
    # -----------------------------------------------------------------------
    feed = feed.withColumn(
        "feed_score",
        F.round(
            W_INTERACTION * F.col("interaction_score") +
            W_POPULARITY * F.col("popularity_score") +
            W_RECENCY * F.col("recency_score"),
            4
        )
    )

    # If same user-post appears from multiple sources, keep the max score
    feed = (
        feed
        .groupBy("user_id", "post_id")
        .agg(F.max("feed_score").alias("feed_score"))
    )

    # -----------------------------------------------------------------------
    # WINDOW RANKING: Select top N posts per user
    # Window partitions by user_id — each partition processed on a separate node.
    # row_number() ranks posts within each user's partition.
    # -----------------------------------------------------------------------
    print(f"      Selecting top {TOP_N_FEED} posts per user using Window ranking...")

    window_spec = (
        Window
        .partitionBy("user_id")
        .orderBy(F.desc("feed_score"))
    )

    final_feed = (
        feed
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") <= TOP_N_FEED)
        .select("user_id", "post_id", "feed_score")
        .orderBy("user_id", F.desc("feed_score"))
    )

    return final_feed


def run_feed_ranking(spark):
    """Execute the full feed ranking pipeline."""
    print("=" * 60)
    print("  FEED RANKING - Distributed Pipeline")
    print("=" * 60)

    # Compute all component scores
    popularity = compute_popularity_scores(spark)
    recency = compute_recency_scores(spark)
    interaction_scores = get_user_interaction_scores(spark)
    collab_recs = get_collaborative_recommendations(spark)

    # Compute final feed
    final_feed = compute_final_feed(
        spark, interaction_scores, popularity, recency, collab_recs
    )

    # Statistics
    total_entries = final_feed.count()
    total_users = final_feed.select("user_id").distinct().count()
    print(f"\n      Generated feeds for {total_users} users ({total_entries} total entries)")

    print("\n      Sample feed (first 3 users):")
    final_feed.filter(F.col("user_id").isin([1, 2, 3])).show(30, truncate=False)

    # Save results
    print("[6/6] Saving final user feeds...")
    ensure_directories()
    save_csv(final_feed, USER_FEED_CSV)

    print("\n[✓] Feed Ranking complete!")
    return final_feed


def main():
    """Entry point for spark-submit."""
    spark = create_spark_session("FeedRanking")
    try:
        run_feed_ranking(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
