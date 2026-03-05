"""
Collaborative Filtering Algorithm (Distributed)
=================================================
Recommends posts to users based on similarity with other users.

Algorithm:
    1. Build user-post interaction matrix from scored interactions
    2. Compute cosine similarity between all user pairs (distributed cross-join)
    3. For each user, find top-K most similar users
    4. Recommend posts that similar users liked but the target user hasn't seen

Distributed Processing:
    - The user-post matrix is built using Spark groupBy + pivot (distributed)
    - Cosine similarity uses a cross-join which distributes pair comparisons
      across all 4 nodes in the cluster
    - Top-K selection uses Spark Window functions for distributed ranking
    - Post recommendation uses distributed anti-join operations

Output:
    results/collaborative_recommendations.csv — columns: user_id, recommended_post_id, similarity_score
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

from utils.spark_session import create_spark_session
from utils.helpers import (
    INTERACTIONS_CSV, USER_POST_SCORES_CSV,
    COLLAB_RECOMMENDATIONS_CSV, TOP_K_SIMILAR_USERS,
    load_csv, save_csv, ensure_directories
)


def build_user_post_matrix(spark):
    """
    Build user-post interaction matrix.
    
    This creates a sparse matrix where rows=users, columns=posts,
    and values=interaction scores. Uses Spark's distributed groupBy
    and pivot operations across all 4 nodes.
    """
    print("\n[1/5] Building user-post interaction matrix (distributed)...")
    
    # Try loading precomputed scores first, fallback to raw interactions
    try:
        import pandas as pd
        scores_pdf = pd.read_csv(USER_POST_SCORES_CSV)
        scores_df = spark.createDataFrame(scores_pdf)
        print("      Loaded precomputed interaction scores (Driver).")
    except Exception:
        print("      Precomputed scores not found. Computing from raw interactions...")
        from spark_jobs.interaction_scoring import run_interaction_scoring
        scores_df = run_interaction_scoring(spark)
    
    # Create user-post matrix: each row is a user, columns are post scores
    # This groupBy operation distributes computation across 4 nodes
    user_post_matrix = (
        scores_df
        .select("user_id", "post_id", "total_score")
    )
    
    num_users = user_post_matrix.select("user_id").distinct().count()
    num_posts = user_post_matrix.select("post_id").distinct().count()
    print(f"      Matrix dimensions: {num_users} users × {num_posts} posts")
    
    return user_post_matrix


def compute_cosine_similarity(user_post_matrix):
    """
    Compute cosine similarity between all pairs of users.
    
    Cosine Similarity = (A · B) / (||A|| × ||B||)
    
    This is computed using:
        1. Self-join on post_id to find shared interactions (distributed)
        2. Dot product via multiplication and sum
        3. Vector magnitudes via squared sum and sqrt
    
    The cross-join is distributed across all 4 nodes for parallel computation.
    """
    print("\n[2/5] Computing cosine similarity between users (distributed cross-join)...")
    
    # Alias the matrix for self-join
    matrix_a = user_post_matrix.alias("a")
    matrix_b = user_post_matrix.alias("b")
    
    # -----------------------------------------------------------------------
    # DISTRIBUTED JOIN: Find all user pairs who interacted with the same posts
    # This join is distributed across 4 nodes — each node processes a subset
    # of the post_id keys.
    # -----------------------------------------------------------------------
    paired = (
        matrix_a.join(
            matrix_b,
            (F.col("a.post_id") == F.col("b.post_id")) &
            (F.col("a.user_id") < F.col("b.user_id")),  # Avoid duplicate & self pairs
            "inner"
        )
        .select(
            F.col("a.user_id").alias("user_a"),
            F.col("b.user_id").alias("user_b"),
            F.col("a.total_score").alias("score_a"),
            F.col("b.total_score").alias("score_b"),
        )
    )
    
    # -----------------------------------------------------------------------
    # Compute dot product: sum(score_a * score_b) for each user pair
    # This aggregation is distributed using groupBy across nodes.
    # -----------------------------------------------------------------------
    dot_product = (
        paired
        .withColumn("product", F.col("score_a") * F.col("score_b"))
        .groupBy("user_a", "user_b")
        .agg(F.sum("product").alias("dot_product"))
    )
    
    # -----------------------------------------------------------------------
    # Compute vector magnitudes: ||A|| = sqrt(sum(a^2))
    # Each node computes magnitudes for its partition of users.
    # -----------------------------------------------------------------------
    magnitudes = (
        user_post_matrix
        .withColumn("score_sq", F.col("total_score") ** 2)
        .groupBy("user_id")
        .agg(F.sqrt(F.sum("score_sq")).alias("magnitude"))
    )
    
    # -----------------------------------------------------------------------
    # Final cosine similarity = dot_product / (magnitude_a * magnitude_b)
    # Uses two distributed joins to attach magnitudes.
    # -----------------------------------------------------------------------
    similarity = (
        dot_product
        .join(magnitudes.alias("mag_a"),
              F.col("user_a") == F.col("mag_a.user_id"), "inner")
        .join(magnitudes.alias("mag_b"),
              F.col("user_b") == F.col("mag_b.user_id"), "inner")
        .withColumn(
            "cosine_similarity",
            (F.col("dot_product") /
             (F.col("mag_a.magnitude") * F.col("mag_b.magnitude"))
            ).cast(DoubleType())
        )
        .select("user_a", "user_b", "cosine_similarity")
    )
    
    print("      Sample similarity scores:")
    similarity.orderBy(F.desc("cosine_similarity")).show(10, truncate=False)
    
    return similarity


def find_top_similar_users(similarity_df, top_k=TOP_K_SIMILAR_USERS):
    """
    For each user, find the top-K most similar users.
    
    Uses Spark Window functions for distributed ranking:
        - PARTITION BY user_id distributes ranking across nodes
        - ORDER BY cosine_similarity DESC ranks within each partition
        - ROW_NUMBER selects top-K per user
    """
    print(f"\n[3/5] Finding top-{top_k} similar users per user (Window ranking)...")
    
    # Make similarity bidirectional (a→b and b→a)
    sim_bidirectional = (
        similarity_df.select(
            F.col("user_a").alias("user_id"),
            F.col("user_b").alias("similar_user"),
            F.col("cosine_similarity")
        ).union(
            similarity_df.select(
                F.col("user_b").alias("user_id"),
                F.col("user_a").alias("similar_user"),
                F.col("cosine_similarity")
            )
        )
    )
    
    # -----------------------------------------------------------------------
    # WINDOW RANKING: Distributed rank computation
    # Each partition (user_id) is processed independently on different nodes.
    # -----------------------------------------------------------------------
    window_spec = Window.partitionBy("user_id").orderBy(F.desc("cosine_similarity"))
    
    top_similar = (
        sim_bidirectional
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") <= top_k)
        .drop("rank")
    )
    
    print(f"      Found similar users for {top_similar.select('user_id').distinct().count()} users")
    return top_similar


def recommend_posts(spark, top_similar_users, user_post_matrix):
    """
    Recommend posts that similar users interacted with but the target user hasn't.
    
    Uses distributed anti-join:
        1. JOIN similar users' posts (what similar users liked)
        2. LEFT ANTI JOIN with target user's posts (exclude already-seen posts)
    """
    print("\n[4/5] Generating recommendations (distributed anti-join)...")
    
    # -----------------------------------------------------------------------
    # DISTRIBUTED JOIN: Get posts interacted by similar users
    # This join distributes post lookup across nodes.
    # -----------------------------------------------------------------------
    similar_user_posts = (
        top_similar_users
        .join(
            user_post_matrix.select(
                F.col("user_id").alias("sim_user"),
                F.col("post_id").alias("recommended_post_id"),
                F.col("total_score").alias("sim_user_score")
            ),
            F.col("similar_user") == F.col("sim_user"),
            "inner"
        )
        .select("user_id", "recommended_post_id", "cosine_similarity", "sim_user_score")
    )
    
    # -----------------------------------------------------------------------
    # DISTRIBUTED ANTI-JOIN: Remove posts the user already interacted with
    # Each node checks its partition for already-seen posts.
    # -----------------------------------------------------------------------
    already_seen = user_post_matrix.select(
        F.col("user_id").alias("seen_user"),
        F.col("post_id").alias("seen_post")
    )
    
    recommendations = (
        similar_user_posts
        .join(
            already_seen,
            (F.col("user_id") == F.col("seen_user")) &
            (F.col("recommended_post_id") == F.col("seen_post")),
            "left_anti"
        )
    )
    
    # Aggregate: if multiple similar users recommend the same post, sum the scores
    final_recommendations = (
        recommendations
        .groupBy("user_id", "recommended_post_id")
        .agg(
            F.round(F.avg("cosine_similarity"), 4).alias("avg_similarity"),
            F.sum("sim_user_score").alias("recommendation_strength")
        )
        .orderBy("user_id", F.desc("recommendation_strength"))
    )
    
    rec_count = final_recommendations.count()
    print(f"      Generated {rec_count} total recommendations")
    
    return final_recommendations


def run_collaborative_filtering(spark):
    """Execute the full collaborative filtering pipeline."""
    print("=" * 60)
    print("  COLLABORATIVE FILTERING - Distributed Pipeline")
    print("=" * 60)
    
    # Step 1: Build interaction matrix
    user_post_matrix = build_user_post_matrix(spark)
    
    # Step 2: Compute cosine similarity (distributed)
    similarity_df = compute_cosine_similarity(user_post_matrix)
    
    # Step 3: Find top-K similar users (Window ranking)
    top_similar = find_top_similar_users(similarity_df)
    
    # Step 4: Generate recommendations (anti-join)
    recommendations = recommend_posts(spark, top_similar, user_post_matrix)
    
    # Step 5: Save results (Driver-side collect)
    print("\n[5/5] Saving collaborative filtering recommendations (Driver)...")
    ensure_directories()
    results_pdf = recommendations.toPandas()
    results_pdf.to_csv(COLLAB_RECOMMENDATIONS_CSV, index=False)
    
    print("\n      Top recommendations:")
    recommendations.show(15, truncate=False)
    
    print("\n[OK] Collaborative Filtering complete!")
    return recommendations


def main():
    """Entry point for spark-submit."""
    spark = create_spark_session("CollaborativeFiltering")
    try:
        run_collaborative_filtering(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
