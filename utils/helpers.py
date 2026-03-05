"""
Helper Utilities
================
Constants, weight maps, and shared helper functions used across all Spark jobs
in the distributed feed recommendation system.
"""

import os

# ---------------------------------------------------------------------------
# Project Paths (relative to the project root: distributed-feed-system/)
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
FEEDS_DIR = os.path.join(RESULTS_DIR, "feeds")

# Input datasets
USERS_CSV = os.path.join(DATA_DIR, "users.csv")
POSTS_CSV = os.path.join(DATA_DIR, "posts.csv")
INTERACTIONS_CSV = os.path.join(DATA_DIR, "interactions.csv")

# Output files
USER_POST_SCORES_CSV = os.path.join(RESULTS_DIR, "user_post_scores.csv")
COLLAB_RECOMMENDATIONS_CSV = os.path.join(RESULTS_DIR, "collaborative_recommendations.csv")
TRENDING_POSTS_CSV = os.path.join(RESULTS_DIR, "trending_posts.csv")
USER_FEED_CSV = os.path.join(FEEDS_DIR, "user_feed.csv")

# ---------------------------------------------------------------------------
# Interaction Scoring Weights (MapReduce scoring weights)
# ---------------------------------------------------------------------------
ACTION_WEIGHTS = {
    "view": 1,
    "like": 3,
    "comment": 4,
    "share": 5,
}

# ---------------------------------------------------------------------------
# Feed Ranking Formula Weights
# FeedScore = W_INTERACTION * InteractionScore
#           + W_POPULARITY * PopularityScore
#           + W_RECENCY   * RecencyScore
# ---------------------------------------------------------------------------
W_INTERACTION = 0.5
W_POPULARITY = 0.3
W_RECENCY = 0.2

# Number of top posts to return per user in the final feed
TOP_N_FEED = 10

# Number of top similar users for collaborative filtering
TOP_K_SIMILAR_USERS = 10

# ---------------------------------------------------------------------------
# Dataset Generation Parameters
# ---------------------------------------------------------------------------
NUM_USERS = 100
NUM_POSTS = 500
NUM_INTERACTIONS = 10000

TOPICS = ["AI", "Sports", "Music", "Technology", "Travel", "Food", "Science",
          "Politics", "Gaming", "Fashion", "Health", "Finance", "Education"]

LOCATIONS = ["India", "USA", "UK", "Germany", "Japan", "Canada", "Australia",
             "France", "Brazil", "South Korea"]

ACTIONS = ["view", "like", "comment", "share"]


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def ensure_directories():
    """Create output directories if they do not exist."""
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(FEEDS_DIR, exist_ok=True)


def load_csv(spark, path, schema=None):
    """
    Load a CSV file into a Spark DataFrame.

    Parameters
    ----------
    spark : SparkSession
    path : str
        Path to the CSV file.
    schema : StructType, optional
        Explicit schema; if None, Spark infers the schema.

    Returns
    -------
    DataFrame
        Spark DataFrame containing the CSV data.
    """
    reader = spark.read.option("header", "true").option("inferSchema", "true")
    if schema:
        reader = reader.schema(schema)
    return reader.csv(path)


def save_csv(df, path, single_file=True):
    """
    Save a Spark DataFrame as a CSV file.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame to save.
    path : str
        Output path.
    single_file : bool
        If True, coalesce to a single partition for a single output file.
    """
    if single_file:
        df = df.coalesce(1)
    df.write.option("header", "true").mode("overwrite").csv(path)
    print(f"[✓] Saved output to: {path}")
