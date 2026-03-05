"""
Synthetic Dataset Generator
============================
Generates synthetic datasets for the distributed feed recommendation system.

Datasets Generated:
    - users.csv   : 100 users with age, location, and interest
    - posts.csv   : 500 posts with topics, creator IDs, and timestamps
    - interactions.csv : 10,000 user-post interactions (view/like/comment/share)

This script uses Python's random module and Pandas for generation,
then saves the outputs as CSV files in the data/ directory.
"""

import sys
import os
import random
import pandas as pd
from datetime import datetime, timedelta

# Add project root to path so we can import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.helpers import (
    DATA_DIR, NUM_USERS, NUM_POSTS, NUM_INTERACTIONS,
    TOPICS, LOCATIONS, ACTIONS, ensure_directories,
    USERS_CSV, POSTS_CSV, INTERACTIONS_CSV
)


def generate_users(num_users=NUM_USERS):
    """
    Generate synthetic user data.
    
    Each user has:
        - user_id: unique integer ID
        - age: random age between 18 and 55
        - location: randomly chosen from predefined locations
        - interest: randomly chosen from predefined topics
    """
    print(f"[*] Generating {num_users} users...")
    users = []
    for uid in range(1, num_users + 1):
        users.append({
            "user_id": uid,
            "age": random.randint(18, 55),
            "location": random.choice(LOCATIONS),
            "interest": random.choice(TOPICS),
        })
    return pd.DataFrame(users)


def generate_posts(num_posts=NUM_POSTS, num_users=NUM_USERS):
    """
    Generate synthetic post data.
    
    Each post has:
        - post_id: unique integer ID starting from 101
        - topic: randomly chosen from predefined topics
        - creator_id: ID of the user who created the post
        - timestamp: random date in the year 2025
    """
    print(f"[*] Generating {num_posts} posts...")
    posts = []
    start_date = datetime(2025, 1, 1)
    for pid in range(101, 101 + num_posts):
        random_days = random.randint(0, 364)
        posts.append({
            "post_id": pid,
            "topic": random.choice(TOPICS),
            "creator_id": random.randint(1, num_users),
            "timestamp": (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d"),
        })
    return pd.DataFrame(posts)


def generate_interactions(num_interactions=NUM_INTERACTIONS,
                          num_users=NUM_USERS, num_posts=NUM_POSTS):
    """
    Generate synthetic interaction data.
    
    Each interaction has:
        - user_id: the user performing the action
        - post_id: the post being interacted with
        - action: one of view, like, comment, share
    
    Distribution is weighted to make 'view' most common and 'share' least common,
    which mirrors real-world interaction patterns.
    """
    print(f"[*] Generating {num_interactions} interactions...")
    # Weighted action distribution: views are most frequent, shares are rarest
    action_weights = [0.50, 0.25, 0.15, 0.10]  # view, like, comment, share
    
    interactions = []
    for _ in range(num_interactions):
        interactions.append({
            "user_id": random.randint(1, num_users),
            "post_id": random.randint(101, 100 + num_posts),
            "action": random.choices(ACTIONS, weights=action_weights, k=1)[0],
        })
    return pd.DataFrame(interactions)


def main():
    """Main entry point: generate all datasets and save to CSV."""
    # Seed for reproducibility
    random.seed(42)
    
    # Ensure output directories exist
    ensure_directories()
    
    # Generate datasets
    users_df = generate_users()
    posts_df = generate_posts()
    interactions_df = generate_interactions()
    
    # Save to CSV
    users_df.to_csv(USERS_CSV, index=False)
    print(f"[✓] Saved {len(users_df)} users to {USERS_CSV}")
    
    posts_df.to_csv(POSTS_CSV, index=False)
    print(f"[✓] Saved {len(posts_df)} posts to {POSTS_CSV}")
    
    interactions_df.to_csv(INTERACTIONS_CSV, index=False)
    print(f"[✓] Saved {len(interactions_df)} interactions to {INTERACTIONS_CSV}")
    
    # Print sample data
    print("\n--- Sample Users ---")
    print(users_df.head())
    print("\n--- Sample Posts ---")
    print(posts_df.head())
    print("\n--- Sample Interactions ---")
    print(interactions_df.head())
    
    print("\n[✓] Dataset generation complete!")


if __name__ == "__main__":
    main()
