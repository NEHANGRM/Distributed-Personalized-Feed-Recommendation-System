"""
Flask Web Application & API
=============================
Serves a beautiful web dashboard and REST API for the distributed
feed recommendation system.

Endpoints:
    GET /                   → Web dashboard homepage
    GET /feed/<user_id>     → JSON API: personalized feed for a user
    GET /trending           → JSON API: trending posts
    GET /dashboard          → Web dashboard with interactive UI
    GET /api/stats          → JSON API: system statistics

The web dashboard provides a visual interface to browse user feeds,
view trending posts, and explore system statistics.
"""

import sys
import os
import csv
import json
from collections import defaultdict

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, jsonify, render_template_string, request

from utils.helpers import (
    USER_FEED_CSV, TRENDING_POSTS_CSV, USERS_CSV, POSTS_CSV,
    INTERACTIONS_CSV, FEEDS_DIR, RESULTS_DIR, BASE_DIR
)

app = Flask(__name__)


# ---------------------------------------------------------------------------
# Data Loading Helpers
# ---------------------------------------------------------------------------

def read_spark_csv(directory_path):
    """
    Read CSV output from Spark (which saves as a directory of part files).
    Looks for part-*.csv files inside the directory.
    Falls back to reading the path as a single file if not a directory.
    """
    rows = []
    headers = None

    if os.path.isdir(directory_path):
        # Spark saves output as directory with part-*.csv files
        for filename in sorted(os.listdir(directory_path)):
            if filename.startswith("part-") and filename.endswith(".csv"):
                filepath = os.path.join(directory_path, filename)
                with open(filepath, "r", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    file_headers = next(reader)
                    if headers is None:
                        headers = file_headers
                    for row in reader:
                        rows.append(row)
    elif os.path.isfile(directory_path):
        with open(directory_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = next(reader)
            for row in reader:
                rows.append(row)

    return headers, rows


def load_user_feeds():
    """Load precomputed user feeds from results/feeds/user_feed.csv."""
    feeds = defaultdict(list)
    headers, rows = read_spark_csv(USER_FEED_CSV)

    if not headers:
        return feeds

    for row in rows:
        if len(row) >= 3:
            user_id = int(float(row[0]))
            post_id = int(float(row[1]))
            feed_score = round(float(row[2]), 4)
            feeds[user_id].append({"post_id": post_id, "feed_score": feed_score})

    # Sort each user's feed by score descending
    for uid in feeds:
        feeds[uid].sort(key=lambda x: x["feed_score"], reverse=True)

    return feeds


def load_trending_posts():
    """Load precomputed trending posts from results/trending_posts.csv."""
    trending = []
    headers, rows = read_spark_csv(TRENDING_POSTS_CSV)

    if not headers:
        return trending

    for row in rows:
        if len(row) >= 2:
            entry = {}
            for i, h in enumerate(headers):
                if i < len(row):
                    try:
                        entry[h] = int(float(row[i]))
                    except ValueError:
                        entry[h] = row[i]
            trending.append(entry)

    trending.sort(key=lambda x: x.get("total_interactions", 0), reverse=True)
    return trending


def load_users():
    """Load user data from data/users.csv."""
    users = {}
    if os.path.isfile(USERS_CSV):
        with open(USERS_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                users[int(row["user_id"])] = row
    return users


def load_posts():
    """Load post data from data/posts.csv."""
    posts = {}
    if os.path.isfile(POSTS_CSV):
        with open(POSTS_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                posts[int(row["post_id"])] = row
    return posts


def get_interaction_stats():
    """Get basic stats about interactions."""
    stats = {"total": 0, "actions": defaultdict(int)}
    if os.path.isfile(INTERACTIONS_CSV):
        with open(INTERACTIONS_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                stats["total"] += 1
                stats["actions"][row["action"]] += 1
    stats["actions"] = dict(stats["actions"])
    return stats


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------

@app.route("/feed/<int:user_id>")
def get_feed(user_id):
    """
    GET /feed/<user_id>
    Returns personalized feed for a specific user as JSON.

    Response:
        {
            "user_id": 1,
            "recommended_posts": [101, 203, 55, ...]
        }
    """
    feeds = load_user_feeds()

    if user_id not in feeds:
        return jsonify({
            "user_id": user_id,
            "recommended_posts": [],
            "message": "No feed found for this user. Run the Spark pipeline first."
        }), 404

    recommended_posts = [item["post_id"] for item in feeds[user_id]]

    return jsonify({
        "user_id": user_id,
        "recommended_posts": recommended_posts
    })


@app.route("/feed/<int:user_id>/detailed")
def get_feed_detailed(user_id):
    """GET /feed/<user_id>/detailed — detailed feed with scores."""
    feeds = load_user_feeds()
    posts = load_posts()
    users = load_users()

    user_info = users.get(user_id, {})
    feed_items = feeds.get(user_id, [])

    enriched = []
    for item in feed_items:
        post = posts.get(item["post_id"], {})
        enriched.append({
            "post_id": item["post_id"],
            "feed_score": item["feed_score"],
            "topic": post.get("topic", "Unknown"),
            "creator_id": post.get("creator_id", "Unknown"),
            "timestamp": post.get("timestamp", "Unknown"),
        })

    return jsonify({
        "user_id": user_id,
        "user_info": user_info,
        "feed": enriched
    })


@app.route("/trending")
def get_trending():
    """GET /trending — returns top trending posts."""
    trending = load_trending_posts()
    return jsonify({"trending_posts": trending[:20]})


@app.route("/api/stats")
def get_stats():
    """GET /api/stats — returns system statistics."""
    feeds = load_user_feeds()
    trending = load_trending_posts()
    users = load_users()
    stats = get_interaction_stats()

    return jsonify({
        "total_users": len(users),
        "users_with_feeds": len(feeds),
        "total_trending_posts": len(trending),
        "interaction_stats": stats,
    })


@app.route("/api/users")
def get_all_users():
    """GET /api/users — returns all users with basic info."""
    users = load_users()
    return jsonify({"users": list(users.values())})


# ---------------------------------------------------------------------------
# Web Dashboard
# ---------------------------------------------------------------------------

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Feed Recommendation System — Dashboard</title>
    <meta name="description" content="Distributed Personalized Feed Recommendation System Dashboard. View user feeds, trending posts, and system analytics.">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-primary: #0a0a1a;
            --bg-secondary: #111128;
            --bg-card: #1a1a3e;
            --bg-card-hover: #222255;
            --accent-primary: #6c5ce7;
            --accent-secondary: #a29bfe;
            --accent-glow: rgba(108, 92, 231, 0.3);
            --accent-warm: #fd79a8;
            --accent-green: #00b894;
            --accent-orange: #fdcb6e;
            --accent-cyan: #00cec9;
            --text-primary: #e8e8ff;
            --text-secondary: #a0a0cc;
            --text-muted: #6c6c99;
            --border: rgba(108, 92, 231, 0.2);
            --border-hover: rgba(108, 92, 231, 0.5);
            --shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
            --shadow-glow: 0 0 40px rgba(108, 92, 231, 0.15);
            --radius: 16px;
            --radius-sm: 10px;
        }

        * { margin: 0; padding: 0; box-sizing: border-box; }

        body {
            font-family: 'Inter', -apple-system, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            overflow-x: hidden;
        }

        /* Animated background */
        body::before {
            content: '';
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background:
                radial-gradient(ellipse at 20% 20%, rgba(108, 92, 231, 0.08) 0%, transparent 50%),
                radial-gradient(ellipse at 80% 80%, rgba(253, 121, 168, 0.06) 0%, transparent 50%),
                radial-gradient(ellipse at 50% 50%, rgba(0, 206, 201, 0.04) 0%, transparent 50%);
            pointer-events: none;
            z-index: 0;
        }

        /* Header */
        .header {
            position: sticky;
            top: 0;
            z-index: 100;
            background: rgba(10, 10, 26, 0.85);
            backdrop-filter: blur(20px);
            border-bottom: 1px solid var(--border);
            padding: 0 2rem;
        }

        .header-inner {
            max-width: 1400px;
            margin: 0 auto;
            display: flex;
            align-items: center;
            justify-content: space-between;
            height: 72px;
        }

        .logo {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .logo-icon {
            width: 42px;
            height: 42px;
            background: linear-gradient(135deg, var(--accent-primary), var(--accent-warm));
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 20px;
            box-shadow: 0 0 20px rgba(108, 92, 231, 0.3);
        }

        .logo-text {
            font-size: 1.3rem;
            font-weight: 700;
            background: linear-gradient(135deg, var(--text-primary), var(--accent-secondary));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .logo-sub {
            font-size: 0.7rem;
            color: var(--text-muted);
            font-weight: 400;
            letter-spacing: 2px;
            text-transform: uppercase;
        }

        .header-badge {
            padding: 6px 16px;
            background: rgba(0, 184, 148, 0.15);
            border: 1px solid rgba(0, 184, 148, 0.3);
            border-radius: 20px;
            font-size: 0.75rem;
            color: var(--accent-green);
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .pulse-dot {
            width: 8px; height: 8px;
            background: var(--accent-green);
            border-radius: 50%;
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.5; transform: scale(0.8); }
        }

        /* Main Layout */
        .main {
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
            position: relative;
            z-index: 1;
        }

        /* Stats Row */
        .stats-row {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }

        .stat-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: var(--radius);
            padding: 1.25rem;
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }

        .stat-card::before {
            content: '';
            position: absolute;
            top: 0; left: 0; right: 0;
            height: 3px;
            border-radius: var(--radius) var(--radius) 0 0;
        }

        .stat-card:nth-child(1)::before { background: linear-gradient(90deg, var(--accent-primary), var(--accent-secondary)); }
        .stat-card:nth-child(2)::before { background: linear-gradient(90deg, var(--accent-warm), #e17055); }
        .stat-card:nth-child(3)::before { background: linear-gradient(90deg, var(--accent-green), #55efc4); }
        .stat-card:nth-child(4)::before { background: linear-gradient(90deg, var(--accent-cyan), #81ecec); }
        .stat-card:nth-child(5)::before { background: linear-gradient(90deg, var(--accent-orange), #ffeaa7); }

        .stat-card:hover {
            border-color: var(--border-hover);
            transform: translateY(-2px);
            box-shadow: var(--shadow-glow);
        }

        .stat-label {
            font-size: 0.75rem;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 1.5px;
            font-weight: 600;
            margin-bottom: 0.5rem;
        }

        .stat-value {
            font-size: 2rem;
            font-weight: 800;
            background: linear-gradient(135deg, var(--text-primary), var(--accent-secondary));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        /* Section Tabs */
        .tabs {
            display: flex;
            gap: 0.5rem;
            margin-bottom: 1.5rem;
            flex-wrap: wrap;
        }

        .tab {
            padding: 10px 24px;
            border-radius: 12px;
            border: 1px solid var(--border);
            background: transparent;
            color: var(--text-secondary);
            cursor: pointer;
            font-family: 'Inter', sans-serif;
            font-size: 0.9rem;
            font-weight: 500;
            transition: all 0.3s ease;
        }

        .tab:hover {
            border-color: var(--border-hover);
            color: var(--text-primary);
            background: rgba(108, 92, 231, 0.1);
        }

        .tab.active {
            background: linear-gradient(135deg, var(--accent-primary), #7c6cf0);
            border-color: var(--accent-primary);
            color: white;
            box-shadow: 0 4px 15px rgba(108, 92, 231, 0.3);
        }

        /* Panels */
        .panel { display: none; }
        .panel.active { display: block; animation: fadeIn 0.4s ease; }

        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }

        /* User Feed Section */
        .search-bar {
            display: flex;
            gap: 1rem;
            margin-bottom: 1.5rem;
            align-items: center;
        }

        .search-input {
            flex: 1;
            max-width: 400px;
            padding: 12px 20px;
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: var(--radius-sm);
            color: var(--text-primary);
            font-family: 'Inter', sans-serif;
            font-size: 0.95rem;
            outline: none;
            transition: all 0.3s ease;
        }

        .search-input:focus {
            border-color: var(--accent-primary);
            box-shadow: 0 0 0 3px rgba(108, 92, 231, 0.15);
        }

        .search-input::placeholder { color: var(--text-muted); }

        .btn {
            padding: 12px 28px;
            border-radius: var(--radius-sm);
            border: none;
            font-family: 'Inter', sans-serif;
            font-size: 0.9rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s ease;
        }

        .btn-primary {
            background: linear-gradient(135deg, var(--accent-primary), #7c6cf0);
            color: white;
            box-shadow: 0 4px 15px rgba(108, 92, 231, 0.3);
        }

        .btn-primary:hover {
            transform: translateY(-1px);
            box-shadow: 0 6px 20px rgba(108, 92, 231, 0.4);
        }

        /* User Info Card */
        .user-info-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: var(--radius);
            padding: 1.5rem;
            margin-bottom: 1.5rem;
            display: none;
        }

        .user-info-card.visible { display: flex; gap: 2rem; align-items: center; }

        .user-avatar {
            width: 64px; height: 64px;
            border-radius: 16px;
            background: linear-gradient(135deg, var(--accent-primary), var(--accent-warm));
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.5rem;
            font-weight: 700;
            color: white;
            flex-shrink: 0;
        }

        .user-details h3 {
            font-size: 1.1rem;
            font-weight: 600;
            margin-bottom: 0.3rem;
        }

        .user-meta {
            display: flex;
            gap: 1.5rem;
            font-size: 0.85rem;
            color: var(--text-secondary);
        }

        .user-meta span {
            display: flex;
            align-items: center;
            gap: 4px;
        }

        /* Feed Grid */
        .feed-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 1rem;
        }

        .feed-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: var(--radius);
            padding: 1.25rem;
            transition: all 0.3s ease;
            position: relative;
        }

        .feed-card:hover {
            border-color: var(--border-hover);
            transform: translateY(-3px);
            box-shadow: var(--shadow-glow);
        }

        .feed-card-rank {
            position: absolute;
            top: -8px;
            right: 16px;
            width: 32px;
            height: 32px;
            background: linear-gradient(135deg, var(--accent-primary), var(--accent-warm));
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 700;
            font-size: 0.85rem;
            color: white;
            box-shadow: 0 4px 12px rgba(108, 92, 231, 0.3);
        }

        .feed-card-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 0.75rem;
        }

        .feed-post-id {
            font-size: 1.1rem;
            font-weight: 600;
        }

        .feed-topic {
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .topic-AI { background: rgba(108, 92, 231, 0.2); color: var(--accent-secondary); }
        .topic-Sports { background: rgba(0, 184, 148, 0.2); color: var(--accent-green); }
        .topic-Music { background: rgba(253, 121, 168, 0.2); color: var(--accent-warm); }
        .topic-Technology { background: rgba(0, 206, 201, 0.2); color: var(--accent-cyan); }
        .topic-Travel { background: rgba(253, 203, 110, 0.2); color: var(--accent-orange); }
        .topic-Food { background: rgba(225, 112, 85, 0.2); color: #e17055; }
        .topic-Science { background: rgba(116, 185, 255, 0.2); color: #74b9ff; }
        .topic-default { background: rgba(162, 155, 254, 0.2); color: var(--accent-secondary); }

        .feed-score-bar {
            width: 100%;
            height: 6px;
            background: rgba(255, 255, 255, 0.05);
            border-radius: 3px;
            margin: 0.75rem 0;
            overflow: hidden;
        }

        .feed-score-fill {
            height: 100%;
            border-radius: 3px;
            background: linear-gradient(90deg, var(--accent-primary), var(--accent-warm));
            transition: width 0.6s ease;
        }

        .feed-meta {
            display: flex;
            justify-content: space-between;
            font-size: 0.8rem;
            color: var(--text-muted);
        }

        /* Trending Table */
        .trending-table {
            width: 100%;
            border-collapse: separate;
            border-spacing: 0;
            background: var(--bg-card);
            border-radius: var(--radius);
            overflow: hidden;
            border: 1px solid var(--border);
        }

        .trending-table th {
            text-align: left;
            padding: 14px 20px;
            background: rgba(108, 92, 231, 0.1);
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 1.5px;
            color: var(--text-muted);
            font-weight: 600;
        }

        .trending-table td {
            padding: 12px 20px;
            border-top: 1px solid var(--border);
            font-size: 0.9rem;
        }

        .trending-table tr:hover td {
            background: rgba(108, 92, 231, 0.05);
        }

        .trend-rank {
            font-weight: 700;
            color: var(--accent-secondary);
        }

        .trend-bar {
            width: 100%;
            max-width: 200px;
            height: 8px;
            background: rgba(255, 255, 255, 0.05);
            border-radius: 4px;
            overflow: hidden;
        }

        .trend-bar-fill {
            height: 100%;
            border-radius: 4px;
            background: linear-gradient(90deg, var(--accent-green), var(--accent-cyan));
        }

        /* Architecture Section */
        .arch-container {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: var(--radius);
            padding: 2rem;
        }

        .arch-flow {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 0.5rem;
            flex-wrap: wrap;
            margin: 2rem 0;
        }

        .arch-node {
            padding: 16px 24px;
            border-radius: var(--radius-sm);
            font-weight: 600;
            font-size: 0.85rem;
            text-align: center;
            min-width: 120px;
        }

        .arch-node.source { background: rgba(253, 203, 110, 0.2); border: 1px solid rgba(253, 203, 110, 0.4); color: var(--accent-orange); }
        .arch-node.spark { background: rgba(108, 92, 231, 0.2); border: 1px solid rgba(108, 92, 231, 0.4); color: var(--accent-secondary); }
        .arch-node.algo { background: rgba(253, 121, 168, 0.2); border: 1px solid rgba(253, 121, 168, 0.4); color: var(--accent-warm); }
        .arch-node.api { background: rgba(0, 184, 148, 0.2); border: 1px solid rgba(0, 184, 148, 0.4); color: var(--accent-green); }
        .arch-node.output { background: rgba(0, 206, 201, 0.2); border: 1px solid rgba(0, 206, 201, 0.4); color: var(--accent-cyan); }

        .arch-arrow {
            font-size: 1.5rem;
            color: var(--text-muted);
        }

        .algo-cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 1rem;
            margin-top: 2rem;
        }

        .algo-card {
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid var(--border);
            border-radius: var(--radius-sm);
            padding: 1.25rem;
        }

        .algo-card h4 {
            font-size: 0.95rem;
            margin-bottom: 0.5rem;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .algo-card p {
            font-size: 0.82rem;
            color: var(--text-secondary);
            line-height: 1.5;
        }

        .algo-card code {
            background: rgba(108, 92, 231, 0.15);
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 0.78rem;
            color: var(--accent-secondary);
        }

        /* Empty State */
        .empty-state {
            text-align: center;
            padding: 4rem 2rem;
            color: var(--text-muted);
        }

        .empty-state-icon {
            font-size: 4rem;
            margin-bottom: 1rem;
        }

        .empty-state h3 {
            font-size: 1.2rem;
            color: var(--text-secondary);
            margin-bottom: 0.5rem;
        }

        .empty-state p {
            font-size: 0.9rem;
            max-width: 400px;
            margin: 0 auto;
            line-height: 1.6;
        }

        .code-block {
            background: rgba(0,0,0,0.3);
            border: 1px solid var(--border);
            border-radius: var(--radius-sm);
            padding: 1rem 1.25rem;
            margin-top: 1rem;
            font-family: 'Courier New', monospace;
            font-size: 0.8rem;
            color: var(--accent-secondary);
            overflow-x: auto;
            white-space: pre;
        }

        /* Users Grid */
        .users-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
            gap: 0.75rem;
            max-height: 500px;
            overflow-y: auto;
            padding-right: 0.5rem;
        }

        .users-grid::-webkit-scrollbar { width: 6px; }
        .users-grid::-webkit-scrollbar-track { background: transparent; }
        .users-grid::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

        .user-chip {
            padding: 10px 14px;
            border-radius: var(--radius-sm);
            background: var(--bg-card);
            border: 1px solid var(--border);
            cursor: pointer;
            transition: all 0.3s ease;
            text-align: center;
        }

        .user-chip:hover {
            border-color: var(--accent-primary);
            background: rgba(108, 92, 231, 0.1);
            transform: translateY(-2px);
        }

        .user-chip-id {
            font-weight: 700;
            font-size: 1rem;
            margin-bottom: 2px;
        }

        .user-chip-info {
            font-size: 0.72rem;
            color: var(--text-muted);
        }

        /* Loading Spinner */
        .spinner {
            width: 24px; height: 24px;
            border: 3px solid var(--border);
            border-top-color: var(--accent-primary);
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
            display: none;
        }

        .spinner.active { display: inline-block; }

        @keyframes spin { to { transform: rotate(360deg); } }

        /* Responsive */
        @media (max-width: 768px) {
            .main { padding: 1rem; }
            .header-inner { padding: 0; }
            .stats-row { grid-template-columns: repeat(2, 1fr); }
            .feed-grid { grid-template-columns: 1fr; }
            .arch-flow { flex-direction: column; }
            .arch-arrow { transform: rotate(90deg); }
            .search-bar { flex-direction: column; }
            .search-input { max-width: 100%; }
        }
    </style>
</head>
<body>

<!-- Header -->
<header class="header">
    <div class="header-inner">
        <div class="logo">
            <div class="logo-icon">⚡</div>
            <div>
                <div class="logo-text">FeedRec</div>
                <div class="logo-sub">Distributed Recommendation Engine</div>
            </div>
        </div>
        <div class="header-badge">
            <span class="pulse-dot"></span>
            4-Node Spark Cluster
        </div>
    </div>
</header>

<!-- Main Content -->
<main class="main">

    <!-- Stats -->
    <div class="stats-row" id="statsRow">
        <div class="stat-card">
            <div class="stat-label">Total Users</div>
            <div class="stat-value" id="statUsers">—</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Users with Feeds</div>
            <div class="stat-value" id="statFeeds">—</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Total Interactions</div>
            <div class="stat-value" id="statInteractions">—</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Trending Posts</div>
            <div class="stat-value" id="statTrending">—</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Cluster Nodes</div>
            <div class="stat-value">4</div>
        </div>
    </div>

    <!-- Tabs -->
    <div class="tabs">
        <button class="tab active" onclick="switchTab('feed')" id="tabFeed">🎯 User Feeds</button>
        <button class="tab" onclick="switchTab('trending')" id="tabTrending">🔥 Trending Posts</button>
        <button class="tab" onclick="switchTab('users')" id="tabUsers">👥 Browse Users</button>
        <button class="tab" onclick="switchTab('architecture')" id="tabArch">🏗️ Architecture</button>
    </div>

    <!-- Panel: User Feeds -->
    <div class="panel active" id="panelFeed">
        <div class="search-bar">
            <input type="number" class="search-input" id="userIdInput" placeholder="Enter User ID (1-100)..." min="1" max="100">
            <button class="btn btn-primary" onclick="loadFeed()">Load Feed</button>
            <div class="spinner" id="feedSpinner"></div>
        </div>
        <div class="user-info-card" id="userInfoCard">
            <div class="user-avatar" id="userAvatar">?</div>
            <div class="user-details">
                <h3 id="userName">User</h3>
                <div class="user-meta">
                    <span>📍 <span id="userLocation">—</span></span>
                    <span>🎂 Age: <span id="userAge">—</span></span>
                    <span>💡 Interest: <span id="userInterest">—</span></span>
                </div>
            </div>
        </div>
        <div class="feed-grid" id="feedGrid">
            <div class="empty-state">
                <div class="empty-state-icon">🎯</div>
                <h3>Search for a User</h3>
                <p>Enter a User ID above to view their personalized feed generated by the Spark recommendation pipeline.</p>
            </div>
        </div>
    </div>

    <!-- Panel: Trending Posts -->
    <div class="panel" id="panelTrending">
        <table class="trending-table" id="trendingTable">
            <thead>
                <tr>
                    <th>Rank</th>
                    <th>Post ID</th>
                    <th>Topic</th>
                    <th>Interactions</th>
                    <th>Popularity</th>
                </tr>
            </thead>
            <tbody id="trendingBody">
                <tr><td colspan="5" style="text-align:center; padding:3rem; color:var(--text-muted);">Loading...</td></tr>
            </tbody>
        </table>
    </div>

    <!-- Panel: Browse Users -->
    <div class="panel" id="panelUsers">
        <p style="margin-bottom: 1rem; color: var(--text-secondary); font-size: 0.9rem;">
            Click on a user to view their personalized feed.
        </p>
        <div class="users-grid" id="usersGrid">
            <div class="empty-state"><p>Loading users...</p></div>
        </div>
    </div>

    <!-- Panel: Architecture -->
    <div class="panel" id="panelArchitecture">
        <div class="arch-container">
            <h2 style="margin-bottom: 0.5rem;">System Architecture</h2>
            <p style="color: var(--text-secondary); font-size: 0.9rem; margin-bottom: 1rem;">
                Distributed processing pipeline on a 4-node Apache Spark cluster.
            </p>

            <div class="arch-flow">
                <div class="arch-node source">📊 Data Source<br><small>CSV Datasets</small></div>
                <div class="arch-arrow">→</div>
                <div class="arch-node spark">⚡ Spark Cluster<br><small>4 Nodes (local[4])</small></div>
                <div class="arch-arrow">→</div>
                <div class="arch-node algo">🧠 Algorithms<br><small>MapReduce + CF</small></div>
                <div class="arch-arrow">→</div>
                <div class="arch-node api">🌐 Flask API<br><small>REST + Dashboard</small></div>
                <div class="arch-arrow">→</div>
                <div class="arch-node output">📋 User Feed<br><small>Top 10 Posts</small></div>
            </div>

            <div class="algo-cards">
                <div class="algo-card">
                    <h4>📌 Interaction Scoring</h4>
                    <p>MapReduce: <code>map(action→score)</code> then <code>reduceByKey(sum)</code> to aggregate user-post scores. Weights: view=1, like=3, comment=4, share=5.</p>
                </div>
                <div class="algo-card">
                    <h4>🤝 Collaborative Filtering</h4>
                    <p>Cosine similarity via distributed <code>cross-join</code>. Finds top-K similar users, recommends their posts using <code>anti-join</code>.</p>
                </div>
                <div class="algo-card">
                    <h4>📊 Feed Ranking</h4>
                    <p>Formula: <code>0.5×Interaction + 0.3×Popularity + 0.2×Recency</code>. Uses <code>Window.partitionBy</code> for top-10 per user.</p>
                </div>
                <div class="algo-card">
                    <h4>🔥 Trending Detection</h4>
                    <p>Classic MapReduce: <code>map(post_id, 1)</code> then <code>reduceByKey(+)</code>. Also demonstrated with DataFrame <code>groupBy.count()</code>.</p>
                </div>
            </div>

            <h3 style="margin-top: 2rem; margin-bottom: 0.75rem;">🚀 Quick Start Commands</h3>
            <div class="code-block">python spark_jobs/generate_dataset.py
spark-submit spark_jobs/interaction_scoring.py
spark-submit spark_jobs/collaborative_filtering.py
spark-submit spark_jobs/trending_posts.py
spark-submit spark_jobs/feed_ranking.py
python backend/app.py</div>
        </div>
    </div>

</main>

<script>
    // -----------------------------------------------------------------------
    // Tab Switching
    // -----------------------------------------------------------------------
    function switchTab(tabName) {
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
        document.getElementById('tab' + tabName.charAt(0).toUpperCase() + tabName.slice(1)).classList.add('active');
        document.getElementById('panel' + tabName.charAt(0).toUpperCase() + tabName.slice(1)).classList.add('active');
    }

    // -----------------------------------------------------------------------
    // Load Stats
    // -----------------------------------------------------------------------
    async function loadStats() {
        try {
            const res = await fetch('/api/stats');
            const data = await res.json();
            document.getElementById('statUsers').textContent = data.total_users || 0;
            document.getElementById('statFeeds').textContent = data.users_with_feeds || 0;
            document.getElementById('statInteractions').textContent = (data.interaction_stats?.total || 0).toLocaleString();
            document.getElementById('statTrending').textContent = data.total_trending_posts || 0;
        } catch(e) {
            console.log('Stats not available yet');
        }
    }

    // -----------------------------------------------------------------------
    // Load User Feed
    // -----------------------------------------------------------------------
    async function loadFeed() {
        const userId = document.getElementById('userIdInput').value;
        if (!userId) return;

        const spinner = document.getElementById('feedSpinner');
        spinner.classList.add('active');

        try {
            // Load detailed feed
            const res = await fetch(`/feed/${userId}/detailed`);
            const data = await res.json();

            // Update user info
            const card = document.getElementById('userInfoCard');
            if (data.user_info && data.user_info.user_id) {
                document.getElementById('userAvatar').textContent = 'U' + userId;
                document.getElementById('userName').textContent = 'User #' + userId;
                document.getElementById('userLocation').textContent = data.user_info.location || '—';
                document.getElementById('userAge').textContent = data.user_info.age || '—';
                document.getElementById('userInterest').textContent = data.user_info.interest || '—';
                card.classList.add('visible');
            } else {
                card.classList.remove('visible');
            }

            // Render feed cards
            const grid = document.getElementById('feedGrid');
            if (data.feed && data.feed.length > 0) {
                const maxScore = Math.max(...data.feed.map(f => f.feed_score));
                grid.innerHTML = data.feed.map((item, i) => {
                    const pct = maxScore > 0 ? (item.feed_score / maxScore * 100) : 0;
                    const topicClass = ['AI','Sports','Music','Technology','Travel','Food','Science']
                        .includes(item.topic) ? 'topic-' + item.topic : 'topic-default';
                    return `
                        <div class="feed-card" style="animation-delay: ${i * 0.05}s">
                            <div class="feed-card-rank">#${i + 1}</div>
                            <div class="feed-card-header">
                                <span class="feed-post-id">Post #${item.post_id}</span>
                                <span class="feed-topic ${topicClass}">${item.topic}</span>
                            </div>
                            <div class="feed-score-bar">
                                <div class="feed-score-fill" style="width: ${pct}%"></div>
                            </div>
                            <div class="feed-meta">
                                <span>Score: ${item.feed_score.toFixed(4)}</span>
                                <span>📅 ${item.timestamp}</span>
                            </div>
                        </div>
                    `;
                }).join('');
            } else {
                grid.innerHTML = `
                    <div class="empty-state">
                        <div class="empty-state-icon">📭</div>
                        <h3>No Feed Found</h3>
                        <p>No recommendations for User #${userId}. Make sure you've run the Spark pipeline first.</p>
                    </div>
                `;
            }
        } catch(e) {
            document.getElementById('feedGrid').innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">⚠️</div>
                    <h3>Error Loading Feed</h3>
                    <p>${e.message}</p>
                </div>
            `;
        }

        spinner.classList.remove('active');
    }

    // -----------------------------------------------------------------------
    // Load Trending Posts
    // -----------------------------------------------------------------------
    async function loadTrending() {
        try {
            const res = await fetch('/trending');
            const data = await res.json();
            const tbody = document.getElementById('trendingBody');

            if (data.trending_posts && data.trending_posts.length > 0) {
                const maxInteractions = Math.max(...data.trending_posts.map(p => p.total_interactions || 0));
                tbody.innerHTML = data.trending_posts.map((post, i) => {
                    const pct = maxInteractions > 0 ? ((post.total_interactions || 0) / maxInteractions * 100) : 0;
                    const topicClass = ['AI','Sports','Music','Technology','Travel','Food','Science']
                        .includes(post.topic) ? 'topic-' + post.topic : 'topic-default';
                    return `
                        <tr>
                            <td class="trend-rank">#${i + 1}</td>
                            <td><strong>Post #${post.post_id}</strong></td>
                            <td><span class="feed-topic ${topicClass}">${post.topic || '—'}</span></td>
                            <td>${(post.total_interactions || 0).toLocaleString()}</td>
                            <td>
                                <div class="trend-bar">
                                    <div class="trend-bar-fill" style="width: ${pct}%"></div>
                                </div>
                            </td>
                        </tr>
                    `;
                }).join('');
            } else {
                tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; padding:3rem; color:var(--text-muted);">No trending data. Run the Spark pipeline first.</td></tr>';
            }
        } catch(e) {
            console.log('Trending data not available');
        }
    }

    // -----------------------------------------------------------------------
    // Load Users List
    // -----------------------------------------------------------------------
    async function loadUsers() {
        try {
            const res = await fetch('/api/users');
            const data = await res.json();
            const grid = document.getElementById('usersGrid');

            if (data.users && data.users.length > 0) {
                grid.innerHTML = data.users.map(u => `
                    <div class="user-chip" onclick="selectUser(${u.user_id})">
                        <div class="user-chip-id">User #${u.user_id}</div>
                        <div class="user-chip-info">${u.location} · ${u.interest}</div>
                    </div>
                `).join('');
            } else {
                grid.innerHTML = '<div class="empty-state"><p>No users found. Run generate_dataset.py first.</p></div>';
            }
        } catch(e) {
            console.log('Users not available');
        }
    }

    function selectUser(userId) {
        document.getElementById('userIdInput').value = userId;
        switchTab('feed');
        loadFeed();
    }

    // Enter key triggers search
    document.getElementById('userIdInput').addEventListener('keydown', function(e) {
        if (e.key === 'Enter') loadFeed();
    });

    // -----------------------------------------------------------------------
    // Initialize
    // -----------------------------------------------------------------------
    loadStats();
    loadTrending();
    loadUsers();
</script>
</body>
</html>
"""


@app.route("/")
@app.route("/dashboard")
def dashboard():
    """Serve the web dashboard."""
    return render_template_string(DASHBOARD_HTML)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("  Feed Recommendation System — Web Dashboard")
    print("=" * 60)
    print(f"\n  Dashboard:  http://localhost:5000")
    print(f"  API Feed:   http://localhost:5000/feed/<user_id>")
    print(f"  Trending:   http://localhost:5000/trending")
    print(f"  Stats:      http://localhost:5000/api/stats")
    print(f"\n{'=' * 60}\n")
    app.run(debug=True, host="0.0.0.0", port=5000)
