# Distributed Personalized Feed Recommendation System

> A distributed system that generates personalized feeds for users using **Apache Spark** and **MapReduce-style** parallel processing, running on a **4-node cluster**.

---

## 📁 Project Structure

```
distributed-feed-system/
├── data/                          # Synthetic datasets
│   ├── users.csv                  # 100 users
│   ├── posts.csv                  # 500 posts
│   └── interactions.csv           # 10,000 interactions
├── spark_jobs/                    # Spark processing jobs
│   ├── generate_dataset.py        # Synthetic data generator
│   ├── interaction_scoring.py     # MapReduce interaction scoring
│   ├── collaborative_filtering.py # Distributed collaborative filtering
│   ├── feed_ranking.py            # Feed ranking with Window functions
│   └── trending_posts.py          # Trending detection (RDD + DataFrame)
├── backend/                       # Web dashboard & API
│   └── app.py                     # Flask application
├── results/                       # Pipeline outputs
│   ├── feeds/
│   │   └── user_feed.csv          # Final top-10 feeds per user
│   ├── user_post_scores.csv       # Interaction scores
│   ├── collaborative_recommendations.csv
│   └── trending_posts.csv
├── utils/                         # Shared utilities
│   ├── spark_session.py           # SparkSession factory (4-node config)
│   └── helpers.py                 # Constants, weights, helper functions
└── README.md
```

---

## 🏗️ System Architecture

```
Data Source → Spark Cluster (4 nodes) → Recommendation Algorithms → Feed Ranking → Flask API → User Feed
```

**Pipeline Flow:**
1. **Generate Data** → Create synthetic users, posts, interactions
2. **Interaction Scoring** → MAP actions to scores, REDUCE by user-post pairs
3. **Collaborative Filtering** → Cosine similarity, recommend unseen posts
4. **Trending Detection** → MapReduce aggregation of post popularity
5. **Feed Ranking** → Weighted combination: `0.5×Interaction + 0.3×Popularity + 0.2×Recency`
6. **Web Dashboard** → Browse feeds, trending posts, user profiles

---

## ⚙️ Installation

### Prerequisites
- **Python 3.8+**
- **Java 8+** (required for Apache Spark)
- **Apache Spark 3.x** (with PySpark)

### Install Python Dependencies

```bash
pip install pyspark flask pandas
```

---

## 🚀 How to Run

### Step 1: Generate Synthetic Datasets

```bash
python spark_jobs/generate_dataset.py
```

Creates `data/users.csv` (100 users), `data/posts.csv` (500 posts), `data/interactions.csv` (10,000 interactions).

### Step 2: Run Spark Jobs (in order)

```bash
# Interaction Scoring (MapReduce: map actions → scores, reduce by user-post)
spark-submit spark_jobs/interaction_scoring.py

# Collaborative Filtering (cosine similarity, distributed cross-join)
spark-submit spark_jobs/collaborative_filtering.py

# Trending Posts Detection (RDD reduceByKey + DataFrame groupBy)
spark-submit spark_jobs/trending_posts.py

# Feed Ranking (weighted formula + Window ranking for top-10)
spark-submit spark_jobs/feed_ranking.py
```

> **Note:** If `spark-submit` is not in your PATH, you can also run these with `python` directly — PySpark's local mode will be used automatically.

### Step 3: Start Web Dashboard & API Server

```bash
python backend/app.py
```

Open **http://localhost:5000** in your browser to access the dashboard.

---

## 🌐 API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Web dashboard |
| `/feed/<user_id>` | GET | Personalized feed (JSON) |
| `/feed/<user_id>/detailed` | GET | Feed with post metadata (JSON) |
| `/trending` | GET | Trending posts (JSON) |
| `/api/stats` | GET | System statistics (JSON) |
| `/api/users` | GET | All users list (JSON) |

### Example API Response

```bash
curl http://localhost:5000/feed/1
```

```json
{
    "user_id": 1,
    "recommended_posts": [101, 203, 55, 342, 189, 467, 128, 291, 415, 73]
}
```

---

## 🧠 Algorithms

### 1. Interaction Scoring (MapReduce)
- **MAP**: `(user_id, post_id, action) → (user_id, post_id, score)`
- **REDUCE**: `groupBy(user_id, post_id).agg(sum(score))`
- **Weights**: view=1, like=3, comment=4, share=5

### 2. Collaborative Filtering
- Build user-post interaction matrix (Spark DataFrames)
- Compute cosine similarity via distributed cross-join
- Find top-10 similar users (Window ranking)
- Recommend posts via anti-join (exclude already-seen)

### 3. Feed Ranking
- **Formula**: `FeedScore = 0.5 × InteractionScore + 0.3 × Popularity + 0.2 × Recency`
- Uses Window `partitionBy(user_id).orderBy(desc(feed_score))` for top-10 selection

### 4. Trending Detection
- **RDD approach**: `map(post_id, 1)` → `reduceByKey(lambda a, b: a + b)`
- **DataFrame approach**: `groupBy(post_id).count().orderBy(desc)`

---

## ⚡ Spark Distributed Features Used

| Feature | Where Used |
|---|---|
| `local[4]` cluster | SparkSession — 4 parallel executors |
| `map` | Interaction scoring, trending (RDD) |
| `reduceByKey` | Trending posts (RDD MapReduce) |
| `groupBy` + `agg` | Interaction scoring, popularity, trending |
| `join` | Collaborative filtering, feed ranking |
| `cross-join` | Cosine similarity computation |
| `anti-join` | Exclude already-seen posts |
| `Window.partitionBy` | Top-K similar users, top-10 feed ranking |
| `coalesce` | Single-file output |
| Shuffle partitions | Set to 4 to match cluster nodes |

---

## 📊 Expected Output

After running the full pipeline:
- `results/user_post_scores.csv` — User-post interaction scores
- `results/collaborative_recommendations.csv` — Collaborative filtering recommendations
- `results/trending_posts.csv` — Trending posts ranked by interaction count
- `results/feeds/user_feed.csv` — **Final feed: Top 10 recommended posts per user**

---

## 🖥️ Web Dashboard

The web dashboard at `http://localhost:5000` provides:
- **📊 Stats Overview** — Total users, feeds, interactions, trending posts
- **🎯 User Feeds** — Search by User ID to view personalized recommendations
- **🔥 Trending Posts** — Ranked table of most popular posts
- **👥 Browse Users** — Click any user to view their feed
- **🏗️ Architecture** — Visual pipeline diagram and algorithm descriptions
