# Cluster Execution Guide (1 Master + 3 Workers)

Follow these steps once your physical cluster is online to run the distributed feed system.

## 1. Prerequisites
- **Python Version**: Ensure **Python 3.13** is installed and set as the default on the Master and all 3 Worker machines.
- **Spark & Hadoop**: Ensure Spark 4.1.1 and Hadoop are configured on all machines.
- **Master Active**: The Master must be running at `spark://10.12.226.131:7077`.
- **Workers Connected**: All 3 Workers must be visible in the Spark Master UI (usually at `http://10.12.226.131:8080`).

## 2. Configuration Check
In `utils/spark_session.py`, ensure these settings are active:
```python
USE_CLUSTER = True
SPARK_MASTER_URL = "spark://10.12.226.131:7077"
DRIVER_HOST_IP = "10.12.227.126"
```

## 3. Execution Steps

Run these commands in order from the `distributed-feed-system` root directory:

### Step A: Generate Master Dataset (If needed)
```bash
python spark_jobs/generate_dataset.py
```

### Step B: Run the Distributed Pipeline
Run these one by one to monitor for errors:
```bash
python spark_jobs/interaction_scoring.py
python spark_jobs/collaborative_filtering.py
python spark_jobs/trending_posts.py
python spark_jobs/feed_ranking.py
```

### Step C: Start the Dashboard
Once the jobs finish, start the API to see the results:
```bash
python backend/app.py
```

## 4. Monitoring
- **Spark UI**: While the jobs are running, visit `http://localhost:4040` on your Master machine to see the task distribution across the 3 workers.
- **Results**: Check the `results/` directory for the generated CSV files.
