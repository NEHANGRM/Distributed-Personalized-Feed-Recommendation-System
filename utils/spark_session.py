"""
Spark Session Factory
=====================
Creates and configures a SparkSession for the distributed feed recommendation system.
The session is configured to simulate a 4-node cluster using local[4] mode,
which spawns 4 worker threads to process data in parallel.
"""

import os
import sys

# ---------------------------------------------------------------------------
# Configure Spark and Hadoop for Windows
# ---------------------------------------------------------------------------
_SPARK_EXTRACTED = r"C:\Users\DELL\Desktop\spark-4.1.1-bin-hadoop3\spark-4.1.1-bin-hadoop3"
_PYTHON_312 = r"C:\Users\DELL\AppData\Local\Programs\Python\Python312\python.exe"
_PYTHON_313 = r"C:\Users\DELL\AppData\Local\Programs\Python\Python313\python.exe"

os.environ["SPARK_HOME"] = _SPARK_EXTRACTED
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
os.environ["SPARK_LOCAL_DIRS"] = r"C:\spark_tmp"
os.environ["_JAVA_OPTIONS"] = "-Djava.io.tmpdir=C:\\spark_tmp"

# ---------------------------------------------------------------------------
# Cluster Configuration (1 Master + 3 Workers)
# ---------------------------------------------------------------------------
# Set to True to use the physical cluster, False for local simulation
USE_CLUSTER = True

# Cluster Details
SPARK_MASTER_URL = "spark://10.12.226.131:7077"
DRIVER_HOST_IP = "10.12.227.126" 

# Choose Python version based on mode
_PYTHON_PATH = _PYTHON_313 if USE_CLUSTER else _PYTHON_312
os.environ["PYSPARK_PYTHON"] = _PYTHON_PATH
os.environ["PYSPARK_DRIVER_PYTHON"] = _PYTHON_PATH

# Ensure binaries are in PATH
os.environ["PATH"] = (
    os.path.join(os.environ["JAVA_HOME"], "bin") + os.pathsep +
    os.path.join(os.environ["SPARK_HOME"], "bin") + os.pathsep +
    os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep +
    os.environ.get("PATH", "")
)

from pyspark.sql import SparkSession

def create_spark_session(app_name="DistributedFeedSystem"):
    """
    Create a SparkSession for the 1+3 node cluster.
    """
    builder = SparkSession.builder.appName(app_name)

    if USE_CLUSTER:
        print(f"[*] CONNECTING TO CLUSTER (1 Master + 3 Workers): {SPARK_MASTER_URL}")
        builder = (
            builder
            .master(SPARK_MASTER_URL)
            .config("spark.driver.host", DRIVER_HOST_IP)
            .config("spark.sql.shuffle.partitions", "3") # Optimizing for 3 workers
        )
    else:
        print("[*] Running in Local Simulation (4-thread emulation)")
        builder = (
            builder
            .master("local[4]")
            .config("spark.sql.shuffle.partitions", "2")
        )

    spark = (
        builder
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

