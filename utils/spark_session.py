"""
Spark Session Factory
=====================
Creates and configures a SparkSession for the distributed feed recommendation system.
The session is configured to simulate a 4-node cluster using local[4] mode,
which spawns 4 worker threads to process data in parallel.
"""

from pyspark.sql import SparkSession


def create_spark_session(app_name="DistributedFeedSystem"):
    """
    Create a SparkSession configured for a 4-node local cluster.

    Parameters
    ----------
    app_name : str
        Name of the Spark application (visible in the Spark UI).

    Returns
    -------
    SparkSession
        A configured SparkSession instance.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Simulate a 4-node cluster with 4 parallel executor threads
        .master("local[4]")
        # Allocate driver memory for processing large datasets
        .config("spark.driver.memory", "2g")
        # Enable adaptive query execution for optimized joins and shuffles
        .config("spark.sql.adaptive.enabled", "true")
        # Set shuffle partitions to match our 4-node cluster
        .config("spark.sql.shuffle.partitions", "4")
        # Suppress excessive logging
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    # Set log level to WARN to reduce console noise
    spark.sparkContext.setLogLevel("WARN")

    return spark
