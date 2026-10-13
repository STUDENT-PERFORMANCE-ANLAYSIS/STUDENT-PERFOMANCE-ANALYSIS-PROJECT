# src/utils.py
"""
Utility module for initializing and managing the Apache Spark session.

Why this file exists:
In Apache Spark, the SparkSession is the unified entry point for programming with 
Spark's Dataset and DataFrame APIs. Since initializing a SparkSession is resource-intensive 
and should only be done once per application runtime (singleton pattern), this module 
ensures a single, configured SparkSession is shared across all other modules.
It also sanitizes environment variables (JAVA_HOME, PYSPARK_PYTHON) so that Spark runs 
reliably across system reboots and independent terminals.

Viva Tips:
- What is SparkSession? It is the entry point introduced in Spark 2.0 that combines SparkContext, SQLContext, and HiveContext.
- What does 'local[*]' mean? It tells Spark to run locally using all available CPU cores on your machine.
- What is spark.driver.host configuration? It binds the driver to localhost, which avoids networking/firewall issues on Windows machines.
"""

import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession

def setup_spark_environment():
    """
    Sanitizes environment variables (JAVA_HOME, PYSPARK_PYTHON) for PySpark JVM execution.
    Ensures paths work reliably across Windows/Linux/WSL after system restarts.
    """
    project_root = Path(__file__).resolve().parent.parent

    # Candidate locations for portable Java JDK inside project
    candidates = [
        project_root / ".jdk-11" / "jdk-11.0.21+9",
    ]
    for portable_java in candidates:
        java_exe = portable_java / "bin" / ("java.exe" if os.name == 'nt' else "java")
        if java_exe.exists():
            os.environ["JAVA_HOME"] = str(portable_java)
            bin_dir = str(portable_java / "bin")
            if bin_dir not in os.environ.get("PATH", ""):
                os.environ["PATH"] = bin_dir + os.path.pathsep + os.environ.get("PATH", "")
            break

    # Sanitize JAVA_HOME to remove trailing slashes/backslashes and outer quotes.
    # Trailing backslashes can escape quotes in Windows command invocation, crashing the PySpark Java gateway.
    if "JAVA_HOME" in os.environ and os.environ["JAVA_HOME"]:
        os.environ["JAVA_HOME"] = os.environ["JAVA_HOME"].strip().rstrip(r"\/").strip('"')

    # Ensure Spark workers use the exact same Python executable as the current environment
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Set fallback HADOOP_HOME if unset to prevent Windows winutils missing warnings
    if "HADOOP_HOME" not in os.environ or not os.environ["HADOOP_HOME"]:
        os.environ["HADOOP_HOME"] = str(project_root)
        dummy_bin = project_root / "bin"
        if not dummy_bin.exists():
            dummy_bin.mkdir(exist_ok=True)


def get_spark_session(app_name="StudentPerformanceAnalysis"):
    """
    Creates or retrieves an existing SparkSession configured for local execution.
    
    Parameters:
    app_name (str): Name of the Spark application visible on the Spark Web UI (port 4040).
    
    Returns:
    SparkSession: The active Spark Session.
    """
    setup_spark_environment()

    cores = max(2, min(os.cpu_count() or 4, 4))
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.warehouse.dir", "spark-warehouse") \
        .config("spark.sql.shuffle.partitions", str(cores)) \
        .config("spark.default.parallelism", str(cores)) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    
    # Set log level to WARN to prevent the console from being flooded with INFO logs.
    # This keeps our terminal clean during student viva demonstrations.
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

