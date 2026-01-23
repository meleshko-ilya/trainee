from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()

def get_spark():
    driver_path = os.getenv("PG_JAR")
    spark_tmp_path = os.getenv("SPARK_TMP_PATH")

    if not driver_path or not os.path.exists(driver_path):
        raise FileNotFoundError(f"PostgreSQL JDBC JAR not found: {driver_path}")

    driver_path_abs = os.path.abspath(driver_path)
    spark_tmp_path_abs = os.path.abspath(spark_tmp_path)

    return (
        SparkSession.builder
        .appName("PySpark app")
        .config("spark.jars", driver_path_abs)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", spark_tmp_path_abs)
        .getOrCreate()
    )
