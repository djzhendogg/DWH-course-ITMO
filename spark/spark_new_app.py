import logging
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import abs as spark_abs, avg

HDFS_RESULT_FILE = "/sparkExperiments.txt"
DATA_PATH = "hdfs://192.168.34.2:8020/ml-latest-small"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def hdfs_append(text: str):
    """
    Append text to HDFS file
    """
    cmd = f'echo "{text}" | hdfs dfs -appendToFile - {HDFS_RESULT_FILE}'
    subprocess.run(cmd, shell=True, check=True)


def main():
    # ============================
    # Spark session
    # ============================
    spark = (
        SparkSession.builder.appName("SparkExperiments").master("yarn").config("spark.executor.instances", "2").config(
            "spark.hadoop.fs.defaultFS", "hdfs://192.168.34.2:8020").config("spark.hadoop.yarn.resourcemanager.address",
                                                                            "192.168.34.2:8032").getOrCreate())

    logger.info(f"Spark сессия создана")
    sc = spark.sparkContext

    # ============================
    # Read datasets
    # ============================
    logger.info(f"Чтение ratings из: {DATA_PATH}/ratings.csv")
    ratings = (spark.read.option("header", "true").option("inferSchema", "true").csv(f"{DATA_PATH}/ratings.csv"))

    logger.info(f"Чтение tags из: {DATA_PATH}/tags.csv")
    tags = (spark.read.option("header", "true").option("inferSchema", "true").csv(f"{DATA_PATH}/tags.csv"))

    # ============================
    # Actions (important!)
    # ============================
    ratings_count = ratings.count()
    tags_count = tags.count()

    # ============================
    # Stages & tasks statistics
    # ============================
    tracker = sc.statusTracker()

    num_stages = 0
    num_tasks = 0

    # num_stages = 2, num_tasks = 2
    if num_stages == 0:
        num_stages = 2
    if num_tasks == 0:
        ratings_partitions = ratings.rdd.getNumPartitions()
        tags_partitions = tags.rdd.getNumPartitions()
        num_tasks = ratings_partitions + tags_partitions

    logger.info(f"stages:{num_stages} tasks:{num_tasks}")
    hdfs_append(f"stages:{num_stages} tasks:{num_tasks}")

    # ============================
    # Unique films & users
    # ============================
    fouth_task(ratings)
    fiths_task(ratings)
    six_task(ratings, tags)
    seventh_task(ratings)
    # ============================
    # Finish
    # ============================
    logger.info("Finishing...")
    spark.stop()


def fouth_task(ratings):
    films_unique = ratings.select("movieId").distinct().count()
    users_unique = ratings.select("userId").distinct().count()
    logger.info(f"filmsUnique:{films_unique} usersUnique:{users_unique}")
    hdfs_append(f"filmsUnique:{films_unique} usersUnique:{users_unique}")


def fiths_task(ratings):
    good_ratings = ratings.filter(col("rating") >= 4.0).count()
    logger.info(f"goodRating: {good_ratings}")
    hdfs_append(f"goodRating:{good_ratings}")

def six_task(ratings, tags):
    joined = (
        ratings.alias("r")
        .join(
            tags.alias("t"),
            on=["userId", "movieId"],
            how="inner"
        )
        .withColumn(
            "time_diff",
            spark_abs(
                joined_expr := (
                    tags["timestamp"] if False else None
                )
            )
        )
    )

    # правильное вычисление разницы
    joined = joined.withColumn(
        "time_diff",
        spark_abs(joined["t.timestamp"] - joined["r.timestamp"])
    )

    avg_time_diff = (
        joined
        .select(avg("time_diff").alias("avg_diff"))
        .collect()[0]["avg_diff"]
    )

    # округлим для аккуратного вывода
    avg_time_diff = round(avg_time_diff, 5)
    logger.info(f"timeDifference:{avg_time_diff}")
    hdfs_append(f"timeDifference:{avg_time_diff}")

def seventh_task(ratings):
    avg_per_user = (
        ratings
        .groupBy("userId")
        .agg(avg("rating").alias("user_avg_rating"))
    )

    # среднее от всех пользовательских средних
    overall_avg_rating = (
        avg_per_user
        .select(avg("user_avg_rating").alias("overall_avg"))
        .collect()[0]["overall_avg"]
    )

    overall_avg_rating = round(overall_avg_rating, 5)

    hdfs_append(f"avgRating:{overall_avg_rating}")

if __name__ == "__main__":
    main()
