from pyspark.sql import SparkSession
import subprocess


HDFS_RESULT_FILE = "/sparkExperiments.txt"
DATA_PATH = "hdfs://192.168.34.2:8020/ml-latest-small"


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
        SparkSession.builder
        .appName("SparkExperiments")
        .master("yarn")
        .config("spark.executor.instances", "2")
        .getOrCreate()
    )

    sc = spark.sparkContext

    # ============================
    # Read datasets
    # ============================
    ratings = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{DATA_PATH}/ratings.csv")
    )

    tags = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{DATA_PATH}/tags.csv")
    )

    # ============================
    # Actions (important!)
    # ============================
    ratings_count = ratings.count()
    tags_count = tags.count()

    # ============================
    # Stages & tasks statistics
    # ============================
    tracker = sc.statusTracker()

    stage_ids = tracker.getStageIdsForAllJobs()

    num_stages = len(stage_ids)
    num_tasks = 0

    for stage_id in stage_ids:
        info = tracker.getStageInfo(stage_id)
        if info is not None:
            num_tasks += info.numTasks()

    hdfs_append(f"stages:{num_stages} tasks:{num_tasks}")

    # ============================
    # Unique films & users
    # ============================
    films_unique = ratings.select("movieId").distinct().count()
    users_unique = ratings.select("userId").distinct().count()

    hdfs_append(f"filmsUnique:{films_unique} usersUnique:{users_unique}")

    # ============================
    # Finish
    # ============================
    spark.stop()


if __name__ == "__main__":
    main()
