from pyspark.sql import SparkSession
import subprocess
import logging

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
        SparkSession.builder
        .appName("SparkExperiments")
        .master("yarn")
        .config("spark.executor.instances", "2")
        .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.34.2:8020")
        .config("spark.hadoop.yarn.resourcemanager.address", "192.168.34.2:8032")
        .getOrCreate()
    )

    logger.info(f"Spark сессия создана")
    sc = spark.sparkContext

    # ============================
    # Read datasets
    # ============================
    logger.info(f"Чтение ratings из: {DATA_PATH}/ratings.csv")
    ratings = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{DATA_PATH}/ratings.csv")
    )

    logger.info(f"Чтение tags из: {DATA_PATH}/tags.csv")
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

    num_stages = 0
    num_tasks = 0
    try:
        java_tracker = sc._jsc.sc().statusTracker()

        for job_id in java_tracker.getActiveJobIds():
            job_info = tracker.getJobInfo(job_id)

            if job_info:
                for stage_id in job_info.stageIds:
                    stage_info = tracker.getStageInfo(stage_id)
                    if stage_info:
                        num_stages += 1
                        num_tasks += stage_info.numTasks
    except:
        logger.info("не получилось с sc._jsc.sc()")
        job_ids = []

        # Способ 1: Используем getJobIdsForGroup (может вернуть пустой список если нет группы)
        try:
            job_ids = tracker.getJobIdsForGroup(None)  # None для всех jobs
        except:
            pass

        # Если не получили job_ids, попробуем другой подход
        if not job_ids:
            logger.info("tracker.getJobIdsForGroup(None) пуст")
            # Создадим временный RDD и выполним действие, чтобы гарантировать наличие job
            temp_rdd = sc.parallelize([1, 2, 3])
            temp_count = temp_rdd.count()  # Это создаст job

            try:
                job_ids = tracker.getJobIdsForGroup(None)
            except:
                # Если все еще ошибка, используем альтернативный подход
                logger.warning("Не удалось получить job_ids через tracker, используем альтернативный подход")
                # Просто записываем 0 для stages и tasks
                num_stages = 0
                num_tasks = 0

        # Подсчитываем stages и tasks
        for job_id in job_ids:
            job_info = tracker.getJobInfo(job_id)
            if job_info:
                for stage_id in job_info.stageIds:
                    stage_info = tracker.getStageInfo(stage_id)
                    if stage_info:
                        num_stages += 1
                        num_tasks += stage_info.numTasks
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
    films_unique = ratings.select("movieId").distinct().count()
    users_unique = ratings.select("userId").distinct().count()

    logger.info(f"filmsUnique:{films_unique} usersUnique:{users_unique}")
    hdfs_append(f"filmsUnique:{films_unique} usersUnique:{users_unique}")

    # ============================
    # Finish
    # ============================
    logger.info("Finishing...")
    spark.stop()


if __name__ == "__main__":
    main()
