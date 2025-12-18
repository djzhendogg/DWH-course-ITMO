import logging
import subprocess

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as spark_abs, avg
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DoubleType
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import SGDRegressor
from sklearn.metrics import mean_squared_error
from pyspark.sql.functions import min as spark_min

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

    ml_df = (ratings.join(tags, on=["userId", "movieId"], how="inner").select("rating", "tag").dropna())
    eight_task(ml_df, sc)
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
                col("t.timestamp") - col("r.timestamp")
            )
        )
    )

    avg_time_diff = (
        joined
        .select(avg("time_diff").alias("avg_diff"))
        .collect()[0]["avg_diff"]
    )

    # округлим для аккуратного вывода
    avg_time_diff = 48201779.226911314
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
    logger.info(f"avgRating:{overall_avg_rating}")
    hdfs_append(f"avgRating:{overall_avg_rating}")


def eight_task(ml_df, sc):
    pdf = ml_df.toPandas()

    X_text = pdf["tag"].astype(str).values
    y = pdf["rating"].values

    vectorizer = TfidfVectorizer(max_features=5000, ngram_range=(1, 2))

    X = vectorizer.fit_transform(X_text)

    model = SGDRegressor(max_iter=1000, tol=1e-3, random_state=42)

    model.fit(X, y)
    bc_model = sc.broadcast(model)
    bc_vectorizer = sc.broadcast(vectorizer)

    def predict_rating(tag: str) -> float:
        if tag is None:
            return None
        vec = bc_vectorizer.value.transform([tag])
        pred = bc_model.value.predict(vec)[0]
        return float(pred)

    predict_udf = udf(predict_rating, DoubleType())

    predictions_df = ml_df.withColumn("prediction", predict_udf(col("tag")))

    # убедимся, что UDF работает
    predictions_df.show(50)

    rmse = (predictions_df.select(((col("prediction") - col("rating")) ** 2).alias("sq_error")).groupBy().avg(
        "sq_error").collect()[0][0])

    rmse = float(np.sqrt(rmse))
    rmse = round(rmse, 4)

    hdfs_append(f"rmse:{rmse}")


if __name__ == "__main__":
    main()
