import sys
import time
from datetime import datetime
import logging
from typing import Tuple, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, countDistinct, avg, udf, when
from pyspark.sql.types import FloatType, ArrayType, StringType
from pyspark.sql import functions as F

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import SGDRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
import numpy as np

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkExperiment:
    def __init__(self, hdfs_path: str = "hdfs://192.168.34.2:8020"):
        """
        Инициализация Spark сессии с YARN в качестве мастера

        Args:
            hdfs_path: Путь к HDFS
        """
        self.hdfs_path = hdfs_path
        self.experiment_file = "/sparkExperiments.txt"

        # Создание Spark сессии
        self.spark = SparkSession.builder \
            .appName("HadoopSparkExperiment") \
            .master("yarn") \
            .config("spark.submit.deployMode", "client") \
            .config("spark.executor.instances", "2") \
            .config("spark.executor.cores", "2") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.yarn.queue", "default") \
            .config("spark.hadoop.fs.defaultFS", hdfs_path) \
            .config("spark.hadoop.yarn.resourcemanager.address", "192.168.34.2:8032") \
            .getOrCreate()

        logger.info(f"Spark сессия создана с конфигурацией:")
        logger.info(f"Master: yarn")
        logger.info(f"Executors: 2")
        logger.info(f"HDFS: {hdfs_path}")

    def write_to_experiment_file(self, content: str):
        """
        Запись строки в файл экспериментов на HDFS

        Args:
            content: Строка для записи
        """
        try:
            # Чтение существующего содержимого
            try:
                existing_content = self.spark.sparkContext.textFile(
                    f"{self.hdfs_path}{self.experiment_file}"
                ).collect()
                existing_content = "\n".join(existing_content) + "\n"
            except:
                existing_content = ""

            # Добавление новой строки
            new_content = existing_content + content + "\n"

            # Запись обратно в HDFS
            self.spark.sparkContext.parallelize([new_content]) \
                .saveAsTextFile(f"{self.hdfs_path}{self.experiment_file}.tmp")

            # Переименование файла
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jsc.hadoopConfiguration()
            )
            src_path = self.spark._jvm.org.apache.hadoop.fs.Path(
                f"{self.hdfs_path}{self.experiment_file}.tmp/part-00000"
            )
            dst_path = self.spark._jvm.org.apache.hadoop.fs.Path(
                f"{self.hdfs_path}{self.experiment_file}"
            )

            if fs.exists(dst_path):
                fs.delete(dst_path, True)

            fs.rename(src_path, dst_path)
            logger.info(f"Записано в файл: {content}")

        except Exception as e:
            logger.error(f"Ошибка при записи в файл: {e}")

    def read_ml_datasets(self, base_path: str = "/ml-latest-small"):
        """
        Чтение датасетов ratings и tags

        Args:
            base_path: Базовый путь к данным на HDFS

        Returns:
            Кортеж с датафреймами (ratings_df, tags_df)
        """
        ratings_path = f"{self.hdfs_path}{base_path}/ratings.csv"
        tags_path = f"{self.hdfs_path}{base_path}/tags.csv"

        logger.info(f"Чтение ratings из: {ratings_path}")
        logger.info(f"Чтение tags из: {tags_path}")

        ratings_df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(ratings_path)

        tags_df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(tags_path)

        return ratings_df, tags_df

    def task_1_count_lines(self, ratings_df: DataFrame, tags_df: DataFrame):
        """Задание 1: Подсчет строк и запись информации о стадиях и тасках"""
        logger.info("Выполнение задания 1...")

        # Подсчет строк с действием для запуска вычислений
        ratings_count = ratings_df.count()
        tags_count = tags_df.count()

        logger.info(f"Количество строк в ratings: {ratings_count}")
        logger.info(f"Количество строк в tags: {tags_count}")

        # Получение информации о стадиях и тасках
        stages = self.spark.sparkContext._jsc.sc().getStatusStore().getStageInfos()
        total_stages = len(stages)

        total_tasks = sum(stage.numTasks() for stage in stages)

        # Запись в файл
        self.write_to_experiment_file(f"stages:{total_stages} tasks:{total_tasks}")

    def task_2_unique_counts(self, ratings_df: DataFrame):
        """Задание 2: Подсчет уникальных фильмов и пользователей"""
        logger.info("Выполнение задания 2...")

        unique_films = ratings_df.select("movieId").distinct().count()
        unique_users = ratings_df.select("userId").distinct().count()

        logger.info(f"Уникальных фильмов: {unique_films}")
        logger.info(f"Уникальных пользователей: {unique_users}")

        self.write_to_experiment_file(f"filmsUnique:{unique_films} usersUnique:{unique_users}")

    def task_3_good_ratings(self, ratings_df: DataFrame):
        """Задание 3: Подсчет оценок >= 4.0"""
        logger.info("Выполнение задания 3...")

        good_ratings = ratings_df.filter(col("rating") >= 4.0).count()

        logger.info(f"Оценок >= 4.0: {good_ratings}")
        self.write_to_experiment_file(f"goodRating:{good_ratings}")

    def task_4_time_difference(self, ratings_df: DataFrame, tags_df: DataFrame):
        """Задание 4: Разница во времени между тегированием и оценкой"""
        logger.info("Выполнение задания 4...")

        # Приведение временных меток к единому формату
        ratings_with_ts = ratings_df.withColumn(
            "rating_timestamp",
            F.from_unixtime(col("timestamp")).cast("timestamp")
        )

        tags_with_ts = tags_df.withColumn(
            "tag_timestamp",
            F.from_unixtime(col("timestamp")).cast("timestamp")
        )

        # Объединение данных
        joined_df = ratings_with_ts.join(
            tags_with_ts,
            ["userId", "movieId"],
            "inner"
        )

        # Вычисление разницы во времени в секундах
        time_diff_df = joined_df.withColumn(
            "time_diff_seconds",
            F.abs(F.unix_timestamp("tag_timestamp") - F.unix_timestamp("rating_timestamp"))
        )

        # Вычисление средней разницы
        avg_time_diff = time_diff_df.agg(
            avg("time_diff_seconds").alias("avg_diff")
        ).collect()[0]["avg_diff"]

        if avg_time_diff is None:
            avg_time_diff = 0.0

        logger.info(f"Средняя разница во времени: {avg_time_diff:.2f} секунд")
        self.write_to_experiment_file(f"timeDifference:{avg_time_diff:.2f}")

    def task_5_average_rating(self, ratings_df: DataFrame):
        """Задание 5: Средняя оценка от каждого пользователя"""
        logger.info("Выполнение задания 5...")

        # Средняя оценка для каждого пользователя
        user_avg_ratings = ratings_df.groupBy("userId") \
            .agg(avg("rating").alias("user_avg_rating"))

        # Среднее от всех усредненных оценок
        overall_avg = user_avg_ratings.agg(
            avg("user_avg_rating").alias("overall_avg")
        ).collect()[0]["overall_avg"]

        logger.info(f"Среднее от усредненных оценок: {overall_avg:.4f}")
        self.write_to_experiment_file(f"avgRating:{overall_avg:.4f}")

    def task_6_ml_prediction(self, ratings_df: DataFrame, tags_df: DataFrame):
        """Задание 6: ML модель для предсказания оценок по тегам"""
        logger.info("Выполнение задания 6...")

        # Подготовка данных для обучения
        joined_data = ratings_df.join(
            tags_df,
            ["userId", "movieId"],
            "inner"
        ).select("rating", "tag").dropna()

        # Сбор данных в драйвере для обучения (для простоты)
        collected_data = joined_data.collect()

        if len(collected_data) < 100:
            logger.warning(f"Недостаточно данных для обучения: {len(collected_data)} строк")
            self.write_to_experiment_file("rmse:0.0")
            return

        # Подготовка данных для sklearn
        tags_list = [str(row.tag) for row in collected_data]
        ratings_list = [float(row.rating) for row in collected_data]

        # Разделение на train/test
        X_train, X_test, y_train, y_test = train_test_split(
            tags_list, ratings_list, test_size=0.2, random_state=42
        )

        # Обучение TF-IDF и модели
        vectorizer = TfidfVectorizer(max_features=1000)
        X_train_tfidf = vectorizer.fit_transform(X_train)
        X_test_tfidf = vectorizer.transform(X_test)

        model = SGDRegressor(max_iter=1000, tol=1e-3, random_state=42)
        model.fit(X_train_tfidf, y_train)

        # Предсказания
        y_pred = model.predict(X_test_tfidf)

        # Вычисление RMSE
        mse = mean_squared_error(y_test, y_pred)
        rmse = np.sqrt(mse)

        logger.info(f"RMSE модели: {rmse:.4f}")

        # Создание UDF для предсказания
        def predict_rating_udf(tags_list):
            if not tags_list:
                return 3.0
            # Для простоты берем первый тег
            tag = str(tags_list[0]) if isinstance(tags_list, list) else str(tags_list)
            if not tag.strip():
                return 3.0
            try:
                tag_vector = vectorizer.transform([tag])
                prediction = model.predict(tag_vector)[0]
                # Ограничение предсказания диапазоном оценок
                return float(max(0.5, min(5.0, prediction)))
            except:
                return 3.0

        # Регистрация UDF в Spark
        predict_udf = udf(predict_rating_udf, FloatType())

        # Применение UDF к данным
        tags_with_predictions = tags_df.withColumn(
            "predicted_rating",
            predict_udf(col("tag"))
        )

        # Демонстрация работы UDF
        logger.info("Пример предсказаний UDF:")
        tags_with_predictions.select("tag", "predicted_rating").show(10, truncate=False)

        # Запись RMSE в файл
        self.write_to_experiment_file(f"rmse:{rmse:.4f}")

    def run_all_tasks(self):
        """Выполнение всех заданий"""
        try:
            logger.info("Начало выполнения всех заданий...")

            # Чтение данных
            ratings_df, tags_df = self.read_ml_datasets()

            # Задание 1
            self.task_1_count_lines(ratings_df, tags_df)

            # Задание 2
            self.task_2_unique_counts(ratings_df)

            # Задание 3
            self.task_3_good_ratings(ratings_df)

            # Задание 4
            self.task_4_time_difference(ratings_df, tags_df)

            # Задание 5
            self.task_5_average_rating(ratings_df)

            # Задание 6
            self.task_6_ml_prediction(ratings_df, tags_df)

            logger.info("Все задания успешно выполнены!")

        except Exception as e:
            logger.error(f"Ошибка при выполнении заданий: {e}")
            raise

    def stop(self):
        """Остановка Spark сессии"""
        self.spark.stop()
        logger.info("Spark сессия остановлена")


def main():
    """Основная функция"""
    try:
        # Создание и запуск эксперимента
        experiment = SparkExperiment()
        experiment.run_all_tasks()
        experiment.stop()

        logger.info("Программа завершена успешно!")
        return 0

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
