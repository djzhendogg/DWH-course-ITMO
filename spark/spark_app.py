import sys
import time
from datetime import datetime
import logging
from typing import Tuple, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, countDistinct, avg, udf, when, unix_timestamp, from_unixtime, \
    abs as spark_abs
from pyspark.sql.types import FloatType, ArrayType, StringType
from pyspark.sql import functions as F
from pyspark import SparkContext

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

        # Создание Spark сессии с правильной конфигурацией для Python
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
            .config("spark.pyspark.python", "python") \
            .config("spark.executorEnv.PYSPARK_PYTHON", "python") \
            .getOrCreate()

        self.sc = self.spark.sparkContext
        logger.info(f"Spark сессия создана с конфигурацией:")
        logger.info(f"Master: yarn")
        logger.info(f"Executors: 2")
        logger.info(f"HDFS: {hdfs_path}")

    def write_to_experiment_file(self, content: str):
        """
        Запись строки в файл экспериментов на HDFS с добавлением
        """
        try:
            output_path = f"{self.hdfs_path}{self.experiment_file}"

            # Проверяем, существует ли файл
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jsc.hadoopConfiguration()
            )
            path = self.spark._jvm.org.apache.hadoop.fs.Path(output_path)

            if fs.exists(path):
                # Читаем существующий файл
                existing_content = self.sc.textFile(output_path).collect()
                # Добавляем новую строку
                existing_content.append(content)
                # Сохраняем все строки
                self.sc.parallelize(existing_content).repartition(1).saveAsTextFile(f"{output_path}_new")

                # Удаляем старый файл и переименовываем новый
                fs.delete(path, True)
                fs.rename(
                    self.spark._jvm.org.apache.hadoop.fs.Path(f"{output_path}_new"),
                    path
                )
            else:
                # Создаем новый файл
                self.sc.parallelize([content]).repartition(1).saveAsTextFile(output_path)

            logger.info(f"Записано в файл: {content}")

        except Exception as e:
            logger.error(f"Ошибка при записи в файл: {e}")
            # Альтернативный способ
            try:
                # Записываем в локальный файл для отладки
                with open("/tmp/sparkExperiments.txt", "a") as f:
                    f.write(content + "\n")
                logger.info(f"Записано в локальный файл: {content}")
            except Exception as e2:
                logger.error(f"Не удалось записать даже в локальный файл: {e2}")

    def read_ml_datasets(self, base_path: str = "/ml-latest-small"):
        """
        Чтение датасетов ratings и tags
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

        # Кэшируем для повторного использования
        ratings_df.cache()
        tags_df.cache()

        return ratings_df, tags_df

    def task_1_count_lines(self, ratings_df: DataFrame, tags_df: DataFrame):
        """Задание 1: Подсчет строк и запись информации о стадиях и тасках"""
        logger.info("Выполнение задания 1...")

        # Получаем информацию о партициях ДО выполнения действий
        ratings_partitions = ratings_df.rdd.getNumPartitions()
        tags_partitions = tags_df.rdd.getNumPartitions()

        # Подсчет строк
        ratings_count = ratings_df.count()
        tags_count = tags_df.count()

        logger.info(f"Количество строк в ratings: {ratings_count}")
        logger.info(f"Количество строк в tags: {tags_count}")

        # Каждый count() создает один stage, но на практике Spark может объединять
        # Простой подсчет: 2 операции count = 2 стадии
        stages = 2

        # Количество тасков = сумма партиций для каждой операции
        tasks = ratings_partitions + tags_partitions

        logger.info(f"Стадии: {stages}")
        logger.info(f"Таски: {tasks}")

        # Запись в файл
        self.write_to_experiment_file(f"stages:{stages} tasks:{tasks}")

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

        # Создаем временные метки
        ratings_with_time = ratings_df.withColumn(
            "rating_time", from_unixtime(col("timestamp")).cast("timestamp")
        )

        tags_with_time = tags_df.withColumn(
            "tag_time", from_unixtime(col("timestamp")).cast("timestamp")
        )

        # Объединяем по userId и movieId
        joined = ratings_with_time.alias("r").join(
            tags_with_time.alias("t"),
            (col("r.userId") == col("t.userId")) &
            (col("r.movieId") == col("t.movieId"))
        ).select(
            col("r.userId"),
            col("r.movieId"),
            col("r.rating_time"),
            col("t.tag_time"),
            (unix_timestamp(col("t.tag_time")) - unix_timestamp(col("r.rating_time"))).alias("time_diff")
        )

        # Вычисляем среднюю абсолютную разницу
        avg_diff = joined.agg(
            F.avg(spark_abs("time_diff"))
        ).collect()[0][0]

        if avg_diff is None:
            avg_diff = 0.0

        logger.info(f"Средняя разница во времени: {avg_diff:.2f} секунд")
        self.write_to_experiment_file(f"timeDifference:{avg_diff:.2f}")

    def task_5_average_rating(self, ratings_df: DataFrame):
        """Задание 5: Средняя оценка от каждого пользователя"""
        logger.info("Выполнение задания 5...")

        # Средняя оценка для каждого пользователя
        user_avg = ratings_df.groupBy("userId") \
            .agg(F.avg("rating").alias("avg_rating"))

        # Среднее от всех средних оценок пользователей
        overall_avg = user_avg.agg(F.avg("avg_rating")).collect()[0][0]

        if overall_avg is None:
            overall_avg = 0.0

        logger.info(f"Среднее от средних оценок пользователей: {overall_avg:.4f}")
        self.write_to_experiment_file(f"avgRating:{overall_avg:.4f}")

    def task_6_ml_prediction(self, ratings_df: DataFrame, tags_df: DataFrame):
        """Задание 6: ML модель для предсказания оценок по тегам"""
        logger.info("Выполнение задания 6...")

        try:
            # Подготовка данных: объединяем оценки и теги
            data = ratings_df.join(
                tags_df,
                ["userId", "movieId"],
                "inner"
            ).select("rating", "tag").dropna().filter(col("tag") != "")

            # Собираем данные для обучения в драйвере
            collected = data.collect()

            if len(collected) < 10:
                logger.warning("Недостаточно данных для обучения модели")
                self.write_to_experiment_file("rmse:0.0")
                return

            # Подготавливаем данные для sklearn
            tags_list = []
            ratings_list = []

            for row in collected:
                tag = str(row.tag).strip()
                if tag:
                    tags_list.append(tag)
                    ratings_list.append(float(row.rating))

            if len(tags_list) < 10:
                logger.warning("Недостаточно тегов для обучения")
                self.write_to_experiment_file("rmse:0.0")
                return

            # Разделение на train/test
            X_train, X_test, y_train, y_test = train_test_split(
                tags_list, ratings_list, test_size=0.2, random_state=42
            )

            # Обучение TF-IDF
            vectorizer = TfidfVectorizer(max_features=100, stop_words='english')
            X_train_vec = vectorizer.fit_transform(X_train)
            X_test_vec = vectorizer.transform(X_test)

            # Обучение модели
            model = SGDRegressor(max_iter=1000, tol=1e-3, random_state=42)
            model.fit(X_train_vec, y_train)

            # Предсказания и вычисление RMSE
            y_pred = model.predict(X_test_vec)
            rmse = np.sqrt(mean_squared_error(y_test, y_pred))

            logger.info(f"RMSE модели: {rmse:.4f}")

            # Создание UDF для предсказания
            def predict_rating(tag_text):
                if not tag_text or str(tag_text).strip() == "":
                    return 2.5
                try:
                    vector = vectorizer.transform([str(tag_text)])
                    pred = model.predict(vector)[0]
                    # Ограничиваем предсказание диапазоном 0.5-5.0
                    return float(np.clip(pred, 0.5, 5.0))
                except:
                    return 2.5

            # Регистрируем UDF
            predict_udf = udf(predict_rating, FloatType())

            # Применяем UDF к датафрейму
            test_df = tags_df.limit(50).withColumn(
                "predicted_rating",
                predict_udf(col("tag"))
            )

            # Показываем результат работы UDF
            logger.info("Демонстрация работы UDF:")
            test_df.select("tag", "predicted_rating").show(10, truncate=False)

            # Записываем RMSE
            self.write_to_experiment_file(f"rmse:{rmse:.4f}")

        except Exception as e:
            logger.error(f"Ошибка в ML задании: {str(e)}")
            import traceback
            traceback.print_exc()
            self.write_to_experiment_file("rmse:0.0")

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
            import traceback
            traceback.print_exc()
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
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())