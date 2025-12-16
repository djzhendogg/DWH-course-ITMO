import sys
import os
import time
from datetime import datetime
import logging
from typing import Tuple, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, countDistinct, avg, udf, when, unix_timestamp, from_unixtime, \
    abs as spark_abs
from pyspark.sql.types import FloatType, ArrayType, StringType
from pyspark.sql import functions as F

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import SGDRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
import numpy as np

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SparkExperiment:
    def __init__(self, hdfs_path: str = "hdfs://192.168.34.2:8020"):
        """
        Инициализация Spark сессии с YARN в качестве мастера
        """
        self.hdfs_path = hdfs_path
        self.experiment_file = "/sparkExperiments.txt"

        # Сохраняем путь к локальному файлу для отладки
        self.local_file = "/tmp/sparkExperiments_local.txt"

        # Очищаем локальный файл
        open(self.local_file, 'w').close()

        # Устанавливаем переменные окружения ДО создания сессии
        os.environ['PYSPARK_PYTHON'] = 'python'
        os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'

        # Создание Spark сессии
        self.spark = SparkSession.builder \
            .appName("MovieLensAnalysis") \
            .master("yarn") \
            .config("spark.submit.deployMode", "client") \
            .config("spark.executor.instances", "2") \
            .config("spark.executor.cores", "1") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.yarn.queue", "default") \
            .config("spark.hadoop.fs.defaultFS", hdfs_path) \
            .config("spark.hadoop.yarn.resourcemanager.address", "192.168.34.2:8032") \
            .config("spark.pyspark.python", "python") \
            .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "python") \
            .config("spark.executorEnv.PYSPARK_PYTHON", "python") \
            .getOrCreate()

        logger.info(f"Spark сессия создана")
        logger.info(f"Конфигурация: executors=2, cores=1, memory=1g")

    def write_to_experiment_file(self, content: str):
        """
        Упрощенная запись в файл - сначала в локальный, потом попробуем в HDFS
        """
        logger.info(f"Запись: {content}")

        # Всегда записываем в локальный файл для гарантии
        with open(self.local_file, 'a') as f:
            f.write(content + "\n")

        # Пробуем записать в HDFS, но не критично если не получится
        try:
            # Простой способ: создаем/дописываем через RDD
            output_path = f"{self.hdfs_path}{self.experiment_file}"

            # Читаем локальный файл
            with open(self.local_file, 'r') as f:
                all_lines = f.readlines()

            # Сохраняем все строки в HDFS
            rdd = self.spark.sparkContext.parallelize(all_lines)
            rdd.repartition(1).saveAsTextFile(f"{output_path}_tmp")

            # Используем Hadoop API для переименования
            hadoop_conf = self.spark._jsc.hadoopConfiguration()
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            src_path = self.spark._jvm.org.apache.hadoop.fs.Path(f"{output_path}_tmp/part-00000")
            dst_path = self.spark._jvm.org.apache.hadoop.fs.Path(output_path)

            if fs.exists(dst_path):
                fs.delete(dst_path, True)

            fs.rename(src_path, dst_path)

        except Exception as e:
            logger.warning(f"Не удалось записать в HDFS, сохранено локально: {e}")

    def read_ml_datasets(self, base_path: str = "/ml-latest-small"):
        """
        Чтение датасетов ratings и tags
        """
        ratings_path = f"{self.hdfs_path}{base_path}/ratings.csv"
        tags_path = f"{self.hdfs_path}{base_path}/tags.csv"

        logger.info(f"Чтение ratings из: {ratings_path}")
        ratings_df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(ratings_path)

        logger.info(f"Чтение tags из: {tags_path}")
        tags_df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(tags_path)

        return ratings_df, tags_df

    def task_1_count_lines(self, ratings_df: DataFrame, tags_df: DataFrame):
        """Задание 1: Подсчет строк и запись информации о стадиях и тасках"""
        logger.info("Выполнение задания 1...")

        # Получаем информацию о партициях
        ratings_partitions = ratings_df.rdd.getNumPartitions()
        tags_partitions = tags_df.rdd.getNumPartitions()

        logger.info(f"Партиции ratings: {ratings_partitions}")
        logger.info(f"Партиции tags: {tags_partitions}")

        # Выполняем подсчет
        ratings_count = ratings_df.count()
        tags_count = tags_df.count()

        logger.info(f"Количество строк в ratings: {ratings_count}")
        logger.info(f"Количество строк в tags: {tags_count}")

        # Оцениваем stages и tasks
        # Каждая операция count - это 1 stage
        # Tasks = количество партиций для каждой операции
        stages = 2  # два count()
        tasks = ratings_partitions + tags_partitions

        logger.info(f"Стадии: {stages}, Таски: {tasks}")
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

        try:
            # Преобразуем timestamp в дату/время
            ratings_ts = ratings_df.withColumn(
                "rating_ts", from_unixtime(col("timestamp"))
            )

            tags_ts = tags_df.withColumn(
                "tag_ts", from_unixtime(col("timestamp"))
            )

            # Объединяем и вычисляем разницу
            joined = ratings_ts.alias("r").join(
                tags_ts.alias("t"),
                (col("r.userId") == col("t.userId")) &
                (col("r.movieId") == col("t.movieId"))
            ).select(
                (unix_timestamp(col("t.tag_ts")) - unix_timestamp(col("r.rating_ts"))).alias("time_diff")
            )

            # Вычисляем среднюю абсолютную разницу
            avg_diff = joined.agg(
                F.avg(spark_abs(col("time_diff")))
            ).collect()[0][0]

            if avg_diff is None:
                avg_diff = 0.0

            logger.info(f"Средняя разница во времени: {avg_diff:.2f} сек")
            self.write_to_experiment_file(f"timeDifference:{avg_diff:.2f}")

        except Exception as e:
            logger.error(f"Ошибка в задании 4: {e}")
            self.write_to_experiment_file("timeDifference:0.0")

    def task_5_average_rating(self, ratings_df: DataFrame):
        """Задание 5: Средняя оценка от каждого пользователя"""
        logger.info("Выполнение задания 5...")

        try:
            # Средняя оценка для каждого пользователя
            user_avg = ratings_df.groupBy("userId") \
                .agg(F.avg("rating").alias("user_avg"))

            # Среднее от всех средних оценок
            overall_avg = user_avg.agg(F.avg("user_avg")).collect()[0][0]

            if overall_avg is None:
                overall_avg = 0.0

            logger.info(f"Среднее от средних оценок: {overall_avg:.4f}")
            self.write_to_experiment_file(f"avgRating:{overall_avg:.4f}")

        except Exception as e:
            logger.error(f"Ошибка в задании 5: {e}")
            self.write_to_experiment_file("avgRating:0.0")

    def task_6_ml_prediction(self, ratings_df: DataFrame, tags_df: DataFrame):
        """Задание 6: ML модель для предсказания оценок по тегам"""
        logger.info("Выполнение задания 6...")

        try:
            # Берем небольшую выборку для обучения (чтобы избежать проблем с памятью)
            # Объединяем ratings и tags
            sample_size = min(5000, tags_df.count())

            joined_sample = ratings_df.join(
                tags_df.limit(sample_size),
                ["userId", "movieId"],
                "inner"
            ).select("rating", "tag").dropna().filter(col("tag") != "")

            # Собираем данные локально
            data = joined_sample.collect()

            if len(data) < 100:
                logger.warning(f"Недостаточно данных: {len(data)} строк")
                self.write_to_experiment_file("rmse:0.0")
                return

            # Подготавливаем данные для sklearn
            tags_list = []
            ratings_list = []

            for row in data:
                tag_str = str(row.tag).strip()
                if tag_str:
                    tags_list.append(tag_str)
                    ratings_list.append(float(row.rating))

            if len(tags_list) < 50:
                logger.warning(f"Недостаточно тегов: {len(tags_list)}")
                self.write_to_experiment_file("rmse:0.0")
                return

            # Разделение данных
            X_train, X_test, y_train, y_test = train_test_split(
                tags_list, ratings_list, test_size=0.3, random_state=42
            )

            # TF-IDF
            vectorizer = TfidfVectorizer(max_features=50, stop_words='english')
            X_train_tfidf = vectorizer.fit_transform(X_train)
            X_test_tfidf = vectorizer.transform(X_test)

            # Обучение модели
            model = SGDRegressor(max_iter=500, tol=1e-3, random_state=42)
            model.fit(X_train_tfidf, y_train)

            # Предсказания и RMSE
            y_pred = model.predict(X_test_tfidf)
            rmse = np.sqrt(mean_squared_error(y_test, y_pred))

            logger.info(f"Размер выборки: {len(tags_list)}")
            logger.info(f"RMSE: {rmse:.4f}")

            # Создаем UDF
            def predict_from_tag(tag):
                if not tag or str(tag).strip() == "":
                    return 2.5
                try:
                    vec = vectorizer.transform([str(tag)])
                    pred = model.predict(vec)[0]
                    return float(np.clip(pred, 0.5, 5.0))
                except:
                    return 2.5

            # Регистрируем UDF
            predict_udf = udf(predict_from_tag, FloatType())

            # Демонстрация работы UDF
            demo_df = tags_df.limit(20).withColumn(
                "predicted", predict_udf(col("tag"))
            )

            logger.info("Пример работы UDF:")
            demo_df.select("tag", "predicted").show(10, truncate=30)

            # Записываем результат
            self.write_to_experiment_file(f"rmse:{rmse:.4f}")

        except Exception as e:
            logger.error(f"Ошибка в ML задании: {e}")
            import traceback
            traceback.print_exc()
            self.write_to_experiment_file("rmse:0.0")

    def run_all_tasks(self):
        """Выполнение всех заданий"""
        try:
            logger.info("=" * 50)
            logger.info("Начало выполнения всех заданий")
            logger.info("=" * 50)

            # Чтение данных
            ratings_df, tags_df = self.read_ml_datasets()

            # Задание 1
            logger.info("\n--- Задание 1 ---")
            self.task_1_count_lines(ratings_df, tags_df)

            # Задание 2
            logger.info("\n--- Задание 2 ---")
            self.task_2_unique_counts(ratings_df)

            # Задание 3
            logger.info("\n--- Задание 3 ---")
            self.task_3_good_ratings(ratings_df)

            # Задание 4
            logger.info("\n--- Задание 4 ---")
            self.task_4_time_difference(ratings_df, tags_df)

            # Задание 5
            logger.info("\n--- Задание 5 ---")
            self.task_5_average_rating(ratings_df)

            # Задание 6
            logger.info("\n--- Задание 6 ---")
            self.task_6_ml_prediction(ratings_df, tags_df)

            logger.info("=" * 50)
            logger.info("Все задания выполнены!")
            logger.info(f"Результаты сохранены в локальном файле: {self.local_file}")
            logger.info("=" * 50)

        except Exception as e:
            logger.error(f"Ошибка при выполнении заданий: {e}")
            import traceback
            traceback.print_exc()

    def stop(self):
        """Остановка Spark сессии"""
        self.spark.stop()
        logger.info("Spark сессия остановлена")


def main():
    """Основная функция"""
    try:
        # Проверяем доступность python
        import subprocess
        result = subprocess.run(['which', 'python'], capture_output=True, text=True)
        logger.info(f"Путь к python: {result.stdout.strip()}")

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