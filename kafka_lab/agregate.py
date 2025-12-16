import psycopg2
import os

from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv("env.conf")

class PostgresAggregator:
    def __init__(self):
        """
        Инициализация подключения к базе данных
        :param db_params: параметры подключения к БД
        """
        self.connection = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)

    def get_hourly_ranges(self):
        """
        Получаем все уникальные часы из данных
        Возвращает список кортежей (hour_start, hour_end)
        """
        query = """
        SELECT 
            DATE_TRUNC('hour', created_at) as hour_start,
            DATE_TRUNC('hour', created_at) + INTERVAL '1 hour' as hour_end
        FROM (
            SELECT created_at FROM raw_events
            UNION ALL
            SELECT created_at FROM dm_posts
        ) all_events
        GROUP BY DATE_TRUNC('hour', created_at)
        ORDER BY hour_start
        """

        self.cursor.execute(query)
        return self.cursor.fetchall()

    def aggregate_hourly_post_stats(self, hour_start, hour_end):
        """
        Агрегирует статистику по постам за указанный час
        """
        query = """
        WITH hourly_events AS (
            SELECT 
                post_id,
                event_type,
                COUNT(*) as count
            FROM raw_events
            WHERE created_at >= %s 
              AND created_at < %s
            GROUP BY post_id, event_type
        ),
        post_likes AS (
            SELECT 
                post_id,
                SUM(count) as likes_count
            FROM hourly_events
            WHERE event_type = 'like'
            GROUP BY post_id
        ),
        post_reposts AS (
            SELECT 
                post_id,
                SUM(count) as reposts_count
            FROM hourly_events
            WHERE event_type = 'repost'
            GROUP BY post_id
        ),
        all_posts_in_hour AS (
            SELECT DISTINCT post_id
            FROM hourly_events
        )
        SELECT 
            a.post_id,
            COALESCE(l.likes_count, 0) as likes_count,
            COALESCE(r.reposts_count, 0) as reposts_count
        FROM all_posts_in_hour a
        LEFT JOIN post_likes l ON a.post_id = l.post_id
        LEFT JOIN post_reposts r ON a.post_id = r.post_id
        """

        self.cursor.execute(query, (hour_start, hour_end))
        return self.cursor.fetchall()

    def aggregate_hourly_user_stats(self, hour_start, hour_end):
        """
        Агрегирует статистику по пользователям за указанный час
        """
        query = """
            WITH hourly_raw_events AS (
                SELECT 
                    user_id,
                    event_type,
                    post_id,
                    COUNT(*) as count
                FROM raw_events
                WHERE created_at >= %s 
                  AND created_at < %s
                GROUP BY user_id, event_type, post_id
            ),
            hourly_posts AS (
                SELECT 
                    post_id,
                    author_id as user_id
                FROM dm_posts
                WHERE created_at >= %s 
                  AND created_at < %s
            ),
            likes_given AS (
                SELECT 
                    user_id,
                    SUM(count) as likes_given
                FROM hourly_raw_events
                WHERE event_type = 'like'
                GROUP BY user_id
            ),
            reposts_made AS (
                SELECT 
                    user_id,
                    SUM(count) as reposts_made
                FROM hourly_raw_events
                WHERE event_type = 'repost'
                GROUP BY user_id
            ),
            post_events_for_user AS (
                SELECT 
                    p.author_id as user_id,
                    e.event_type,
                    SUM(e.count) as count
                FROM raw_events e
                JOIN dm_posts p ON e.post_id = p.post_id
                WHERE e.created_at >= %s 
                  AND e.created_at < %s
                GROUP BY p.author_id, e.event_type
            ),
            likes_received AS (
                SELECT 
                    user_id,
                    SUM(count) as likes_received
                FROM post_events_for_user
                WHERE event_type = 'like'
                GROUP BY user_id
            ),
            reposts_received AS (
                SELECT 
                    user_id,
                    SUM(count) as reposts_received
                FROM post_events_for_user
                WHERE event_type = 'repost'
                GROUP BY user_id
            ),
            -- 3. Все пользователи, которые были активны в этот час
            users_with_activity AS (
                -- Пользователи, которые что-то делали
                SELECT DISTINCT user_id FROM hourly_raw_events
                UNION
                -- Пользователи, чьи посты что-то получили
                SELECT DISTINCT user_id FROM post_events_for_user
                UNION
                -- Пользователи, которые создали посты
                SELECT DISTINCT user_id FROM hourly_posts
            )
            SELECT 
                u.user_id,
                COALESCE(lg.likes_given, 0) as likes_given,
                COALESCE(lr.likes_received, 0) as likes_received,
                COALESCE(rm.reposts_made, 0) as reposts_made,
                COALESCE(rr.reposts_received, 0) as reposts_received,
                (COALESCE(rm.reposts_made, 0) * 37 + COALESCE(lg.likes_given, 0) * 3) as engagement_score
            FROM users_with_activity u
            LEFT JOIN likes_given lg ON u.user_id = lg.user_id
            LEFT JOIN likes_received lr ON u.user_id = lr.user_id
            LEFT JOIN reposts_made rm ON u.user_id = rm.user_id
            LEFT JOIN reposts_received rr ON u.user_id = rr.user_id
            """

        self.cursor.execute(query, (hour_start, hour_end, hour_start, hour_end, hour_start, hour_end))
        return self.cursor.fetchall()

    def insert_post_stats(self, hour_start, post_stats):
        """
        Вставляет агрегированные данные по постам в таблицу f_hourly_post_stats
        """
        insert_query = """
        INSERT INTO f_hourly_post_stats 
            (hour_start, post_id, likes_count, reposts_count)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (hour_start, post_id) 
        DO UPDATE SET 
            likes_count = EXCLUDED.likes_count,
            reposts_count = EXCLUDED.reposts_count
        """

        for stat in post_stats:
            self.cursor.execute(insert_query,
                                (hour_start, stat['post_id'], stat['likes_count'], stat['reposts_count']))

    def insert_user_stats(self, hour_start, user_stats):
        """
        Вставляет агрегированные данные по пользователям в таблицу f_hourly_user_stats
        """
        # insert_query = """
        # INSERT INTO f_hourly_user_stats
        #     (hour_start, user_id, likes_given, likes_received,
        #      reposts_made, reposts_received, engagement_score)
        # VALUES (%s, %s, %s, %s, %s, %s, %s)
        # ON CONFLICT (hour_start, user_id)
        # DO UPDATE SET
        #     likes_given = EXCLUDED.likes_given,
        #     likes_received = EXCLUDED.likes_received,
        #     reposts_made = EXCLUDED.reposts_made,
        #     reposts_received = EXCLUDED.reposts_received,
        #     engagement_score = EXCLUDED.engagement_score
        # """
        insert_query = """
                INSERT INTO f_hourly_user_stats
                    (hour_start, user_id, likes_given, likes_received,
                     reposts_made, reposts_received, engagement_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """

        for stat in user_stats:
            self.cursor.execute(insert_query,
                                (hour_start, stat['user_id'],
                                 stat['likes_given'], stat['likes_received'],
                                 stat['reposts_made'], stat['reposts_received'],
                                 stat['engagement_score']))

    def get_most_liked_users(self, hour_start, limit=10):
        """
        Получает наиболее залайканных пользователей за час
        (пользователи с наибольшим likes_received)
        """
        query = """
        SELECT 
            user_id,
            likes_received
        FROM f_hourly_user_stats
        WHERE hour_start = %s
        ORDER BY likes_received DESC
        LIMIT %s
        """

        self.cursor.execute(query, (hour_start, limit))
        return self.cursor.fetchall()

    def get_most_engaged_users(self, hour_start, limit=10):
        """
        Получает наиболее вовлечённых пользователей за час
        (пользователи с наибольшим engagement_score)
        """
        query = """
        SELECT 
            user_id,
            engagement_score,
            likes_given,
            reposts_made
        FROM f_hourly_user_stats
        WHERE hour_start = %s
        ORDER BY engagement_score DESC
        LIMIT %s
        """

        self.cursor.execute(query, (hour_start, limit))
        return self.cursor.fetchall()

    def process_all_hours(self):
        """
        Основной метод для обработки всех часов
        """
        logger.info("Начинаем агрегацию данных по часам...")

        hourly_ranges = self.get_hourly_ranges()
        logger.info(f"Найдено {len(hourly_ranges)} часов для обработки")

        for hour_range in hourly_ranges:
            hour_start = hour_range['hour_start']
            hour_end = hour_range['hour_end']

            logger.info(f"Обработка часа: {hour_start}")

            try:
                # Агрегируем данные по постам
                post_stats = self.aggregate_hourly_post_stats(hour_start, hour_end)
                self.insert_post_stats(hour_start, post_stats)

                # Агрегируем данные по пользователям
                user_stats = self.aggregate_hourly_user_stats(hour_start, hour_end)
                self.insert_user_stats(hour_start, user_stats)

                # Получаем топ пользователей для этого часа
                most_liked = self.get_most_liked_users(hour_start, 10)
                most_engaged = self.get_most_engaged_users(hour_start, 10)

                # Выводим информацию о топ пользователях
                if most_liked:
                    logger.info(f"Наиболее залайканные пользователи за {hour_start}:")
                    for user in most_liked:
                        logger.info(f"  {user['user_id']}: {user['likes_received']} лайков")

                if most_engaged:
                    logger.info(f"Наиболее вовлечённые пользователи за {hour_start}:")
                    for user in most_engaged:
                        logger.info(f"  {user['user_id']}: engagement={user['engagement_score']}")

                self.connection.commit()
                logger.info(f"Час {hour_start} успешно обработан")

            except Exception as e:
                self.connection.rollback()
                logger.error(f"Ошибка при обработке часа {hour_start}: {e}")

    def refresh_aggregations(self):
        """
        Полная пересборка всех агрегаций
        """
        logger.info("Начинаем полную пересборку агрегаций...")

        # Очищаем существующие таблицы
        self.cursor.execute("TRUNCATE TABLE f_hourly_post_stats")
        self.cursor.execute("TRUNCATE TABLE f_hourly_user_stats")
        self.connection.commit()

        # Запускаем процесс агрегации
        self.process_all_hours()

        logger.info("Полная пересборка агрегаций завершена")

    def incremental_update(self, last_hour=None):
        """
        Инкрементальное обновление агрегаций для последнего часа
        :param last_hour: если указан, обновляет указанный час, иначе берет последний
        """
        if last_hour:
            hour_start = last_hour
        else:
            # Берем последний час из данных
            query = """
            SELECT MAX(DATE_TRUNC('hour', created_at)) as last_hour
            FROM (
                SELECT created_at FROM raw_events
                UNION ALL
                SELECT created_at FROM dm_posts
            ) all_events
            """
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            hour_start = result['last_hour'] if result else None

        if not hour_start:
            logger.info("Нет данных для обновления")
            return

        hour_end = hour_start + timedelta(hours=1)
        logger.info(f"Инкрементальное обновление для часа: {hour_start}")

        try:
            # Обновляем данные по постам
            post_stats = self.aggregate_hourly_post_stats(hour_start, hour_end)
            self.insert_post_stats(hour_start, post_stats)

            # Обновляем данные по пользователям
            user_stats = self.aggregate_hourly_user_stats(hour_start, hour_end)
            self.insert_user_stats(hour_start, user_stats)

            self.connection.commit()
            logger.info(f"Инкрементальное обновление для часа {hour_start} завершено")

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Ошибка при инкрементальном обновлении: {e}")

    def close(self):
        """Закрывает соединение с БД"""
        self.cursor.close()
        self.connection.close()


def main():
    """
    Пример использования агрегатора
    """
    # Создаем экземпляр агрегатора
    aggregator = PostgresAggregator()

    try:
        # Вариант 1: Полная пересборка всех агрегаций
        aggregator.refresh_aggregations()

        # Вариант 2: Инкрементальное обновление (для последнего часа)
        # aggregator.incremental_update()

        # Получение статистики за конкретный час
        # specific_hour = datetime(2024, 1, 1, 10, 0, 0)  # 1 января 2024, 10:00
        # most_liked = aggregator.get_most_liked_users(specific_hour, 5)
        # most_engaged = aggregator.get_most_engaged_users(specific_hour, 5)

    finally:
        aggregator.close()


if __name__ == "__main__":
    main()