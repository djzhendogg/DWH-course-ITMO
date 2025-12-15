import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Aggregator:
    def __init__(self, db_params):
        """
        Инициализация агрегатора

        :param db_params: параметры подключения к БД
        """
        self.db_params = db_params
        self.conn = None

    def connect(self):
        """Установка соединения с БД"""
        try:
            self.conn = psycopg2.connect(**self.db_params)
            logger.info("Успешное подключение к БД")
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            raise

    def disconnect(self):
        """Закрытие соединения с БД"""
        if self.conn:
            self.conn.close()
            logger.info("Соединение с БД закрыто")

    def get_hour_start(self, timestamp):
        """
        Округление timestamp до начала часа

        :param timestamp: временная метка
        :return: timestamp начала часа
        """
        return timestamp.replace(minute=0, second=0, microsecond=0)

    def aggregate_hourly_post_stats(self, hour_start=None):
        """
        Агрегация статистики по постам за час

        :param hour_start: начало часа для агрегации (если None - текущий час)
        """
        if hour_start is None:
            hour_start = self.get_hour_start(datetime.now())
        else:
            hour_start = self.get_hour_start(hour_start)

        hour_end = hour_start + timedelta(hours=1)

        query = """
        INSERT INTO f_hourly_post_stats (hour_start, post_id, likes_count, reposts_count)
        SELECT 
            %s as hour_start,
            re.post_id,
            COUNT(CASE WHEN re.event_type = 'like' THEN 1 END) as likes_count,
            COUNT(CASE WHEN re.event_type = 'repost' THEN 1 END) as reposts_count
        FROM raw_events re
        WHERE re.created_at >= %s 
            AND re.created_at < %s
        GROUP BY re.post_id
        ON CONFLICT (hour_start, post_id) 
        DO UPDATE SET
            likes_count = EXCLUDED.likes_count,
            reposts_count = EXCLUDED.reposts_count
        """

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (hour_start, hour_start, hour_end))
                self.conn.commit()
                logger.info(f"Агрегирована статистика по постам за {hour_start}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Ошибка агрегации статистики по постам: {e}")
            raise

    def aggregate_hourly_user_stats(self, hour_start=None):
        """
        Агрегация статистики по пользователям за час

        :param hour_start: начало часа для агрегации (если None - текущий час)
        """
        if hour_start is None:
            hour_start = self.get_hour_start(datetime.now())
        else:
            hour_start = self.get_hour_start(hour_start)

        hour_end = hour_start + timedelta(hours=1)

        # CTE для подсчета лайков и репостов по пользователям
        query = """
        WITH user_actions AS (
            -- Действия пользователя (лайки и репосты, которые он сделал)
            SELECT 
                re.user_id,
                COUNT(CASE WHEN re.event_type = 'like' THEN 1 END) as likes_given,
                COUNT(CASE WHEN re.event_type = 'repost' THEN 1 END) as reposts_made,
                SUM(CASE 
                    WHEN re.event_type = 'like' THEN 3
                    WHEN re.event_type = 'repost' THEN 37
                    ELSE 0 
                END) as engagement_score
            FROM raw_events re
            WHERE re.created_at >= %s 
                AND re.created_at < %s
            GROUP BY re.user_id
        ),
        post_reactions AS (
            -- Реакции на посты пользователя (лайки и репосты, которые получили его посты)
            SELECT 
                dp.author_id as user_id,
                COUNT(CASE WHEN re.event_type = 'like' THEN 1 END) as likes_received,
                COUNT(CASE WHEN re.event_type = 'repost' THEN 1 END) as reposts_received
            FROM raw_events re
            JOIN dm_posts dp ON re.post_id = dp.post_id
            WHERE re.created_at >= %s 
                AND re.created_at < %s
            GROUP BY dp.author_id
        ),
        user_stats AS (
            -- Объединение статистики
            SELECT 
                COALESCE(ua.user_id, pr.user_id) as user_id,
                COALESCE(ua.likes_given, 0) as likes_given,
                COALESCE(pr.likes_received, 0) as likes_received,
                COALESCE(ua.reposts_made, 0) as reposts_made,
                COALESCE(pr.reposts_received, 0) as reposts_received,
                COALESCE(ua.engagement_score, 0) as engagement_score
            FROM user_actions ua
            FULL OUTER JOIN post_reactions pr ON ua.user_id = pr.user_id
        )
        INSERT INTO f_hourly_user_stats (hour_start, user_id, likes_given, likes_received, 
                                        reposts_made, reposts_received, engagement_score)
        SELECT 
            %s as hour_start,
            us.user_id,
            us.likes_given,
            us.likes_received,
            us.reposts_made,
            us.reposts_received,
            us.engagement_score
        FROM user_stats us
        ON CONFLICT (hour_start, user_id) 
        DO UPDATE SET
            likes_given = EXCLUDED.likes_given,
            likes_received = EXCLUDED.likes_received,
            reposts_made = EXCLUDED.reposts_made,
            reposts_received = EXCLUDED.reposts_received,
            engagement_score = EXCLUDED.engagement_score
        """

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (hour_start, hour_end, hour_start, hour_end, hour_start))
                self.conn.commit()
                logger.info(f"Агрегирована статистика по пользователям за {hour_start}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Ошибка агрегации статистики по пользователям: {e}")
            raise

    def get_most_liked_users(self, hour_start=None, limit=10):
        """
        Получение наиболее залайканных пользователей за час

        :param hour_start: начало часа (если None - текущий час)
        :param limit: количество пользователей в результате
        :return: список пользователей с количеством лайков
        """
        if hour_start is None:
            hour_start = self.get_hour_start(datetime.now())
        else:
            hour_start = self.get_hour_start(hour_start)

        query = """
        SELECT 
            user_id,
            likes_received
        FROM f_hourly_user_stats
        WHERE hour_start = %s
        ORDER BY likes_received DESC
        LIMIT %s
        """

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (hour_start, limit))
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Ошибка получения наиболее залайканных пользователей: {e}")
            return []

    def get_top_engaged_users(self, hour_start=None, limit=10):
        """
        Получение пользователей с наибольшей вовлеченностью за час

        :param hour_start: начало часа (если None - текущий час)
        :param limit: количество пользователей в результате
        :return: список пользователей с engagement_score
        """
        if hour_start is None:
            hour_start = self.get_hour_start(datetime.now())
        else:
            hour_start = self.get_hour_start(hour_start)

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

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (hour_start, limit))
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Ошибка получения наиболее вовлеченных пользователей: {e}")
            return []

    def aggregate_all_for_hour(self, hour_start=None):
        """
        Выполнение всех агрегаций за указанный час

        :param hour_start: начало часа для агрегации
        """
        logger.info(f"Начало агрегации за {hour_start}")

        # Агрегация по постам
        self.aggregate_hourly_post_stats(hour_start)

        # Агрегация по пользователям
        self.aggregate_hourly_user_stats(hour_start)

        logger.info(f"Завершена агрегация за {hour_start}")

    def get_aggregation_status(self, hours_back=24):
        """
        Получение статуса агрегации за последние N часов

        :param hours_back: количество часов для проверки
        :return: статус агрегации
        """
        end_hour = self.get_hour_start(datetime.now())
        start_hour = end_hour - timedelta(hours=hours_back)

        query = """
        SELECT 
            hour_start,
            COUNT(DISTINCT post_id) as posts_count,
            COUNT(DISTINCT user_id) as users_count
        FROM (
            SELECT hour_start, post_id, NULL as user_id
            FROM f_hourly_post_stats
            WHERE hour_start >= %s AND hour_start < %s
            UNION ALL
            SELECT hour_start, NULL as post_id, user_id
            FROM f_hourly_user_stats
            WHERE hour_start >= %s AND hour_start < %s
        ) combined
        GROUP BY hour_start
        ORDER BY hour_start DESC
        """

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (start_hour, end_hour, start_hour, end_hour))
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Ошибка получения статуса агрегации: {e}")
            return []


# Пример использования
def main():
    # Параметры подключения к БД
    db_params = {
        'host': 'localhost',
        'port': 5432,
        'database': 'your_database',
        'user': 'your_user',
        'password': 'your_password'
    }

    # Создание агрегатора
    aggregator = Aggregator(db_params)

    try:
        # Подключение к БД
        aggregator.connect()

        # Пример 1: Агрегация за текущий час
        aggregator.aggregate_all_for_hour()

        # Пример 2: Получение наиболее залайканных пользователей
        most_liked = aggregator.get_most_liked_users(limit=5)
        print("Наиболее залайканные пользователи:")
        for user in most_liked:
            print(f"  Пользователь {user['user_id']}: {user['likes_received']} лайков")

        # Пример 3: Получение наиболее вовлеченных пользователей
        top_engaged = aggregator.get_top_engaged_users(limit=5)
        print("\nНаиболее вовлеченные пользователи:")
        for user in top_engaged:
            print(f"  Пользователь {user['user_id']}: engagement_score={user['engagement_score']}")

        # Пример 4: Получение статуса агрегации
        status = aggregator.get_aggregation_status(hours_back=3)
        print("\nСтатус агрегации за последние 3 часа:")
        for stat in status:
            print(f"  {stat['hour_start']}: {stat['posts_count']} постов, {stat['users_count']} пользователей")

        # Пример 5: Агрегация за конкретный час (например, 2 часа назад)
        two_hours_ago = datetime.now() - timedelta(hours=2)
        aggregator.aggregate_all_for_hour(two_hours_ago)

    except Exception as e:
        logger.error(f"Ошибка в основном процессе: {e}")
    finally:
        # Закрытие соединения
        aggregator.disconnect()


if __name__ == "__main__":
    main()