import sqlite3
import threading
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

_local = threading.local()

def get_db_connection():
    """Получение соединения с базой данных с поддержкой многопоточности"""
    if not hasattr(_local, "conn") or _local.conn is None:
        try:
            _local.conn = sqlite3.connect(
                'Users_bot.db',
                timeout=30,
                check_same_thread=False,
                isolation_level=None  # Используем ручное управление транзакциями
            )
            _local.conn.execute("PRAGMA journal_mode=WAL")
            _local.conn.execute("PRAGMA synchronous=NORMAL")
            _local.conn.execute("PRAGMA busy_timeout=30000")  # 30 секунд timeout
            logger.info("Создано новое соединение с базой данных")
        except Exception as e:
            logger.error(f"Ошибка создания соединения с БД: {e}")
            raise
    return _local.conn

@contextmanager
def db_transaction():
    """Контекстный менеджер для управления транзакциями"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка транзакции, выполнен откат: {e}")
        raise
    finally:
        cursor.close()

def close_db_connection():
    """Закрытие соединения с базой данных"""
    if hasattr(_local, "conn") and _local.conn is not None:
        try:
            _local.conn.close()
            logger.info("Соединение с БД закрыто")
        except Exception as e:
            logger.error(f"Ошибка при закрытии соединения с БД: {e}")
        finally:
            _local.conn = None