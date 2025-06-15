import pandas as pd
from datetime import datetime, timedelta
import sqlite3
import logging
import os

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class ShiftsHandler:
    def __init__(self):
        self.conn = sqlite3.connect('Users_bot.db', check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.setup_database()

    def setup_database(self):
        """Создание необходимых таблиц в базе данных"""
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_shifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                employee_name TEXT,
                status TEXT,
                UNIQUE(date, employee_name)
            )
        ''')
        self.conn.commit()

    def check_admin_status(self, admin_name):
        try:
            if not admin_name:
                logger.error("Передано пустое имя администратора")
                return None
            
            current_date = datetime.now().strftime('%d.%m.%Y')
            
            # Проверяем в daily_shifts
            self.cursor.execute('''
                SELECT status 
                FROM daily_shifts 
                WHERE date = ? AND employee_name = ?
            ''', (current_date, admin_name))
            
            result = self.cursor.fetchone()
            if result:
                return result[0]
            
            # Если нет в daily_shifts, считаем что на вахте (по умолчанию для администраторов)
            return "ДА"
                
        except Exception as e:
            logger.error(f"Ошибка при проверке статуса администратора {admin_name}: {e}")
            return None

    def load_tabel(self):
        """Загрузка и обработка данных из табеля"""
        try:
            df = pd.read_excel('tabels.xlsx')
            
            current_date = datetime.now().strftime('%d.%m.%Y')
            
            date_columns = []
            for col in df.columns:
                if col != 'ФИО':
                    try:
                        date_obj = pd.to_datetime(col, dayfirst=True, errors='coerce')
                        if pd.notna(date_obj):
                            date_str = date_obj.strftime('%d.%m.%Y')
                            df = df.rename(columns={col: date_str})
                            date_columns.append(date_str)
                    except Exception as e:
                        logger.warning(f"Ошибка при обработке даты {col}: {e}")
                        continue
            
            # Если текущей даты нет
            if current_date not in df.columns:
                logger.info(f"Добавление новой даты {current_date} в табель")
                if date_columns:
                    last_date = sorted(date_columns, 
                                    key=lambda x: datetime.strptime(x, '%d.%m.%Y'))[-1]
                    df[current_date] = df[last_date]
                else:
                    # Если нет предыдущих дат, заполняем "НЕТ"
                    df[current_date] = "НЕТ"
                
                df.to_excel('tabels.xlsx', index=False)
                logger.info(f"Табель обновлен с новой датой {current_date}")
            
            current_statuses = df[['ФИО', current_date]].copy()
            current_statuses.columns = ['employee_name', 'status']
            
            self.cursor.execute('DELETE FROM daily_shifts WHERE date = ?', (current_date,))
            
            for _, row in current_statuses.iterrows():
                if pd.notna(row['status']):
                    status = str(row['status']).strip().upper()
                    self.cursor.execute('''
                        INSERT INTO daily_shifts (date, employee_name, status)
                        VALUES (?, ?, ?)
                    ''', (current_date, row['employee_name'], status))
            
            self.conn.commit()
            
        except FileNotFoundError:
            logger.error("Файл tabels.xlsx не найден. Создаем новый файл.")
            try:
                self.cursor.execute('SELECT name FROM Users_user_bot')
                employees = [row[0] for row in self.cursor.fetchall()]
                
                df = pd.DataFrame({'ФИО': employees})
                current_date = datetime.now().strftime('%d.%m.%Y')
                df[current_date] = "НЕТ"  # По умолчанию все "НЕТ"
                
                df.to_excel('tabels.xlsx', index=False)
                logger.info("Создан новый файл tabels.xlsx")
                
                # Рекурсия...
                self.load_tabel()
                
            except Exception as e:
                logger.error(f"Ошибка при создании нового файла табеля: {e}")
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке табеля: {e}")
            if 'strptime()' in str(e):
                logger.error("Ошибка в формате даты. Текущие колонки в табеле:")
                try:
                    df = pd.read_excel('tabels.xlsx')
                    logger.error(f"Колонки: {df.columns.tolist()}")
                except:
                    pass

    def get_absent_users(self) -> list:
        # Возвращает список отсутствующих в формате [(name, status), ...]
        try:
            current_date = datetime.now().strftime('%d.%m.%Y')
            self.cursor.execute('''
                SELECT employee_name, status FROM daily_shifts
                WHERE date = ? AND status IN ("НЕТ", "О", "Б")
            ''', (current_date,))
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Ошибка получения отсутствующих: {e}")
            return []

    def check_employee_status(self, employee_name: str) -> str:
        return "ДА"

    def is_user_available(self, employee_name):
        return True

    def get_active_users(self):
        """Получение списка активных пользователей на текущий день"""
        try:
            current_date = datetime.now().strftime('%d.%m.%Y')
            
            self.cursor.execute('''
                SELECT u.tab_number, u.name, u.location, u.division, u.t_number
                FROM Users_user_bot u
                JOIN daily_shifts ds ON u.name = ds.employee_name
                WHERE ds.date = ? AND ds.status = "ДА"
            ''', (current_date,))
            
            return self.cursor.fetchall()
            
        except Exception as e:
            logger.error(f"Ошибка при получении активных пользователей: {e}")
            return []

    def get_users_on_shift(self):
        """Получение списка пользователей на смене"""
        try:
            self.cursor.execute('''
                SELECT tab_number, name
                FROM shifts 
                WHERE is_on_shift = 'ДА'
            ''')
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Ошибка получения списка пользователей на смене: {e}")
            return []

    def get_users_info(self):
        """Получение информации о пользователях"""
        try:
            self.cursor.execute('''
                SELECT u.tab_number, u.name, u.location, u.division
                FROM Users_user_bot u
            ''')
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Ошибка получения информации о пользователях: {e}")
            return []

    def __del__(self):
        """Закрытие соединения с базой данных"""
        try:
            self.conn.close()
        except:
            pass 