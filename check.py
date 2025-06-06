import io
import pandas as pd
import os
from datetime import datetime
import sqlite3
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext, CallbackQueryHandler

logger = logging.getLogger(__name__)

class MeterValidator:
    """Класс для валидации показаний счетчиков"""
    
    def __init__(self):
        self.equipment_df = None
        self.load_equipment()
        self.conn = sqlite3.connect('Users_bot.db', check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.equipment_df = None
        self.load_equipment()

    def load_equipment(self):
        """Загрузка справочника оборудования"""
        try:
            self.equipment_df = pd.read_excel('Equipment.xlsx')
            # Приводим названия колонок к стандартному виду
            self.equipment_df.columns = [col.strip() for col in self.equipment_df.columns]
            logger.info("Справочник оборудования успешно загружен")
        except Exception as e:
            logger.error(f"Ошибка при загрузке справочника оборудования: {e}")
            # Создаем пустой DataFrame с нужными колонками
            self.equipment_df = pd.DataFrame(columns=[
                'Локация', 'Подразделение', 'Гос. номер', 'Инв. №', 
                'Счётчик', 'Тип счетчика'
            ])

    def _get_equipment_for_location_division(self, location, division):
        """Получение списка оборудования для локации и подразделения"""
        try:
            if self.equipment_df is None or self.equipment_df.empty:
                self.load_equipment()
            
            if self.equipment_df.empty:
                logger.warning("Справочник оборудования пуст")
                return pd.DataFrame()
            
            # Фильтруем оборудование по локации и подразделению
            mask = (
                (self.equipment_df['Локация'] == location) & 
                (self.equipment_df['Подразделение'] == division)
            )
            result_df = self.equipment_df[mask].copy()
            logger.info(f"Найдено {len(result_df)} единиц оборудования для локации {location} и подразделения {division}")
            return result_df
        except Exception as e:
            logger.error(f"Ошибка получения оборудования для локации/подразделения: {e}")
            return pd.DataFrame()
    
    def _get_last_reading(self, inv_num, meter_type):
        """Получение последнего показания для данного счетчика"""
        try:
            self.cursor.execute('''
                SELECT reading, reading_date
                FROM meter_readings_history
                WHERE inventory_number = ? AND meter_type = ?
                ORDER BY reading_date DESC
                LIMIT 1
            ''', (inv_num, meter_type))
            
            result = self.cursor.fetchone()
            if result:
                return {
                    'reading': result[0],
                    'reading_date': result[1]
                }
            return None
        except Exception as e:
            logger.error(f"Ошибка получения последнего показания: {e}")
            return None
    
    def _get_days_between(self, last_date_str):
        """Вычисление количества дней между датами"""
        try:
            last_date = datetime.strptime(last_date_str, '%Y-%m-%d %H:%M:%S')
            now = datetime.now()
            delta = now - last_date
            return max(delta.days, 1)  # Минимум 1 день, чтобы избежать деления на ноль
        except Exception as e:
            logger.error(f"Ошибка расчета дней между датами: {e}")
            return 1  # По умолчанию возвращаем 1 день

    def get_admin_for_division(self, division):
        """Получение ID администратора для данного подразделения"""
        try:
            # Проверяем наличие подразделения
            if not division:
                return []
                
            self.cursor.execute('''
                SELECT tab_number, name
                FROM Users_admin_bot
                WHERE division = ?
            ''', (division,))
            
            admins = self.cursor.fetchall()
            
            # Если нет администраторов для подразделения, вернем всех администраторов
            if not admins:
                self.cursor.execute('''
                    SELECT tab_number, name
                    FROM Users_admin_bot
                ''')
                admins = self.cursor.fetchall()
                
            return admins
        except Exception as e:
            logger.error(f"Ошибка получения администратора для подразделения: {e}")
            return []
        
def validate_file(self, file_path, user_info):
    """Улучшенная валидация файла с показаниями"""
    try:
        # Загружаем файл с показаниями
        readings_df = pd.read_excel(file_path)
        logger.info(f"Загружен файл показаний: {file_path}")
        
        # Проверяем наличие всех необходимых колонок
        required_columns = ['№ п/п', 'Гос. номер', 'Инв. №', 'Счётчик', 'Показания', 'Комментарий']
        missing_columns = [col for col in required_columns if col not in readings_df.columns]
        if missing_columns:
            return {
                'is_valid': False,
                'errors': [f"Отсутствуют обязательные колонки: {', '.join(missing_columns)}"]
            }
        
        # Получаем список оборудования для локации и подразделения
        equipment_df = self._get_equipment_for_location_division(
            user_info['location'],
            user_info['division']
        )
        
        errors = []
        warnings = []
        
        # Проверяем каждую строку с показаниями
        for idx, row in readings_df.iterrows():
            # Проверяем существование оборудования
            equipment_mask = (
                (equipment_df['Гос. номер'] == row['Гос. номер']) &
                (equipment_df['Инв. №'] == row['Инв. №']) &
                (equipment_df['Счётчик'] == row['Счётчик'])
            )
            
            if not equipment_df[equipment_mask].empty:
                equipment = equipment_df[equipment_mask].iloc[0]
                
                # Проверка 1: Если показания пустые, должен быть комментарий
                if pd.isna(row['Показания']) and pd.isna(row['Комментарий']):
                    errors.append(f"Строка {idx + 1}: Необходимо указать либо показания, либо комментарий")
                    continue
                
                # Проверка 2: Если комментарий "В ремонте", используем последнее показание
                if str(row['Комментарий']).strip() == "В ремонте" and pd.isna(row['Показания']):
                    last_reading = self._get_last_reading(row['Инв. №'], row['Счётчик'])
                    if last_reading:
                        readings_df.at[idx, 'Показания'] = last_reading['reading']
                        warnings.append(f"Строка {idx + 1}: Автоматически использовано последнее показание для оборудования в ремонте")
                
                # Проверка 3: Если есть показания, проверяем их корректность
                if not pd.isna(row['Показания']):
                    try:
                        value = float(row['Показания'])
                        if value < 0:
                            errors.append(f"Строка {idx + 1}: Показания не могут быть отрицательными")
                            continue
                            
                        # Проверка 4: Значение должно быть >= последнего
                        last_reading = self._get_last_reading(row['Инв. №'], row['Счётчик'])
                        if last_reading and value < last_reading['reading']:
                            errors.append(f"Строка {idx + 1}: Показание ({value}) меньше предыдущего ({last_reading['reading']})")
                            continue
                            
                        # Проверка 5: Для счетчиков PM - не более 24 в сутки
                        if row['Счётчик'].startswith('PM') and last_reading:
                            days_between = self._get_days_between(last_reading['reading_date'])
                            if days_between > 0:
                                daily_change = (value - last_reading['reading']) / days_between
                                if daily_change > 24:
                                    errors.append(f"Строка {idx + 1}: Для счетчика PM превышено максимальное изменение (24 в сутки). Текущее: {daily_change:.2f}")
                                    continue
                                    
                        # Проверка 6: Для счетчиков KM - не более 500 в сутки
                        if row['Счётчик'].startswith('KM') and last_reading:
                            days_between = self._get_days_between(last_reading['reading_date'])
                            if days_between > 0:
                                daily_change = (value - last_reading['reading']) / days_between
                                if daily_change > 500:
                                    errors.append(f"Строка {idx + 1}: Для счетчика KM превышено максимальное изменение (500 в сутки). Текущее: {daily_change:.2f}")
                                    continue
                    
                    except ValueError:
                        errors.append(f"Строка {idx + 1}: Показания должны быть числом")
                        continue
                
                # Проверка 7: Допустимые значения комментариев
                if not pd.isna(row['Комментарий']):
                    valid_comments = ["В ремонте", "Неисправен", "Убыло", "Нет на локации"]
                    if str(row['Комментарий']).strip() not in valid_comments:
                        errors.append(f"Строка {idx + 1}: Недопустимый комментарий. Допустимые значения: {', '.join(valid_comments)}")
            else:
                errors.append(f"Строка {idx + 1}: Оборудование не найдено (Гос. номер: {row['Гос. номер']}, Инв. №: {row['Инв. №']}, Счётчик: {row['Счётчик']})")
        
        if errors:
            return {
                'is_valid': False,
                'errors': errors,
                'warnings': warnings
            }
        
        # Если все проверки пройдены, сохраняем показания
        self._save_readings_to_history(readings_df, user_info)
        
        return {
            'is_valid': True,
            'warnings': warnings
        }
        
    except Exception as e:
        logger.error(f"Ошибка при валидации файла: {e}")
        return {
            'is_valid': False,
            'errors': [f"Ошибка при валидации файла: {str(e)}"]
        }
    
def generate_final_report(week_folder):
    report_data = []
    
    for filename in os.listdir(week_folder):
        try:
            df = pd.read_excel(f"{week_folder}/{filename}")
            # Извлекаем метаданные из файла
            user_info = {
                'name': df['name'].iloc[0] if 'name' in df.columns else 'Неизвестно',
                'location': df['location'].iloc[0] if 'location' in df.columns else 'Неизвестно',
                'division': df['division'].iloc[0] if 'division' in df.columns else 'Неизвестно',
                'timestamp': df['timestamp'].iloc[0] if 'timestamp' in df.columns else 'Неизвестно'
            }
            
            # Добавляем данные в отчет
            for _, row in df.iterrows():
                report_data.append({
                    'Гос. номер': row['Гос. номер'],
                    'Инв. №': row['Инв. №'],
                    'Счётчик': row['Счётчик'],
                    'Комментарий': row['Комментарий'] if 'Комментарий' in row else '',
                    'Наименование': row.get('Наименование', ''),
                    'Дата': user_info['timestamp'],
                    'Подразделение': user_info['division'],
                    'Локация': user_info['location'],
                    'Отправитель': user_info['name']
                })
        except Exception as e:
            logger.error(f"Ошибка обработки файла {filename}: {e}")
    
    if not report_data:
        return None
    
    report_df = pd.DataFrame(report_data)
    output = io.BytesIO()
    report_df.to_excel(output, index=False)
    output.seek(0)
    
    return output

def _get_last_editor(self, inv_num, meter_type):
    """Получает информацию о последнем редакторе"""
    self.cursor.execute('''
        SELECT user_name 
        FROM meter_readings_history
        WHERE inventory_number = ? AND meter_type = ?
        ORDER BY timestamp DESC
        LIMIT 1
    ''', (inv_num, meter_type))
    result = self.cursor.fetchone()
    return result[0] if result else None