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
        self.conn = sqlite3.connect('Users_bot.db', check_same_thread=False)
        self.cursor = self.conn.cursor()
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
        
    def handle_ubylo_status(self, inv_num: str, meter_type: str, user_info: dict) -> dict:
        """Обработка статуса 'Убыло' с подтверждением администратора"""
        try:
            # Получаем информацию об оборудовании
            self.cursor.execute('''
                SELECT location, division FROM equipment 
                WHERE inventory_number = ? AND meter_type = ?
            ''', (inv_num, meter_type))
            equipment_info = self.cursor.fetchone()
            
            if not equipment_info:
                return {'status': 'error', 'message': 'Оборудование не найдено'}
            
            equipment_location, equipment_division = equipment_info
            
            # Получаем администраторов для этого подразделения
            admins = self.get_admin_for_division(equipment_division)
            
            if not admins:
                return {'status': 'error', 'message': 'Не найдены администраторы для подтверждения'}
            
            # Создаем запрос на подтверждение
            request_id = f"ubylo_{inv_num}_{meter_type}_{datetime.now().timestamp()}"
            
            # Сохраняем запрос в базу данных
            self.cursor.execute('''
                INSERT INTO pending_requests 
                (request_id, inv_num, meter_type, user_tab, user_name, location, division, status, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (request_id, inv_num, meter_type, user_info['tab_number'], user_info['name'], 
                equipment_location, equipment_division, 'pending', datetime.now()))
            self.conn.commit()
            
            # Отправляем уведомления администраторам
            admin_keyboard = []
            for admin_tab, admin_name in admins:
                admin_keyboard.append([
                    InlineKeyboardButton(
                        f"Подтвердить для {inv_num} ({meter_type})",
                        callback_data=f"confirm_ubylo_{request_id}"
                    ),
                    InlineKeyboardButton(
                        "Отклонить",
                        callback_data=f"reject_ubylo_{request_id}"
                    )
                ])
            
            reply_markup = InlineKeyboardMarkup(admin_keyboard)
            
            message = (
                f"🚨 *Требуется подтверждение администратора*\n\n"
                f"Пользователь {user_info['name']} отметил оборудование как 'Убыло':\n"
                f"Инв. №: {inv_num}\n"
                f"Счётчик: {meter_type}\n"
                f"Локация: {equipment_location}\n"
                f"Подразделение: {equipment_division}\n\n"
                f"Пожалуйста, подтвердите или отклоните этот запрос:"
            )
            
            # Возвращаем информацию для отправки уведомлений администраторам
            return {
                'status': 'pending',
                'request_id': request_id,
                'admins': admins,
                'message': message,
                'reply_markup': reply_markup
            }
            
        except Exception as e:
            logger.error(f"Ошибка обработки статуса 'Убыло': {e}")
            return {'status': 'error', 'message': str(e)}
    
    def save_to_history(self, report_df, week_number):
        """Сохраняет данные отчета в таблицу meter_readings_history"""
        try:
            # Проверяем и преобразуем данные
            required_columns = ['Гос. номер', 'Инв. №', 'Счётчик', 'Показания', 
                            'Комментарий', 'Дата', 'Подразделение', 'Локация', 'Отправитель']
            
            if not all(col in report_df.columns for col in required_columns):
                logger.error("Отсутствуют необходимые колонки в отчете")
                return False
                
            # Подготовка данных для вставки
            data_to_insert = []
            for _, row in report_df.iterrows():
                data_to_insert.append((
                    row['Инв. №'],
                    row['Счётчик'],
                    float(row['Показания']) if pd.notna(row['Показания']) else None,
                    row['Комментарий'] if pd.notna(row['Комментарий']) else '',
                    row['Отправитель'],
                    row['Локация'],
                    row['Подразделение'],
                    datetime.strptime(row['Даата'], '%Y-%m-%d %H:%M:%S') if isinstance(row['Дата'], str) else row['Дата'],
                    week_number,
                    datetime.now()
                ))
            
            # Вставка данных в базу
            self.cursor.executemany('''
                INSERT INTO meter_readings_history (
                    inventory_number, 
                    meter_type, 
                    reading, 
                    comment, 
                    user_name, 
                    location, 
                    division, 
                    reading_date, 
                    report_week, 
                    timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_to_insert)
            
            self.conn.commit()
            logger.info(f"Успешно сохранено {len(data_to_insert)} записей в историю показаний")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения в историю показаний: {e}")
            self.conn.rollback()
            return False
    
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
                            
                            # Проверка 7: Формат файла только Excel
                            if not file_path.lower().endswith(('.xlsx', '.xls')):
                                return {'is_valid': False, 'errors': ["Файл должен быть в формате Excel (.xlsx, .xls)"]}
                        
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
    
    
    def generate_final_report(self, week_folder):
        """Генерация и сохранение сводного отчета за неделю"""
        report_data = []
        week_number = os.path.basename(week_folder).replace('week_', '')
        
        for filename in os.listdir(week_folder):
            try:
                df = pd.read_excel(f"{week_folder}/{filename}")
                # Извлекаем метаданные из файла
                user_info = {
                    'name': df['name'].iloc[0] if 'name' in df.columns else 'Неизвестно',
                    'location': df['location'].iloc[0] if 'location' in df.columns else 'Неизвестно',
                    'division': df['division'].iloc[0] if 'division' in df.columns else 'Неизвестно',
                    'timestamp': df['timestamp'].iloc[0] if 'timestamp' in df.columns else datetime.now()
                }
                
                # Добавляем данные в отчет
                for _, row in df.iterrows():
                    report_data.append({
                        'Гос. номер': row['Гос. номер'],
                        'Инв. №': row['Инв. №'],
                        'Счётчик': row['Счётчик'],
                        'Показания': row['Показания'],
                        'Комментарий': row['Комментарий'] if 'Комментарий' in row else '',
                        'Дата': user_info['timestamp'],
                        'Подразделение': user_info['division'],
                        'Локация': user_info['location'],
                        'Отправитель': user_info['name']
                    })
            except Exception as e:
                logger.error(f"Ошибка обработки файла {filename}: {e}")
                continue
        
        if not report_data:
            return None
        
        report_df = pd.DataFrame(report_data)
        
        # Сохраняем в базу данных
        self.save_to_history(report_df, week_number)
        
        # Генерируем Excel файл
        output = io.BytesIO()
        report_df.to_excel(output, index=False)
        output.seek(0)
        
        return output