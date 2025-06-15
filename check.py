import pandas as pd
import os
from datetime import datetime
import sqlite3
import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile
import os
from db_utils import db_transaction

logger = logging.getLogger(__name__)

class MeterValidator:
    """Класс для валидации показаний счетчиков"""
    def __init__(self):
        self.equipment_df = None
        self.conn = sqlite3.connect('Users_bot.db', check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.cursor = self.conn.cursor()
        self.load_equipment()
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
            
            mask = (
                (self.equipment_df['Локация'] == location) & 
                (self.equipment_df['Подразделение'] == division)
            )
            result_df = self.equipment_df[mask].copy()
            
            if result_df.empty:
                logger.warning(f"Не найдено оборудования для {location}, {division}")
                
            return result_df
        except Exception as e:
            logger.error(f"Ошибка получения оборудования: {e}")
            return pd.DataFrame()
        
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
    
    def _get_last_reading(self, inv_num, meter_type):
        """Получение последнего показания для данного счетчика из final_report"""
        try:
            self.cursor.execute('''
                SELECT reading, date
                FROM final_report
                WHERE inv_number = ? AND meter_type = ?
                ORDER BY date DESC
                LIMIT 1
            ''', (inv_num, meter_type))
            
            result = self.cursor.fetchone()
            if result:
                return {
                    'reading': float(result[0]) if result[0] is not None else None,
                    'reading_date': result[1]
                }
            return None  # Возвращаем None, если показаний нет
        except Exception as e:
            logger.error(f"Ошибка получения последнего показания: {e}")
            return None
        
    def _get_admins_for_division(self, division):
        """Исправленная версия с использованием контекстного менеджера"""
        try:
            with db_transaction() as cursor:
                cursor.execute('''
                    SELECT tab_number, name, chat_id FROM Users_admin_bot 
                    WHERE division = ? AND chat_id IS NOT NULL
                ''', (division,))
                admins = cursor.fetchall()
                
                if not admins:
                    cursor.execute('''
                        SELECT tab_number, name, chat_id FROM Users_admin_bot 
                        WHERE chat_id IS NOT NULL AND role = 'Администратор'
                    ''')
                    admins = cursor.fetchall()
                
                return admins
        except Exception as e:
            logger.error(f"Ошибка при поиске администраторов: {str(e)}")
            return []
        
    def _has_pending_ubylo(self, inv_num: str, meter_type: str) -> bool:
        """Проверяет наличие активного запроса 'Убыло' для оборудования"""
        try:
            self.cursor.execute('''
                SELECT 1 FROM pending_requests 
                WHERE inv_num = ? AND meter_type = ? 
                AND status = 'pending'
                AND timestamp > datetime('now', '-5 days')
            ''', (inv_num, meter_type))
            return self.cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Ошибка проверки pending-статуса: {e}")
            return False
        
    def handle_ubylo_status(self, context, inv_num, meter_type, user_info):
        try:
            if self._has_pending_ubylo(inv_num, meter_type):
                return {'status': 'pending', 'message': 'Запрос уже существует'}
            
            request_id = f"ubylo_{datetime.now().timestamp()}"
            user_chat_id = user_info.get('chat_id')
            
            if not user_chat_id:
                self.cursor.execute('SELECT chat_id FROM Users_user_bot WHERE tab_number = ?', (user_info['tab_number'],))
                result = self.cursor.fetchone()
                user_chat_id = result[0] if result else None
            
            if not user_chat_id:
                logger.error(f"Не удалось определить chat_id пользователя {user_info['tab_number']}")
                return {'status': 'error', 'message': 'Не удалось определить chat_id пользователя'}

            # Сохраняем запрос в базу
            with db_transaction() as cursor:
                self.cursor.execute('''
                    INSERT INTO pending_requests (
                        request_id, inv_num, meter_type, user_tab, user_name, 
                        location, division, timestamp, status, user_chat_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    request_id, inv_num, meter_type,
                    user_info['tab_number'], user_info['name'],
                    user_info.get('location', ''), user_info.get('division', ''),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'pending', user_chat_id
                ))
                self.conn.commit()

            admins = self._get_admins_for_division(user_info.get('division', ''))
            if not admins:
                logger.warning("Нет администраторов с chat_id")
                return {'status': 'error', 'message': 'Нет доступных администраторов'}

            sent_to = set()
            for admin_tab, admin_name, admin_chat_id in admins:
                if admin_chat_id in sent_to:
                    continue
                    
                try:
                    keyboard = [
                        [InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_ubylo_{request_id}")],
                        [InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_ubylo_{request_id}")]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    
                    context.bot.send_message(
                        chat_id=admin_chat_id,
                        text=f"⚠️ Запрос на отметку 'Убыло'\n\n"
                            f"Инв. №: {inv_num}\nСчётчик: {meter_type}\n"
                            f"Пользователь: {user_info['name']}\n"
                            f"Локация: {user_info.get('location', '')}\n"
                            f"Подразделение: {user_info.get('division', '')}",
                        reply_markup=reply_markup
                    )
                    sent_to.add(admin_chat_id)
                    logger.info(f"Уведомление отправлено администратору {admin_name} (chat_id: {admin_chat_id})")
                except Exception as e:
                    logger.error(f"Ошибка отправки уведомления администратору {admin_name}: {str(e)}")

            return {'status': 'pending', 'request_id': request_id}
        except Exception as e:
            logger.error(f"Ошибка в handle_ubylo_status: {str(e)}")
            return {'status': 'error', 'message': str(e)}
            
    def validate_file(self, file_path, user_info, context=None):
        """Улучшенная валидация файла с показаниями"""
        try:
            if not all(k in user_info for k in ['tab_number', 'name', 'location', 'division']):
                return {
                    'is_valid': False,
                    'errors': ["Отсутствуют необходимые данные пользователя"],
                    'warnings': []
                }
                
            # Чтение и проверка файла
            readings_df = pd.read_excel(file_path).dropna(how='all')
            
            # Проверка обязательных колонок
            required_columns = {
                '№ п/п': ['№ п/п', '№', 'Номер', 'П/П'],
                'Гос. номер': ['Гос. номер', 'Гос номер', 'Номер', 'ГРЗ'],
                'Инв. №': ['Инв. №', 'Инв №', 'Инвентарный номер'],
                'Счётчик': ['Счётчик', 'Счетчик', 'Тип счетчика'],
                'Показания': ['Показания', 'Значение', 'Текущие показания'],
                'Комментарий': ['Комментарий', 'Примечание', 'Статус']
            }
            
            # Стандартизация названий колонок
            columns_map = {}
            for standard_col, variants in required_columns.items():
                for col in readings_df.columns:
                    if col in variants:
                        columns_map[col] = standard_col
                        break
            
            readings_df = readings_df.rename(columns=columns_map)
            
            # Проверка наличия обязательных колонок
            missing_columns = [col for col in required_columns if col not in readings_df.columns]
            if missing_columns:
                return {
                    'is_valid': False,
                    'errors': [f"Отсутствуют обязательные колонки: {', '.join(missing_columns)}"],
                    'warnings': []
                }
            
            # Получаем список оборудования для локации и подразделения
            equipment_df = self._get_equipment_for_location_division(
                user_info['location'],
                user_info['division']
            )
            
            errors = []
            warnings = []
            pending_ubylo_requests = []
            
            # Обработка комментариев и валидация данных
            for idx, row in readings_df.iterrows():
                comment = str(row['Комментарий']).strip() if pd.notna(row['Комментарий']) else None
                
                # Обработка "В ремонте"
                if comment == "В ремонте":
                    if pd.isna(row['Показания']):
                        last_reading = self._get_last_reading(row['Инв. №'], row['Счётчик'])
                        if last_reading and last_reading['reading'] is not None:
                            readings_df.at[idx, 'Показания'] = last_reading['reading']
                            warnings.append(f"Строка {idx + 1}: Автоматически использовано последнее показание для оборудования в ремонте")
                
                # Обработка "Убыло"
                elif comment == "Убыло":
                    if pd.notna(row['Показания']):
                        readings_df.at[idx, 'Показания'] = None
                        warnings.append(f"Строка {idx + 1}: Показания игнорированы для оборудования с статусом 'Убыло'")
                    
                    # Проверяем статус подтверждения
                    self.cursor.execute('''
                        SELECT status FROM pending_requests 
                        WHERE inv_num = ? AND meter_type = ?
                        AND timestamp > datetime('now', '-5 days')
                        ORDER BY timestamp DESC
                        LIMIT 1
                    ''', (row['Инв. №'], row['Счётчик']))
                    
                    result = self.cursor.fetchone()
                    
                    if result:
                        status = result[0]
                        if status == 'pending':
                            # Для pending запроса НЕ добавляем уведомление пользователю
                            continue
                        elif status == 'rejected':
                            errors.append(f"Строка {idx + 1}: Статус 'Убыло' был отклонен администратором")
                    elif context is not None:
                        # Если запроса нет - создаем новый и уведомляем пользователя
                        request_result = self.handle_ubylo_status(
                            context, 
                            row['Инв. №'], 
                            row['Счётчик'], 
                            user_info
                        )
                        
                        if request_result.get('status') == 'pending':
                            pending_ubylo_requests.append({
                                'row': idx + 1,
                                'inv_num': row['Инв. №'],
                                'meter_type': row['Счётчик'],
                                'request_id': request_result['request_id']
                            })
                            warnings.append(f"Строка {idx + 1}: Создан запрос на подтверждение статуса 'Убыло'")
                        else:
                            errors.append(
                                f"Строка {idx + 1}: Ошибка создания запроса: " +
                                request_result.get('message', 'Неизвестная ошибка')
                            )
            
            # Проверка каждой строки (основные проверки показаний)
            for idx, row in readings_df.iterrows():
                # Пропускаем строки с "Убыло" - они уже обработаны
                comment = str(row['Комментарий']).strip() if pd.notna(row['Комментарий']) else None
                if comment == "Убыло":
                    continue
                    
                equipment_mask = (
                    (equipment_df['Гос. номер'] == row['Гос. номер']) &
                    (equipment_df['Инв. №'] == row['Инв. №']) &
                    (equipment_df['Счётчик'] == row['Счётчик'])
                )
                
                if not equipment_df[equipment_mask].empty:
                    # Проверка показаний
                    if not pd.isna(row['Показания']):
                        try:
                            value = float(row['Показания'])
                            if value < 0:
                                errors.append(f"Строка {idx + 1}: Показания не могут быть отрицательными")
                                continue
                                
                            last_reading = self._get_last_reading(row['Инв. №'], row['Счётчик'])
                            if last_reading and last_reading['reading'] is not None and value < last_reading['reading']:
                                errors.append(f"Строка {idx + 1}: Показание ({value}) меньше предыдущего ({last_reading['reading']})")
                                continue
                                
                        except ValueError:
                            errors.append(f"Строка {idx + 1}: Показания должны быть числом")
                            continue
                
                else:
                    errors.append(f"Строка {idx + 1}: Оборудование не найдено (Гос. номер: {row['Гос. номер']}, Инв. №: {row['Инв. №']}, Счётчик: {row['Счётчик']}")
            
            if errors:
                return {
                    'is_valid': False,
                    'errors': errors,
                    'warnings': warnings,
                    'pending_ubylo_requests': pending_ubylo_requests
                }
            
            return {
                'is_valid': True,
                'warnings': warnings,
                'pending_ubylo_requests': pending_ubylo_requests
            }
                
        except Exception as e:
            logger.error(f"Ошибка при валидации файла: {e}")
            return {
                'is_valid': False,
                'errors': [f"Ошибка при валидации файла: {str(e)}"]
            }
            
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
        
    def finish_admin_readings(self, df, user_info=None):
        """Сохранение показаний администратора в финальный отчет"""
        try:
            # Добавляем метаданные пользователя
            if user_info:
                required_fields = ['name', 'location', 'division']
                if all(field in user_info for field in required_fields):
                    df['name'] = user_info['name']
                    df['location'] = user_info['location']
                    df['division'] = user_info['division']
                    
                    if 'tab_number' in user_info:
                        df['tab_number'] = user_info['tab_number']
                else:
                    logger.warning("Не все обязательные поля пользователя предоставлены")
            
            # Проверяем наличие обязательных колонок
            required_columns = ['Гос. номер', 'Инв. №', 'Счётчик', 'name', 'location', 'division']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                return {
                    'status': 'error',
                    'message': f"Отсутствуют обязательные колонки: {', '.join(missing_columns)}"
                }
                
            # Сохраняем в базу данных
            with db_transaction() as cursor:
                for _, row in df.iterrows():
                    cursor.execute('''
                        INSERT OR REPLACE INTO final_report (
                            gov_number, inv_number, meter_type, reading, comment,
                            name, date, division, location, sender
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        row['Гос. номер'],
                        row['Инв. №'],
                        row['Счётчик'],
                        row['Показания'] if pd.notna(row['Показания']) else None,
                        row['Комментарий'] if pd.notna(row['Комментарий']) else '',
                        row['name'],
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        row['division'],
                        row['location'],
                        'Администратор'
                    ))
            
            return {'status': 'success', 'message': 'Показания успешно сохранены'}
                
        except Exception as e:
            logger.error(f"Ошибка сохранения показаний администратора: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def save_to_final_report(self, file_path_or_df, user_tab_number=None):
        """Сохранение данных из Excel в final_report с получением user_info из БД"""
        try:
            if isinstance(file_path_or_df, str):
                df = pd.read_excel(file_path_or_df)
            elif isinstance(file_path_or_df, pd.DataFrame):
                df = file_path_or_df
            else:
                return {'status': 'error', 'message': 'Invalid input type'}
                
            # Если передан tab_number, получаем данные пользователя из БД
            user_info = {}
            if user_tab_number:
                with db_transaction() as cursor:
                    cursor.execute('''
                        SELECT name, location, division FROM Users_user_bot 
                        WHERE tab_number = ?
                    ''', (user_tab_number,))
                    user_data = cursor.fetchone()
                    
                    if user_data:
                        user_info = {
                            'name': user_data[0],
                            'location': user_data[1],
                            'division': user_data[2],
                            'tab_number': user_tab_number
                        }
            
            # Добавляем метаданные пользователя в DataFrame
            if user_info:
                df['name'] = user_info['name']
                df['location'] = user_info['location']
                df['division'] = user_info['division']
                df['tab_number'] = user_info['tab_number']
                
            # Проверяем обязательные колонки
            required_columns = ['Гос. номер', 'Инв. №', 'Счётчик', 'name', 'location', 'division']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                return {
                    'status': 'error',
                    'message': f"Отсутствуют обязательные колонки: {', '.join(missing_columns)}"
                }
                
            # Проверка на дубликаты (инв. номер + счётчик) в текущем DataFrame
            duplicate_mask = df.duplicated(subset=['Инв. №', 'Счётчик'], keep=False)
            if duplicate_mask.any():
                duplicates = df[duplicate_mask][['Инв. №', 'Счётчик']].drop_duplicates()
                return {
                    'status': 'error',
                    'message': f"Обнаружены дубликаты в загружаемых данных:\n{duplicates.to_string(index=False)}"
                }
                
            # Проверка на дубликаты в базе данных
            with db_transaction() as cursor:
                for _, row in df.iterrows():
                    cursor.execute('''
                        SELECT 1 FROM final_report 
                        WHERE inv_number = ? AND meter_type = ? 
                        AND date >= datetime('now', '-5 days')
                    ''', (row['Инв. №'], row['Счётчик']))
                    
                    if cursor.fetchone():
                        return {
                            'status': 'error',
                            'message': f"Для инв. № {row['Инв. №']} и счетчика {row['Счётчик']} уже есть запись в базе за последние 5 дней"
                        }
            
            # Сохраняем в базу данных
            with db_transaction() as cursor:
                for _, row in df.iterrows():
                    cursor.execute('''
                        INSERT OR REPLACE INTO final_report (
                            gov_number, inv_number, meter_type, reading, comment,
                            name, date, division, location, sender
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        row['Гос. номер'],
                        row['Инв. №'],
                        row['Счётчик'],
                        row['Показания'] if pd.notna(row['Показания']) else None,
                        row['Комментарий'] if pd.notna(row['Комментарий']) else '',
                        row['name'],
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        row['division'],
                        row['location'],
                        'Администратор' if not user_tab_number else 'Пользователь'
                    ))
            
            return {'status': 'success', 'message': 'Показания успешно сохранены'}
                
        except Exception as e:
            logger.error(f"Ошибка сохранения показаний: {e}")
            return {'status': 'error', 'message': str(e)}
        
class FinalReportGenerator:
    """Класс для генерации сводных отчетов по показаниям счетчиков"""
    
    def __init__(self, bot=None):
        self.bot = bot
        self.conn = sqlite3.connect('Users_bot.db', check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.current_week = datetime.now().strftime('%Y-W%U')

    def generate_final_report(self, week_folder):
        """Генерация финального отчета за неделю"""
        try:
            report_data = []
            week_number = os.path.basename(week_folder).replace('week_', '')
            
            for filename in os.listdir(week_folder):
                if not filename.endswith('.xlsx'):
                    continue
                    
                try:
                    file_path = os.path.join(week_folder, filename)
                    df = pd.read_excel(file_path)
                    
                    # Проверяем наличие необходимых колонок
                    required_columns = ['Гос. номер', 'Инв. №', 'Счётчик', 'Показания', 'Комментарий']
                    if not all(col in df.columns for col in required_columns):
                        logger.error(f"Файл {filename} не содержит всех необходимых колонок")
                        continue
                    
                    # Извлекаем метаданные
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
                            'Показания': row['Показания'] if pd.notna(row['Показания']) else None,
                            'Комментарий': row['Комментарий'] if pd.notna(row['Комментарий']) else '',
                            'Наименование': row['Гос. номер'],
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
            
            # Сохраняем в таблицу final_report
            if not self.save_to_final_report(report_df):
                return None
        
            # Также сохраняем в Excel (по желанию)
            output_path = os.path.join(week_folder, f'final_report_{week_number}.xlsx')
            report_df.to_excel(output_path, index=False)
            
            return output_path
            
        except Exception as e:
            logger.error(f"Ошибка генерации финального отчета: {e}")
            return None