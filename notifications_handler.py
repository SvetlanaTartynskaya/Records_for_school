import pandas as pd
from datetime import datetime, time
import pytz
import os
import logging
from telegram import InputFile
import io

from shifts_handler import ShiftsHandler

def send_absence_notifications(context):
    handler = ShiftsHandler()
    absent_users = handler.get_absent_users()
    for user in absent_users:
        context.bot.send_message(...)

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def get_last_readings():
    """Получение последних показаний счетчиков"""
    try:
        # В будущем здесь будет интеграция с 1С
        # Пока читаем из Excel файла
        df = pd.read_excel('last_readings.xlsx')
        return df
    except Exception as e:
        logger.error(f"Ошибка при получении последних показаний: {e}")
        return pd.DataFrame()

def get_active_users(cursor):
    """Получение списка активных пользователей на вахте"""
    try:
        cursor.execute('''
            SELECT u.tab_number, u.name, u.location, u.division, u.t_number 
            FROM Users_user_bot u
            JOIN shifts s ON u.tab_number = s.tab_number
            WHERE s.is_on_shift = "ДА"
        ''')
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"Ошибка при получении активных пользователей: {e}")
        return []

def create_user_excel(equipment_data, user_info):
    """Создание персональной таблицы Excel для пользователя"""
    try:
        df = pd.DataFrame(columns=[
            '№ п/п', 'Гос. номер', 'Инв. №', 'Счётчик', 
            'Показания', 'Комментарий'
        ])
        
        # Фильтрация оборудования по локации и подразделению
        user_equipment = equipment_data[
            (equipment_data['Локация'] == user_info['location']) & 
            (equipment_data['Подразделение'] == user_info['division'])
        ]
        
        # Заполнение таблицы
        for idx, row in user_equipment.iterrows():
            df.loc[len(df)] = {
                '№ п/п': len(df) + 1,
                'Гос. номер': row['Гос. номер'],
                'Инв. №': row['Инв. №'],
                'Счётчик': row['Счётчик'],
                'Показания': row['Последние показания'],
                'Комментарий': ''
            }
        
        # Создание буфера для файла Excel
        excel_buffer = io.BytesIO()
        df.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)
        
        return excel_buffer
    except Exception as e:
        logger.error(f"Ошибка при создании Excel файла: {e}")
        return None

def weekly_data_preparation(context):
    """Подготовка и рассылка таблиц по средам"""
    try:
        # Получение последних показаний
        equipment_data = get_last_readings()
        if equipment_data.empty:
            logger.error("Не удалось получить данные о показаниях")
            return
        
        # Получение списка активных пользователей
        conn = context.bot_data.get('db_connection')
        if not conn:
            logger.error("Нет подключения к базе данных")
            return
            
        cursor = conn.cursor()
        active_users = get_active_users(cursor)
        
        # Формирование и отправка персональных таблиц
        for user in active_users:
            tab_number, name, location, division, t_number = user
            
            user_info = {
                'tab_number': tab_number,
                'name': name,
                'location': location,
                'division': division,
                't_number': t_number
            }
            
            # Создание персональной таблицы
            excel_file = create_user_excel(equipment_data, user_info)
            if not excel_file:
                continue
            
            # Формирование сообщения
            message = (
                f"🔔 Уважаемый(ая) {name}!\n\n"
                f"Напоминаем о необходимости подать показания счетчиков "
                f"до пятницы 14:00 МСК.\n\n"
                f"📍 Локация: {location}\n"
                f"🏢 Подразделение: {division}\n\n"
                f"К сообщению прикреплена таблица для заполнения.\n"
                f"Пожалуйста, заполните столбец 'Показания' и при необходимости "
                f"добавьте комментарии из списка:\n"
                f"- Неисправен\n"
                f"- В ремонте\n"
                f"- Убыло"
            )
            
            # Отправка сообщения и файла
            context.bot.send_message(
                chat_id=t_number,
                text=message
            )
            
            context.bot.send_document(
                chat_id=t_number,
                document=InputFile(
                    excel_file,
                    filename=f'readings_{location}_{division}_{datetime.now().strftime("%Y%m%d")}.xlsx'
                )
            )
            
            # Сохраняем информацию о напоминании
            if 'reminders' not in context.bot_data:
                context.bot_data['reminders'] = {}
            context.bot_data['reminders'][tab_number] = {
                'name': name,
                'location': location,
                'division': division,
                't_number': t_number,
                'status': 'sent'
            }
            
    except Exception as e:
        logger.error(f"Ошибка в weekly_data_preparation: {e}")

def check_missing_reports(context):
    """Проверка неподанных показаний в пятницу 14:00"""
    try:
        # Получаем список отправленных напоминаний
        reminders = context.bot_data.get('reminders', {})
        
        # Проверяем папку с полученными отчетами
        current_week = datetime.now().strftime('%Y-W%U')
        reports_folder = f'meter_readings/week_{current_week}'
        
        # Получаем список файлов с отчетами
        submitted_reports = set()
        if os.path.exists(reports_folder):
            for filename in os.listdir(reports_folder):
                # Извлекаем табельный номер из имени файла
                parts = filename.split('_')
                if len(parts) >= 4:
                    tab_number = parts[3].split('.')[0]
                    submitted_reports.add(int(tab_number))
        
        # Проверяем, кто не подал отчеты
        for tab_number, user_info in reminders.items():
            if int(tab_number) not in submitted_reports:
                # Отправляем повторное напоминание
                message = (
                    f"⚠️ Уважаемый(ая) {user_info['name']}!\n\n"
                    f"Напоминаем, что сегодня до 14:00 МСК необходимо подать "
                    f"показания счетчиков.\n\n"
                    f"📍 Локация: {user_info['location']}\n"
                    f"🏢 Подразделение: {user_info['division']}\n\n"
                    f"Если вы уже отправили показания, пожалуйста, проигнорируйте "
                    f"это сообщение."
                )
                
                context.bot.send_message(
                    chat_id=user_info['t_number'],
                    text=message
                )
                
                # Обновляем статус напоминания
                reminders[tab_number]['status'] = 'reminded'
        
        # Сохраняем обновленные данные
        context.bot_data['reminders'] = reminders
        
    except Exception as e:
        logger.error(f"Ошибка в check_missing_reports: {e}")

def notify_administrators(context):
    """Уведомление администраторов в пятницу 15:00"""
    try:
        # Получаем список отправленных напоминаний
        reminders = context.bot_data.get('reminders', {})
        
        # Получаем текущую неделю
        current_week = datetime.now().strftime('%Y-W%U')
        reports_folder = f'meter_readings/week_{current_week}'
        
        # Получаем список поданных отчетов
        submitted_reports = set()
        if os.path.exists(reports_folder):
            for filename in os.listdir(reports_folder):
                parts = filename.split('_')
                if len(parts) >= 4:
                    tab_number = parts[3].split('.')[0]
                    submitted_reports.add(int(tab_number))
        
        # Группируем неподанные отчеты по локациям и подразделениям
        missing_reports = {}
        for tab_number, user_info in reminders.items():
            if int(tab_number) not in submitted_reports:
                key = (user_info['location'], user_info['division'])
                if key not in missing_reports:
                    missing_reports[key] = []
                missing_reports[key].append(user_info)
        
        # Получаем список администраторов
        conn = context.bot_data.get('db_connection')
        cursor = conn.cursor()
        
        # Уведомляем администраторов
        for (location, division), users in missing_reports.items():
            # Находим ответственного администратора
            cursor.execute('''
                SELECT tab_number, name, t_number 
                FROM Users_admin_bot 
                WHERE location = ? AND division = ?
            ''', (location, division))
            admin = cursor.fetchone()
            
            if admin:
                admin_tab, admin_name, admin_number = admin
                
                # Формируем сообщение
                message = (
                    f"🚨 Внимание, администратор {admin_name}!\n\n"
                    f"Отсутствуют показания счетчиков:\n"
                    f"📍 Локация: {location}\n"
                    f"🏢 Подразделение: {division}\n\n"
                    f"Не предоставили данные:\n"
                )
                
                for user in users:
                    message += f"- {user['name']}\n"
                
                message += "\nТребуется ваше вмешательство."
                
                # Отправляем уведомление
                context.bot.send_message(
                    chat_id=admin_number,
                    text=message
                )
                
                # Сохраняем информацию об уведомлении администратора
                if 'admin_notifications' not in context.bot_data:
                    context.bot_data['admin_notifications'] = {}
                
                context.bot_data['admin_notifications'][(location, division)] = {
                    'admin_tab': admin_tab,
                    'admin_name': admin_name,
                    'users': users,
                    'timestamp': datetime.now().timestamp()
                }
        
    except Exception as e:
        logger.error(f"Ошибка в notify_administrators: {e}")

def notify_managers(context):
    """Уведомление руководителей в понедельник 08:00"""
    try:
        # Получаем информацию о неподанных отчетах и уведомлениях администраторов
        admin_notifications = context.bot_data.get('admin_notifications', {})
        
        # Проверяем, были ли какие-то действия от администраторов
        conn = context.bot_data.get('db_connection')
        cursor = conn.cursor()
        
        for (location, division), notification in admin_notifications.items():
            # Проверяем, прошло ли достаточно времени
            time_passed = datetime.now().timestamp() - notification['timestamp']
            if time_passed < 60 * 60 * 24 * 2:  # 2 дня
                continue
            
            # Находим ответственного руководителя
            cursor.execute('''
                SELECT tab_number, name, t_number 
                FROM Users_dir_bot 
                WHERE location = ? AND division = ?
            ''', (location, division))
            manager = cursor.fetchone()
            
            if manager:
                manager_tab, manager_name, manager_number = manager
                
                # Формируем сообщение
                message = (
                    f"🚨 Внимание, руководитель {manager_name}!\n\n"
                    f"Требуется ваше вмешательство:\n"
                    f"📍 Локация: {location}\n"
                    f"🏢 Подразделение: {division}\n\n"
                    f"Администратор {notification['admin_name']} не предпринял "
                    f"необходимых действий по отсутствующим показаниям.\n\n"
                    f"Не предоставили данные:\n"
                )
                
                for user in notification['users']:
                    message += f"- {user['name']}\n"
                
                # Отправляем уведомление
                context.bot.send_message(
                    chat_id=manager_number,
                    text=message
                )
        
    except Exception as e:
        logger.error(f"Ошибка в notify_managers: {e}")

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

def notify_users(self, message, users=None):
    """Отправка уведомлений пользователям"""
    try:
        if users is None:
            users = self.get_users_info()
        
        for user in users:
            tab_number, name, location, division = user
            user_info = {
                'tab_number': tab_number,
                'name': name,
                'location': location,
                'division': division
            }
            
            try:
                self.bot.send_message(
                    chat_id=tab_number,  # Используем tab_number как chat_id
                    text=message,
                    parse_mode='Markdown'
                )
                logger.info(f"Уведомление отправлено пользователю {name}")
            except Exception as e:
                logger.error(f"Ошибка отправки уведомления пользователю {name}: {e}")
                
                try:
                    self.bot.send_message(
                        chat_id=tab_number,
                        text="❌ Ошибка отправки уведомления. Пожалуйста, свяжитесь с администратором.",
                        parse_mode='Markdown'
                    )
                except:
                    pass
                    
        return True
    except Exception as e:
        logger.error(f"Ошибка массовой рассылки уведомлений: {e}")
        return False

def notify_admins(self, message, user_info=None):
    """Отправка уведомлений администраторам"""
    try:
        # Получаем список администраторов
        self.cursor.execute('''
            SELECT tab_number, name
            FROM Users_admin_bot
        ''')
        admins = self.cursor.fetchall()
        
        for admin in admins:
            try:
                self.bot.send_message(
                    chat_id=admin[0],  # Используем tab_number как chat_id
                    text=message,
                    parse_mode='Markdown'
                )
                logger.info(f"Уведомление отправлено администратору {admin[1]}")
            except Exception as e:
                logger.error(f"Ошибка отправки уведомления администратору {admin[1]}: {e}")
        
        return True
    except Exception as e:
        logger.error(f"Ошибка отправки уведомлений администраторам: {e}")
        return False