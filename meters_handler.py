import pandas as pd
from telegram import Update, InputFile, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext, MessageHandler, Filters
import io
import os
from datetime import time, datetime
import pytz
import sqlite3
import logging
from typing import Dict, List, Tuple
import glob
from time_utils import RUSSIAN_TIMEZONES
from check import MeterValidator

# Настройка логгирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

conn = sqlite3.connect('Users_bot.db', check_same_thread=False)
cursor = conn.cursor()

# Состояния для ConversationHandler
WAITING_FOR_METERS_DATA = 1

def get_timezone_for_location(location: str) -> str:
    """Определяем часовой пояс по названию локации"""
    # Проверяем первые 5 букв локации
    location_prefix = location.strip()[:5].capitalize()
    
    if location_prefix in RUSSIAN_TIMEZONES:
        return RUSSIAN_TIMEZONES[location_prefix]
    
    # Если не нашли по первым 5 буквам, пробуем найти по содержанию
    location_lower = location.lower()
    
    # Поиск по наиболее характерным частям названий
    if 'москв' in location_lower:
        return 'Europe/Moscow'
    elif 'калин' in location_lower:
        return 'Europe/Kaliningrad'
    elif 'самар' in location_lower or 'саратов' in location_lower:
        return 'Europe/Samara'
    elif 'екатер' in location_lower or 'свердл' in location_lower:
        return 'Asia/Yekaterinburg'
    elif 'омск' in location_lower:
        return 'Asia/Omsk'
    elif 'красноярск' in location_lower:
        return 'Asia/Krasnoyarsk'
    elif 'краснодар' in location_lower:
        return 'Europe/Moscow'
    elif 'иркут' in location_lower or 'бурят' in location_lower:
        return 'Asia/Irkutsk'
    elif 'якут' in location_lower or 'саха' in location_lower:
        return 'Asia/Yakutsk'
    elif 'владив' in location_lower or 'примор' in location_lower:
        return 'Asia/Vladivostok'
    elif 'магад' in location_lower or 'сахал' in location_lower:
        return 'Asia/Magadan'
    elif 'камчат' in location_lower or 'чукот' in location_lower:
        return 'Asia/Kamchatka'
    
    # По умолчанию возвращаем московское время
    return 'Europe/Moscow'

def get_local_datetime(location: str) -> datetime:
    """Получает текущее время в указанной локации"""
    timezone_str = get_timezone_for_location(location)
    timezone = pytz.timezone(timezone_str)
    return datetime.now(timezone)

def format_datetime_for_timezone(dt: datetime, location: str) -> str:
    """Форматирует дату/время с учетом часового пояса локации"""
    timezone_str = get_timezone_for_location(location)
    timezone = pytz.timezone(timezone_str)
    local_dt = dt.astimezone(timezone)
    return local_dt.strftime('%Y-%m-%d %H:%M:%S (%Z)')

def get_equipment_data() -> pd.DataFrame:
    """Получаем данные об оборудовании из 1С:ERP (заглушка)"""
    try:
        # В реальной реализации здесь будет подключение к 1С:ERP через шину данных
        # Примерный код подключения к 1С:ERP:
        # import requests
        # erp_api_url = os.getenv('ERP_API_URL', 'http://erp.example.com/api/equipment')
        # response = requests.get(erp_api_url, headers={'Authorization': os.getenv('ERP_API_KEY')})
        # if response.status_code == 200:
        #     data = response.json()
        #     equipment_df = pd.DataFrame(data)
        #     logger.info("Данные об оборудовании успешно загружены из 1С:ERP")
        #     return equipment_df
        
        # Временная заглушка - чтение из локального файла
        try:
            equipment_df = pd.read_excel('Equipment.xlsx')
            logger.info("Данные об оборудовании успешно загружены")
            return equipment_df
        except FileNotFoundError:
            logger.warning("Файл Equipment.xlsx не найден. Создаем пустой DataFrame")
            # Создаем пустой DataFrame с необходимой структурой
            return pd.DataFrame(columns=['№ п/п', 'Гос. номер', 'Инв. №', 'Счётчик', 'Локация', 'Подразделение'])
    except Exception as e:
        logger.error(f"Ошибка загрузки данных об оборудовании: {e}")
        return pd.DataFrame(columns=['№ п/п', 'Гос. номер', 'Инв. №', 'Счётчик', 'Локация', 'Подразделение'])

def get_users_on_shift() -> List[Tuple[int, str, str, str]]:
    """Получаем список пользователей на вахте"""
    try:
        cursor.execute('''
            SELECT u.tab_number, u.name, u.location, u.division 
            FROM Users_user_bot u
            JOIN shifts s ON u.tab_number = s.tab_number
            WHERE s.is_on_shift = "ДА"
        ''')
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"Ошибка получения пользователей на вахте: {e}")
        return []

def schedule_weekly_reminders(context: CallbackContext):
    """Планирование еженедельных напоминаний"""
    try:
        logger.info("Запуск планирования еженедельных напоминаний")
        
        # Москва - базовый часовой пояс для планирования
        moscow_tz = pytz.timezone('Europe/Moscow')
        
        job_kwargs = {
            'misfire_grace_time': 3600,  # Допустимая задержка 1 час
            'coalesce': True,  # Объединять пропущенные запуски
            'max_instances': 1  # Максимум 1 экземпляр задания
        }
        
        # Планируем задание на среду в 08:00 МСК
        context.job_queue.run_daily(
            callback=prepare_weekly_reminders,
            time=time(hour=8, minute=0, tzinfo=moscow_tz),
            days=(2,),  # 2 - среда
            name="weekly_meters_reminder",
            job_kwargs=job_kwargs
        )
        
        # Планируем задание на пятницу в 14:00 МСК для проверки отправленных данных
        context.job_queue.run_daily(
            callback=check_missing_reports,
            time=time(hour=14, minute=0, tzinfo=moscow_tz),
            days=(4,),  # 4 - пятница
            name="check_reports_14_00",
            job_kwargs=job_kwargs
        )
        
        # Планируем задание на пятницу в 15:00 МСК для уведомления администраторов
        context.job_queue.run_daily(
            callback=notify_admins_about_missing_reports,
            time=time(hour=15, minute=0, tzinfo=moscow_tz),
            days=(4,),  # 4 - пятница
            name="notify_admins_15_00",
            job_kwargs=job_kwargs
        )
        
        # Планируем задание на понедельник в 08:00 МСК для уведомления руководителей
        context.job_queue.run_daily(
            callback=notify_managers_about_missing_reports,
            time=time(hour=8, minute=0, tzinfo=moscow_tz),
            days=(0,),  # 0 - понедельник
            name="notify_managers_monday_08_00",
            job_kwargs=job_kwargs
        )
        
        logger.info("Все еженедельные напоминания и проверки запланированы")
    except Exception as e:
        logger.error(f"Ошибка планирования еженедельных напоминаний: {e}")

def prepare_weekly_reminders(context: CallbackContext):
    """Подготовка еженедельных напоминаний в среду"""
    try:
        logger.info("Подготовка еженедельных напоминаний")
        
        # Получаем данные из 1С:ERP
        equipment_df = get_equipment_data()
        if equipment_df.empty:
            logger.error("Не удалось загрузить данные об оборудовании")
            return
        
        # Получаем пользователей на вахте
        users_on_shift = get_users_on_shift()
        if not users_on_shift:
            logger.info("Нет пользователей на вахте")
            return
        
        # Группируем оборудование по локациям и подразделениям
        grouped_equipment = equipment_df.groupby(['Локация', 'Подразделение'])
        
        # Для каждого пользователя на вахте готовим напоминание
        for user in users_on_shift:
            tab_number, name, location, division = user
            
            # Получаем оборудование для локации и подразделения пользователя
            try:
                equipment = grouped_equipment.get_group((location, division))
                if not equipment.empty:
                    # Планируем отправку напоминания на среду 08:00 МСК
                    schedule_reminder(
                        context=context,
                        tab_number=tab_number,
                        name=name,
                        location=location,
                        division=division,
                        equipment=equipment,
                        hour=8,  # Фиксированное время 08:00 МСК
                        timezone=pytz.timezone('Europe/Moscow')
                    )
            except KeyError:
                logger.info(f"Нет оборудования для {location}, {division}")
                continue
                
    except Exception as e:
        logger.error(f"Ошибка подготовки еженедельных напоминаний: {e}")

def schedule_reminder(context: CallbackContext, tab_number: int, name: str, 
                    location: str, division: str, equipment: pd.DataFrame,
                    hour: int, timezone: pytz.timezone):
    """Планирование напоминания"""
    try:
        # Используем московское время как базовое
        moscow_tz = pytz.timezone('Europe/Moscow')
        
        # Планируем на среду в 08:00 МСК
        context.job_queue.run_daily(
            callback=send_reminder,
            time=time(hour=8, minute=0),  # 08:00 МСК
            days=(2,),  # 2 - это среда
            context={
                'tab_number': tab_number,
                'name': name,
                'location': location,
                'division': division,
                'equipment': equipment.to_dict('records'),
                'deadline': '14:00 МСК'  # Срок сдачи показаний
            },
            name=f"reminder_{tab_number}",
            timezone=moscow_tz  # Используем московское время
        )
        
        logger.info(f"Напоминание для {name} запланировано на среду 08:00 МСК")
    except Exception as e:
        logger.error(f"Ошибка планирования напоминания для {tab_number}: {e}")

def send_reminder(context: CallbackContext):
    """Отправка напоминания"""
    job_context = context.job.context
    tab_number = job_context['tab_number']
    name = job_context['name']
    location = job_context['location']
    division = job_context['division']
    equipment = pd.DataFrame.from_records(job_context['equipment'])
    deadline = job_context['deadline']
    
    try:
        # Получаем местное время
        local_tz = pytz.timezone(get_timezone_for_location(location))
        current_local_time = datetime.now(local_tz)
        formatted_time = current_local_time.strftime('%Y-%m-%d %H:%M:%S (%Z)')
        
        # Получаем московское время для дедлайна
        moscow_tz = pytz.timezone('Europe/Moscow')
        deadline_time = time(hour=14, minute=0, tzinfo=moscow_tz)
        deadline_datetime = datetime.combine(datetime.now(moscow_tz).date(), deadline_time)
        
        # Конвертируем дедлайн в местное время
        local_deadline = deadline_datetime.astimezone(local_tz)
        local_deadline_str = local_deadline.strftime('%H:%M (%Z)')
        
        # Создаем шаблон таблицы
        template_df = pd.DataFrame(columns=[
            '№ п/п', 'Гос. номер', 'Инв. №', 'Счётчик', 'Показания', 'Комментарий'
        ])
        
        template_df['№ п/п'] = equipment['№ п/п']
        template_df['Гос. номер'] = equipment['Гос. номер']
        template_df['Инв. №'] = equipment['Инв. №']
        template_df['Счётчик'] = equipment['Счётчик']
        
        # Сохраняем в Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            template_df.to_excel(writer, index=False)
        output.seek(0)
        
        # Отправляем пользователю
        context.bot.send_message(
            chat_id=tab_number,
            text=f"⏰ *Уважаемый {name}, необходимо подать показания счетчиков!*\n\n"
                f"📍 Локация: {location}\n"
                f"🏢 Подразделение: {division}\n"
                f"🕒 Срок подачи: сегодня до {local_deadline_str}\n"
                f"🕒 Текущее время: {formatted_time}\n\n"
                "Заполните столбцы 'Показания' и 'Комментарий' и отправьте файл обратно.",
            parse_mode='Markdown'
        )
        
        context.bot.send_document(
            chat_id=tab_number,
            document=InputFile(output, filename=f'Показания_{location}_{division}.xlsx'),
            caption="Шаблон для заполнения показаний счетчиков"
        )
        
        # Сохраняем информацию о пользователе
        context.user_data['waiting_for_meters'] = True
        context.user_data['location'] = location
        context.user_data['division'] = division
        
        logger.info(f"Напоминание отправлено {name} (tab: {tab_number})")
    except Exception as e:
        logger.error(f"Ошибка отправки напоминания {tab_number}: {e}")

def handle_meters_file(update: Update, context: CallbackContext):
    # При получении файла с показаниями, сохраняем с учетом часового пояса
    try:
        if not update.message.document:
            update.message.reply_text("Пожалуйста, отправьте заполненный файл Excel.")
            return
            
        file = update.message.document
        file_id = file.file_id
        new_file = context.bot.get_file(file_id)
        
        # Создаем папку, если не существует
        os.makedirs('meter_readings', exist_ok=True)
        
        # Создаем папку для отчетов текущей недели, если не существует
        current_week = datetime.now().strftime('%Y-W%U')  # Год-Номер недели
        report_folder = f'meter_readings/week_{current_week}'
        os.makedirs(report_folder, exist_ok=True)
        
        # Получаем данные пользователя
        tab_number = context.user_data.get('tab_number')
        if not tab_number:
            update.message.reply_text("Ошибка: не удалось определить ваш табельный номер. Пожалуйста, запустите /start.")
            return
            
        cursor.execute('''
            SELECT name, location, division FROM Users_user_bot WHERE tab_number = ?
        ''', (tab_number,))
        user_data = cursor.fetchone()
        
        if not user_data:
            update.message.reply_text("Ошибка: пользователь не найден в базе данных.")
            return
            
        name, location, division = user_data
        
        # Получаем текущее время в часовом поясе пользователя
        local_time = get_local_datetime(location)
        timestamp = local_time.strftime('%Y%m%d_%H%M%S')
        
        # Формируем имя файла с учетом часового пояса и недели
        file_path = f'{report_folder}/meters_{location}_{division}_{tab_number}_{timestamp}.xlsx'
        new_file.download(file_path)
        
        # Проверяем, что файл Excel
        if not file.file_name.lower().endswith(('.xlsx', '.xls')):
            update.message.reply_text("Пожалуйста, отправьте файл в формате Excel (.xlsx, .xls)")
            if os.path.exists(file_path):
                os.remove(file_path)
            return
            
        try:
            # Открываем файл и добавляем метаданные
            df = pd.read_excel(file_path)
            
            # Формируем информацию для метаданных
            user_info = {
                'name': name,
                'location': location,
                'division': division,
                'tab_number': tab_number,
                'timestamp': format_datetime_for_timezone(local_time, location)
            }
            
            for key, value in user_info.items():
                if key not in df.columns:
                    df[key] = value
            
            # Сохраняем обновленный файл с метаданными
            df.to_excel(file_path, index=False)
        except Exception as e:
            update.message.reply_text(f"Ошибка при чтении файла Excel: {str(e)}")
            if os.path.exists(file_path):
                os.remove(file_path)
            return
        
        # Проверяем файл с показаниями
        validator = MeterValidator()
        validation_result = validator.validate_file(file_path, user_info)
        
        if not validation_result['is_valid']:
            # Формируем сообщение об ошибках только для пользователя
            errors_text = "\n".join(validation_result['errors'])
            update.message.reply_text(
                f"❌ Обнаружены ошибки:\n\n{errors_text}\n\n"
                "Пожалуйста, исправьте ошибки и отправьте файл повторно."
            )
            
            # Удаляем временный файл
            if os.path.exists(file_path):
                os.remove(file_path)
            
            return
        
        # Код для успешной валидации
        # Получаем московское время для проверки сроков
        moscow_tz = pytz.timezone('Europe/Moscow')
        moscow_now = datetime.now(moscow_tz)
        moscow_time_str = moscow_now.strftime('%H:%M %d.%m.%Y')
        
        # Проверяем, является ли день пятницей (4) и время до 14:00
        is_on_time = moscow_now.weekday() == 4 and moscow_now.hour < 14
        
        # Отправляем подтверждение пользователю
        if is_on_time:
            message_text = (f"✅ Спасибо! Ваши показания счетчиков приняты и прошли проверку.\n\n"
                           f"📍 Локация: {location}\n"
                           f"🏢 Подразделение: {division}\n"
                           f"⏰ Время получения: {moscow_time_str} МСК\n\n"
                           f"Показания предоставлены в срок. Благодарим за своевременную подачу данных!")
        else:
            message_text = (f"✅ Спасибо! Ваши показания счетчиков приняты и прошли проверку.\n\n"
                           f"📍 Локация: {location}\n"
                           f"🏢 Подразделение: {division}\n"
                           f"⏰ Время получения: {moscow_time_str} МСК")
        
        update.message.reply_text(message_text)
        
        # Если есть предупреждения, сообщаем о них
        if validation_result['warnings']:
            warnings_text = "\n".join(validation_result['warnings'])
            update.message.reply_text(f"⚠️ Предупреждения при проверке:\n\n{warnings_text}")
        
        # Удаляем пользователя из списка тех, кому отправлено напоминание
        if 'missing_reports' in context.bot_data and tab_number in context.bot_data['missing_reports']:
            del context.bot_data['missing_reports'][tab_number]
            logger.info(f"Пользователь {name} удален из списка неотправивших отчеты")
        
        # Создаем сводный отчет
        report_generator = context.bot_data.get('report_generator')
        if not report_generator:
            from check import FinalReportGenerator
            report_generator = FinalReportGenerator(context.bot)
            context.bot_data['report_generator'] = report_generator
        
        # Добавляем отчет в сводный
        cycle_id = report_generator.init_new_report_cycle()
        if cycle_id:
            report_path = report_generator.add_user_report(file_path, user_info)
            
            if report_path:
                # Отправляем подтверждение пользователю
                update.message.reply_text("✅ Ваши показания приняты и добавлены в сводный отчет")
        
        # Уведомляем администраторов и руководителей
        notify_admins_and_managers(context, tab_number, name, location, division, file_path)
            
    except Exception as e:
        logger.error(f"Ошибка обработки файла показаний: {e}")
        update.message.reply_text(f"❌ Произошла ошибка при обработке файла: {str(e)}")

def notify_admins_and_managers(context: CallbackContext, user_tab_number: int, user_name: str, 
                             location: str, division: str, file_path: str):
    """Уведомление администраторов и руководителей о новых показаниях"""
    try:
        # Убеждаемся, что папка meter_readings существует
        os.makedirs('meter_readings', exist_ok=True)
        
        # Загружаем данные отчета
        report_df = pd.read_excel(file_path)
        
        # Получаем список всех администраторов
        cursor.execute('SELECT tab_number, name FROM Users_admin_bot')
        admins = cursor.fetchall()
        
        # Получаем список всех руководителей
        cursor.execute('SELECT tab_number, name FROM Users_dir_bot')
        managers = cursor.fetchall()
        
        # Получаем текущее время в часовом поясе локации
        local_time = get_local_datetime(location)
        formatted_time = format_datetime_for_timezone(local_time, location)
        
        # Сообщение
        message = f"📊 *Получены новые показания счетчиков*\n\n" \
                  f"👤 От: {user_name}\n" \
                  f"📍 Локация: {location}\n" \
                  f"🏢 Подразделение: {division}\n" \
                  f"⏰ Время: {formatted_time}"
                  
        # Для администраторов, получаем генератор отчетов из контекста
        report_generator = context.bot_data.get('report_generator')
        if not report_generator:
            from check import FinalReportGenerator
            report_generator = FinalReportGenerator(context.bot)
            context.bot_data['report_generator'] = report_generator
            
        # Инициализируем новый цикл, если еще не инициализирован
        cycle_id = report_generator.init_new_report_cycle()
        
        # Проверяем, что цикл создан успешно
        if not cycle_id:
            logger.error("Не удалось инициализировать цикл отчётности")
            Update.message.reply_text("❌ Ошибка при создании цикла отчётности. Пожалуйста, свяжитесь с администратором.")
            return
            
        # Добавляем отчет пользователя
        user_info = {
            'name': user_name, 
            'location': location, 
            'division': division, 
            'tab_number': user_tab_number
        }
        report_path = report_generator.add_user_report(file_path, user_info)
        
        # Проверяем, что отчет добавлен успешно
        if not report_path:
            logger.error("Не удалось добавить отчет пользователя в цикл")
            return
            
        # Отправляем запрос на подтверждение администраторам
        report_generator.send_verification_request(context, report_path)
        
        # Уведомляем руководителей (без кнопки подтверждения)
        for manager_id, manager_name in managers:
            try:
                # Отправляем уведомление
                context.bot.send_message(
                    chat_id=manager_id,
                    text=f"{message}\n\nОтчёт прикреплен ниже.",
                    parse_mode='Markdown'
                )
                
                # Проверяем существование файла перед отправкой
                if os.path.exists(file_path):
                    # Отправляем файл
                    with open(file_path, 'rb') as f:
                        context.bot.send_document(
                            chat_id=manager_id,
                            document=f,
                            caption=f"Показания счетчиков от {user_name}"
                        )
                else:
                    logger.error(f"Файл не найден при отправке руководителю: {file_path}")
            except Exception as e:
                logger.error(f"Ошибка уведомления руководителя {manager_id}: {e}")
                
    except Exception as e:
        logger.error(f"Ошибка уведомления о новых показаниях: {e}")

def notify_admin_about_errors(context: CallbackContext, user_tab_number: int, user_name: str,
                             location: str, division: str, file_path: str, errors: list):
    """Уведомление администратора о проблемах с файлом показаний"""
    try:
        # Убеждаемся, что папка meter_readings существует
        os.makedirs('meter_readings', exist_ok=True)
        
        # Получаем администраторов данного подразделения
        from check import MeterValidator
        validator = MeterValidator()
        admins = validator.get_admin_for_division(division)
        
        if not admins:
            logger.error(f"Не найдены администраторы для подразделения {division}")
            return
            
        # Формируем текст сообщения
        errors_text = "\n".join(errors)
        local_time = get_local_datetime(location)
        formatted_time = format_datetime_for_timezone(local_time, location)
        
        message = f"⚠️ *Ошибки в показаниях счетчиков*\n\n" \
                  f"👤 От: {user_name}\n" \
                  f"📍 Локация: {location}\n" \
                  f"🏢 Подразделение: {division}\n" \
                  f"⏰ Время: {formatted_time}\n\n" \
                  f"Обнаружены следующие ошибки:\n{errors_text}"
        
        # Отправляем сообщение всем администраторам подразделения
        for admin_id, admin_name in admins:
            try:
                context.bot.send_message(
                    chat_id=admin_id,
                    text=message,
                    parse_mode='Markdown'
                )
                
                # Проверяем существование файла перед отправкой
                if os.path.exists(file_path):
                    # Отправляем файл
                    with open(file_path, 'rb') as f:
                        context.bot.send_document(
                            chat_id=admin_id,
                            document=f,
                            caption=f"Показания счетчиков с ошибками от {user_name}"
                        )
                else:
                    logger.error(f"Файл не найден при отправке администратору: {file_path}")
                    context.bot.send_message(
                        chat_id=admin_id,
                        text=f"⚠️ Файл показаний не найден или был удалён.",
                        parse_mode='Markdown'
                    )
            except Exception as e:
                logger.error(f"Ошибка уведомления администратора {admin_id}: {e}")
                
    except Exception as e:
        logger.error(f"Ошибка уведомления администраторов о проблемах: {e}")

def check_missing_reports(context: CallbackContext):
    """Проверка отсутствующих отчетов в пятницу в 14:00 и отправка повторных напоминаний"""
    try:
        logger.info("Проверка отсутствующих отчетов в 14:00")
        
        # Получаем пользователей на вахте
        users_on_shift = get_users_on_shift()
        if not users_on_shift:
            logger.info("Нет пользователей на вахте для проверки отчетов")
            return
        
        # Создаем папку для отчетов текущей недели, если не существует
        current_week = datetime.now().strftime('%Y-W%U')  # Год-Номер недели
        report_folder = f'meter_readings/week_{current_week}'
        os.makedirs(report_folder, exist_ok=True)
        
        # Проверяем каждого пользователя
        for user in users_on_shift:
            tab_number, name, location, division = user
            
            # Проверяем, подал ли пользователь отчет
            report_pattern = f'{report_folder}/*_{location}_{division}_{tab_number}_*.xlsx'
            user_reports = glob.glob(report_pattern)
            
            if not user_reports:  # Если отчет не найден
                # Отправляем повторное напоминание
                try:
                    moscow_tz = pytz.timezone('Europe/Moscow')
                    current_moscow_time = datetime.now(moscow_tz).strftime('%H:%M')
                    
                    context.bot.send_message(
                        chat_id=tab_number,
                        text=f"⚠️ *ПОВТОРНОЕ НАПОМИНАНИЕ* ⚠️\n\n"
                             f"Уважаемый {name}, вы не подали показания счетчиков!\n\n"
                             f"📍 Локация: {location}\n"
                             f"🏢 Подразделение: {division}\n"
                             f"🕒 Текущее время: {current_moscow_time} МСК\n\n"
                             f"Пожалуйста, подайте показания до 15:00 МСК, иначе о факте неподачи будет уведомлен администратор.",
                        parse_mode='Markdown'
                    )
                    logger.info(f"Отправлено повторное напоминание пользователю {name} (tab: {tab_number})")
                    
                    # Сохраняем информацию о пользователе, которому отправлено повторное напоминание
                    context.bot_data.setdefault('missing_reports', {})
                    context.bot_data['missing_reports'][tab_number] = {
                        'name': name,
                        'location': location,
                        'division': division,
                        'reminder_sent': True
                    }
                except Exception as e:
                    logger.error(f"Ошибка отправки повторного напоминания {tab_number}: {e}")
            else:
                logger.info(f"Пользователь {name} (tab: {tab_number}) уже подал отчет")
                
    except Exception as e:
        logger.error(f"Ошибка проверки отсутствующих отчетов: {e}")

def notify_admins_about_missing_reports(context: CallbackContext):
    """Уведомление администраторов об отсутствующих отчетах в пятницу в 15:00"""
    try:
        # Получаем информацию о пользователях, не подавших отчеты
        missing_reports = context.bot_data.get('missing_reports', {})
        
        for tab_number, user_info in missing_reports.items():
            # Получаем администраторов для этого подразделения
            cursor.execute('''
                SELECT tab_number FROM Users_admin_bot 
                WHERE division = ? AND location = ?
            ''', (user_info['division'], user_info['location']))
            
            admins = cursor.fetchall()
            
            for admin in admins:
                admin_tab = admin[0]
                try:
                    context.bot.send_message(
                        chat_id=admin_tab,
                        text=f"⚠️ Пользователь {user_info['name']} не предоставил показания!\n\n"
                             f"Локация: {user_info['location']}\n"
                             f"Подразделение: {user_info['division']}\n\n"
                             f"Вы можете заполнить показания за пользователя, отправив файл Excel.",
                        parse_mode='Markdown'
                    )
                except Exception as e:
                    logger.error(f"Ошибка уведомления администратора {admin_tab}: {e}")
    except Exception as e:
        logger.error(f"Ошибка уведомления администраторов: {e}")

def notify_managers_about_missing_reports(context: CallbackContext):
    """Уведомление руководителей в понедельник в 08:00"""
    try:
        logger.info("Проверка реакции администраторов и уведомление руководителей в понедельник")
        
        # Получаем информацию о пользователях, не подавших отчеты
        missing_reports = context.bot_data.get('missing_reports', {})
        if not missing_reports:
            logger.info("Нет отсутствующих отчетов, все отчеты обработаны")
            return
        
        # Получаем московское время для сообщения
        moscow_tz = pytz.timezone('Europe/Moscow')
        current_moscow_time = datetime.now(moscow_tz).strftime('%H:%M %d.%m.%Y')
        
        # Для каждого пользователя, не подавшего отчет и по которому уведомлен администратор
        for tab_number, user_info in list(missing_reports.items()):
            if not user_info.get('admin_notified', False):
                continue
                
            name = user_info['name']
            location = user_info['location']
            division = user_info['division']
            admin_name = user_info.get('admin_name', 'Неизвестно')
                
            # Находим руководителей
            try:
                cursor.execute('''
                    SELECT tab_number, name 
                    FROM Users_dir_bot 
                    WHERE division = ?
                ''', (division,))
                managers = cursor.fetchall()
                    
                if not managers:
                    # Если нет руководителей для конкретного подразделения, берем всех
                    cursor.execute('SELECT tab_number, name FROM Users_dir_bot')
                    managers = cursor.fetchall()
                        
                if not managers:
                    logger.error(f"Не найдены руководители для уведомления")
                    continue
                        
                # Уведомляем каждого руководителя
                for manager_id, manager_name in managers:
                    try:
                        context.bot.send_message(
                            chat_id=manager_id,
                            text=f"🚨 *КРИТИЧЕСКОЕ УВЕДОМЛЕНИЕ* 🚨\n\n"
                                f"Руководитель {manager_name}, показания счетчиков не поданы:\n\n"
                                f"👤 Пользователь: {name}\n"
                                f"📍 Локация: {location}\n"
                                f"🏢 Подразделение: {division}\n"
                                f"👨‍💼 Ответственный администратор: {admin_name}\n"
                                f"🕒 Время проверки: {current_moscow_time} МСК\n\n"
                                f"Требуется ваше вмешательство для разрешения ситуации.",
                            parse_mode='Markdown'
                        )
                        logger.info(f"Уведомление отправлено руководителю {manager_name} (ID: {manager_id})")
                            
                    except Exception as e:
                        logger.error(f"Ошибка отправки уведомления руководителю {manager_id}: {e}")
            except Exception as e:
                logger.error(f"Ошибка получения руководителей: {e}")
                
        # Очищаем список отсутствующих отчетов после обработки всех уведомлений
        context.bot_data['missing_reports'] = {}
            
    except Exception as e:
        logger.error(f"Ошибка уведомления руководителей: {e}")

def setup_meters_handlers(dispatcher):
    """Настройка обработчиков для работы с показаниями счетчиков"""
    try:
        # Планируем еженедельные напоминания при старте бота
        # Добавляем небольшую задержку для полной инициализации системы
        dispatcher.job_queue.run_once(
            callback=schedule_weekly_reminders,
            when=10,  # Задержка в 10 секунд для гарантированной инициализации
            name="init_weekly_schedule",
            job_kwargs={'misfire_grace_time': 60}  # Допустимая задержка выполнения в секундах
        )
        
        # Регистрация обработчика файлов с показаниями
        dispatcher.add_handler(
            MessageHandler(
                Filters.document.file_extension("xls") | Filters.document.file_extension("xlsx"),
                handle_meters_file
            )
        )
        
        logger.info("Обработчики показаний счетчиков зарегистрированы")
    except Exception as e:
        logger.error(f"Ошибка настройки обработчиков показаний: {e}")