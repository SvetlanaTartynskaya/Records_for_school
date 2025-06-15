import io
import pandas as pd
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Message
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, ConversationHandler, CallbackContext, CallbackQueryHandler
from telegram.error import NetworkError
import sqlite3
import pytz
from datetime import time, datetime
from shifts_handler import ShiftsHandler
import os
import logging
from dotenv import load_dotenv
from check import MeterValidator
from db_utils import db_transaction

# Загрузка переменных окружения из файла .env
load_dotenv()

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Состояния для ConversationHandler
ENTER_TAB_NUMBER, ENTER_READINGS, WAITING_FOR_ADMIN_CHOICE, WAIT_MANAGER_EXCEL, WAITING_FOR_MANAGER_CHOICE, WAIT_ADMIN_EXCEL, ENTER_ADMIN_READING, WAITING_FOR_CHOICE, SELECT_EQUIPMENT, ENTER_VALUE, CONFIRM_READINGS, WAITING_FOR_FILE, WAIT_EXCEL_FILE, ENTER_READING_VALUE = range(14)

# Инициализация обработчика табеля
shifts_handler = ShiftsHandler()

# Загрузка таблицы пользователей
def load_users_table():
    try:
        df = pd.read_excel('Users.xlsx')
        return df
    except Exception as e:
        print(f"Ошибка при загрузке файла Users.xlsx: {e}")
        return pd.DataFrame()

# Загрузка таблицы смен
def load_shifts_table():
    try:
        df = pd.read_excel('tabels.xlsx')
        return df
    except Exception as e:
        print(f"Ошибка при загрузке файла tabels.xlsx: {e}")
        return pd.DataFrame()

# Обработка команды /start
def start(update: Update, context: CallbackContext) -> int:
    if 'started' in context.user_data:
        return ENTER_TAB_NUMBER

    try:
        user_id = update.effective_user.id
        logger.info(f"Получена команда /start от пользователя {user_id}")
        
        # Очищаем данные пользователя при новом старте
        context.user_data.clear()
        logger.info("Очищены предыдущие данные пользователя")
        
        # Устанавливаем начальное состояние
        context.user_data['state'] = ENTER_TAB_NUMBER
        logger.info("Установлено начальное состояние: ENTER_TAB_NUMBER")
        
        # Отправляем приветственное сообщение
        welcome_message = "Добро пожаловать!\nДля начала работы введите ваш табельный номер:"
        update.message.reply_text(welcome_message)
        logger.info(f"Отправлено приветственное сообщение пользователю {user_id}")
        
        logger.info(f"Переход в состояние ENTER_TAB_NUMBER для пользователя {user_id}")
        return ENTER_TAB_NUMBER
        
    except Exception as e:
        logger.error(f"Ошибка в функции start: {e}", exc_info=True)
        update.message.reply_text(
            "Произошла ошибка при запуске бота.\n"
            "Пожалуйста, попробуйте позже или обратитесь к администратору."
        )
        return ConversationHandler.END

def check_tab_number_exists_in_excel(tab_number):
    """Проверка существования табельного номера в Users.xlsx"""
    try:
        df_users = load_users_table()
        if df_users.empty:
            logger.error("Файл Users.xlsx пуст или не загружен")
            return None
            
        # Проверяем, что столбец существует
        if 'Табельный номер' not in df_users.columns:
            logger.error("В файле Users.xlsx отсутствует столбец 'Табельный номер'")
            return None
            
        # Преобразуем табельные номера к строковому типу для сравнения
        user_data = df_users[df_users['Табельный номер'].astype(str) == str(tab_number)]
        
        if not user_data.empty:
            logger.info(f"Найден пользователь с табельным номером {tab_number}")
            return user_data
            
        logger.warning(f"Пользователь с табельным номером {tab_number} не найден")
        return None
        
    except Exception as e:
        logger.error(f"Ошибка при проверке табельного номера в Users.xlsx: {e}")
        return None
    
def error_handler(update: Update, context: CallbackContext):
    """Логируем ошибки"""
    logger.error(f'Update {update} caused error {context.error}', exc_info=context.error)
    
    # Для сетевых ошибок
    if isinstance(context.error, NetworkError):
        logger.error("Проблемы с подключением к Telegram API")

# Обработка введенного табельного номера
def handle_tab_number(update: Update, context: CallbackContext) -> int:
    try:
        tab_number = int(update.message.text)
        chat_id = update.effective_chat.id 
        
        # Проверяем существование табельного номера в Excel
        user = check_tab_number_exists_in_excel(tab_number)
        
        if user is not None:
            name = user['ФИО'].values[0]
            role = determine_role(user)
            location = user['Локация'].values[0]
            division = user['Подразделение'].values[0] if 'Подразделение' in user.columns else ""
            
            # Добавляем пользователя в базу данных с chat_id
            add_user_to_db(tab_number, name, role, chat_id, location, division)
            
            # Сохраняем данные пользователя в контексте
            context.user_data.update({
                'tab_number': tab_number,  # это точно табельный номер
                'chat_id': chat_id,       # это точно telegram chat_id
                'name': name,
                'role': role,
                'location': location,
                'division': division
            })
                
            update.message.reply_text(
                f"Здравствуйте, {name}!\n"
                f"Ваша роль: {role}\n"
                f"Локация: {location}\n"
                f"Подразделение: {division}"
            )
            
            # Разные сообщения для разных ролей
            if role in ['Администратор', 'Руководитель']:
                update.message.reply_text("✅ Вы имеете постоянный доступ к боту.")
            else:
                if check_shift_status(tab_number):
                    update.message.reply_text("✅ Вы на вахте. Бот доступен для работы.")
                else:
                    update.message.reply_text("⛔ В настоящее время вы не на вахте. Бот недоступен.")
            
            show_role_specific_menu(update, role)
            return ConversationHandler.END
        else:
            update.message.reply_text(
                "Пользователь с таким табельным номером не найден.\n"
                "Пожалуйста, проверьте номер и попробуйте снова:"
            )
            return ENTER_TAB_NUMBER
        
    except Exception as e:
        logger.error(f"Критическая ошибка в handle_tab_number: {e}", exc_info=True)
        update.message.reply_text(
            "Произошла ошибка при обработке табельного номера.\n"
            "Пожалуйста, попробуйте снова или обратитесь к администратору."
        )
        return ENTER_TAB_NUMBER

# Проверка статуса вахты
def check_shift_status(tab_number):
    try:
        # Получаем ФИО сотрудника по табельному номеру
        with db_transaction() as cursor:
            cursor.execute('SELECT name FROM Users_user_bot WHERE tab_number = ?', (tab_number,))
            result = cursor.fetchone()
        if not result:
            return False
            
        employee_name = result[0]
        # Проверяем статус в табеле
        status = shifts_handler.check_employee_status(employee_name)
        if not status:
            return False
            
        return status == 'ДА'
    except Exception as e:
        logger.error(f"Ошибка при проверке статуса вахты: {e}")
        return False

def is_user_available(tab_number: int, role: str) -> bool:
    try:
        # Руководители и администраторы всегда имеют доступ
        if role in ['Руководитель', 'Администратор']:
            return True
            
        # Пользователи также всегда имеют доступ для тестирования
        return True
    except Exception as e:
        logger.error(f"Ошибка проверки доступности: {e}")
        return True

def check_access(update: Update, context: CallbackContext) -> bool:
    # Проверка доступа перед выполнением команд
    if 'tab_number' not in context.user_data or 'role' not in context.user_data:
        update.message.reply_text("Пожалуйста, сначала введите ваш табельный номер через /start")
        return False
    
    # Всегда возвращаем True для тестирования
    return True

# Определение роли пользователя
def determine_role(user):
    role = user['Роль'].values[0] if 'Роль' in user.columns else "Пользователь"
    
    if 'Администратор' in str(role):
        return 'Администратор'
    elif 'Руководитель' in str(role):
        return 'Руководитель'
    else:
        return 'Пользователь'

# Показ меню в зависимости от роли
def show_role_specific_menu(update: Update, role: str):
    if role == 'Администратор':
        keyboard = [
            ['Посмотреть показания за эту неделю'],
            ['В начало']
        ]
    elif role == 'Руководитель':
        keyboard = [
            ['Посмотреть показания за эту неделю'],
            ['В начало']
        ]
    else:  # Оператор
        keyboard = [
            ['Загрузить показания'],
            ['В начало']
        ]
    
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
    
    if role == 'Администратор':
        update.message.reply_text("Доступные команды для администратора:", reply_markup=reply_markup)
    elif role == 'Руководитель':
        update.message.reply_text("Доступные команды для руководителя:", reply_markup=reply_markup)
    else:
        update.message.reply_text("Доступные команды для оператора:", reply_markup=reply_markup)

def handle_button(update: Update, context: CallbackContext):
    text = update.message.text
    if text == 'В начало':
        return return_to_start(update, context)
    elif text == 'Посмотреть показания за эту неделю':
        return handle_view_week_report(update, context)

# Удаление пользователя из базы данных
def delete_user(tab_number, role):
    try:
        if role == 'Администратор':
            with db_transaction() as cursor:
                cursor.execute('DELETE FROM Users_admin_bot WHERE tab_number = ?', (tab_number,))
        elif role == 'Руководитель':
            with db_transaction() as cursor:
                cursor.execute('DELETE FROM Users_dir_bot WHERE tab_number = ?', (tab_number,))
        else:
            with db_transaction() as cursor:
                cursor.execute('DELETE FROM Users_user_bot WHERE tab_number = ?', (tab_number,))
        
        # Также удаляем из таблицы смен
        cursor.execute('DELETE FROM shifts WHERE tab_number = ?', (tab_number,))
        return True
    except Exception as e:
        print(f"Ошибка при удалении пользователя: {e}")
        return False

# Проверка, существует ли пользователь в базе данных
def is_user_in_db(tab_number, role):
    try:
        if role == 'Администратор':
            with db_transaction() as cursor:
                cursor.execute('SELECT * FROM Users_admin_bot WHERE tab_number = ?', (tab_number,))
        elif role == 'Руководитель':
            with db_transaction() as cursor:
                cursor.execute('SELECT * FROM Users_dir_bot WHERE tab_number = ?', (tab_number,))
        else:
            with db_transaction() as cursor:
                cursor.execute('SELECT * FROM Users_user_bot WHERE tab_number = ?', (tab_number,))
        
        return cursor.fetchone() is not None
    except Exception as e:
        print(f"Ошибка при проверке пользователя в БД: {e}")
        return False

# Добавление пользователя в соответствующую таблицу базы данных
def add_user_to_db(tab_number, name, role, chat_id, location, division):
    """Добавление пользователя в базу данных"""
    try:
        table_name = {
            'Администратор': 'Users_admin_bot',
            'Руководитель': 'Users_dir_bot',
            'Пользователь': 'Users_user_bot'
        }.get(role, 'Users_user_bot')
        
        with db_transaction() as cursor:
            cursor.execute(f'''
                INSERT OR REPLACE INTO {table_name} 
                (tab_number, name, role, chat_id, location, division) 
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (tab_number, name, role, chat_id, location, division))
        return True
    except Exception as e:
        logger.error(f"Ошибка добавления пользователя в БД: {e}")
        return False
    
def check_admin_chat_ids(context: CallbackContext):
    """Проверка и обновление chat_id администраторов"""
    try:
        with db_transaction() as cursor:  
            cursor.execute('SELECT tab_number, name FROM Users_admin_bot')
            admins = cursor.fetchall()
            
            for admin in admins:
                try:
                    chat = context.bot.get_chat(admin[0])
                    cursor.execute('UPDATE Users_admin_bot SET chat_id = ? WHERE tab_number = ?', 
                                 (chat.id, admin[0]))
                    logger.info(f"Обновлен chat_id для администратора {admin[1]}")
                except Exception as e:
                    logger.error(f"Не удалось обновить chat_id для администратора {admin[1]}: {e}")
    except Exception as e:
        logger.error(f"Ошибка при проверке chat_id администраторов: {e}")

def update_shifts_from_excel():
    try:
        df = load_shifts_table()
        if not df.empty:
            # Очистка таблицы перед обновлением
            with db_transaction() as cursor:
                cursor.execute('DELETE FROM shifts')
            
            # Вставка новых данных
            for _, row in df.iterrows():
                tab_number = row['tab_number'] if 'tab_number' in row else None
                name = row['name'] if 'name' in row else row['ФИО'] if 'ФИО' in row else None
                shift_status = str(row['is_on_shift']).upper().strip() if 'is_on_shift' in row and pd.notna(row['is_on_shift']) else "НЕТ"
                is_on_shift = shift_status in ["ДА", "YES", "TRUE", "1", "1.0"]
                
                if tab_number and name:
                    cursor.execute('''
                    INSERT INTO shifts (name, tab_number, is_on_shift)
                    VALUES (?, ?, ?)
                    ON CONFLICT(tab_number) DO UPDATE SET
                        name = excluded.name,
                        is_on_shift = excluded.is_on_shift
                    ''', (name, tab_number, is_on_shift))
            print("Данные о сменах в БД обновлены.")
    except FileNotFoundError:
        print("Файл tabels.xlsx не найден.")
    except Exception as e:
        print(f"Ошибка при обновлении таблицы смен: {e}")

# Обновление всех таблиц из Excel
def update_db_from_excel():
    try:
        # Обновляем таблицу пользователей
        df_users = load_users_table()
        if not df_users.empty:
            # Очистка таблиц перед обновлением
            with db_transaction() as cursor:
                cursor.execute('DELETE FROM Users_admin_bot')
                cursor.execute('DELETE FROM Users_dir_bot')
                cursor.execute('DELETE FROM Users_user_bot')
            
            # Вставка новых данных
            for _, row in df_users.iterrows():
                tab_number = row['Табельный номер']
                name = row['ФИО']
                role = determine_role(pd.DataFrame([row]))
                t_number = row['Номер телефона']
                location = row['Локация']
                division = row['Подразделение'] if 'Подразделение' in row else ""
                
                add_user_to_db(tab_number, name, role, t_number, location, division)
            print("Данные пользователей в БД обновлены.")
        
        # Обновляем таблицу смен
        update_shifts_from_excel()
        
    except Exception as e:
        print(f"Ошибка при обновлении БД: {e}")

def daily_update(context: CallbackContext):
    """Ежедневное обновление данных и отправка уведомлений"""
    try:
        # Обновляем данные из табеля
        shifts_handler.load_tabel()
        
    except Exception as e:
        logger.error(f"Ошибка при ежедневном обновлении: {e}")

def cancel(update: Update, context: CallbackContext) -> int:
    """Отменяет текущее действие и возвращает пользователя в главное меню."""
    user = update.message.from_user
    context.user_data.clear()  # Очищаем временные данные пользователя
    
    # Получаем роль пользователя из контекста или базы данных
    role = context.user_data.get('role')
    if not role:
        try:
            tab_number = context.user_data.get('tab_number')
            if tab_number:
                with db_transaction() as cursor:
                    cursor.execute('SELECT role FROM Users_user_bot WHERE tab_number = ?', (tab_number,))
                result = cursor.fetchone()
                role = result[0] if result else 'Пользователь'
        except Exception as e:
            print(f"Ошибка при получении роли: {e}")
            role = 'Пользователь'
    
    update.message.reply_text(
        "❌ Текущее действие отменено.\n\n"
        "Вы можете начать заново с команды /start",
        reply_markup=ReplyKeyboardMarkup([['/start']], one_time_keyboard=True)
    )
    if role:
        show_role_specific_menu(update, role)
    
    return ConversationHandler.END

def return_to_start(update: Update, context: CallbackContext):
    context.user_data.clear()
    
    # Отправляем сообщение с инструкцией
    update.message.reply_text(
        "Вы вернулись в начало работы с ботом.\n\n"
        "Для начала работы введите ваш табельный номер:",
        reply_markup=ReplyKeyboardMarkup([['/start']], one_time_keyboard=True)
    )
    
    # Возвращаем состояние ENTER_TAB_NUMBER, если используется ConversationHandler
    return ENTER_TAB_NUMBER

# Обработчик команды для администраторов
def admin_command(update: Update, context: CallbackContext):
    # Проверка прав доступа
    if not check_access(update, context):
        return
        
    role = context.user_data.get('role')
    if role != 'Администратор':
        update.message.reply_text("Эта команда доступна только для администраторов.")
        return
        
    keyboard = [
        ['Выгрузить данные', 'Редактировать справочники'],
        ['Список пользователей', 'Связаться с оператором'],
        ['Связаться с руководителем', 'Назад']
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
    update.message.reply_text(
        "Панель администратора. Выберите действие:",
        reply_markup=reply_markup
    )

# Обработчик команды для руководителей
def manager_command(update: Update, context: CallbackContext):
    # Проверка прав доступа
    if not check_access(update, context):
        return
        
    role = context.user_data.get('role')
    if role != 'Руководитель':
        update.message.reply_text("Эта команда доступна только для руководителей.")
        return
        
    keyboard = [
        ['Выгрузить данные'],
        ['Список пользователей', 'Связаться с администратором'],
        ['Назад']
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
    update.message.reply_text(
        "Панель руководителя. Выберите действие:",
        reply_markup=reply_markup
    )

# Обработчик команды для пользователей
def user_command(update: Update, context: CallbackContext):
    # Проверка прав доступа
    if not check_access(update, context):
        return
        
    keyboard = [
        ['Загрузить показания'],
        ['Связаться с администратором', 'Назад']
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
    update.message.reply_text(
        "Панель пользователя. Выберите действие:",
        reply_markup=reply_markup
    )

def handle_upload_readings(update: Update, context: CallbackContext):
    """Обработчик команды 'Загрузить показания'"""
    if not check_access(update, context):
        return ConversationHandler.END
        
    tab_number = context.user_data.get('tab_number')
    
    # Получаем информацию о пользователе с использованием контекстного менеджера
    try:
        with db_transaction() as cursor:
            cursor.execute('''
                SELECT name, location, division FROM Users_user_bot 
                WHERE tab_number = ?
            ''', (tab_number,))
            user_data = cursor.fetchone()
    except Exception as e:
        logger.error(f"Ошибка получения данных пользователя: {e}")
        update.message.reply_text("Ошибка: не удалось получить данные пользователя.")
        return ConversationHandler.END
    
    if not user_data:
        update.message.reply_text("Ошибка: пользователь не найден в базе данных.")
        return ConversationHandler.END
        
    name, location, division = user_data
    
    # Создаем клавиатуру с выбором способа ввода
    keyboard = [
        [InlineKeyboardButton("Загрузить Excel файл", callback_data='upload_excel')],
        [InlineKeyboardButton("Ввести показания вручную", callback_data='enter_readings')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        f"Выберите способ подачи показаний:\n\n"
        f"📍 Локация: {location}\n"
        f"🏢 Подразделение: {division}",
        reply_markup=reply_markup
    )
    
    return WAITING_FOR_CHOICE
    
def generate_excel_template(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    
    # Исправленная версия с использованием контекстного менеджера
    tab_number = context.user_data.get('tab_number')
    try:
        with db_transaction() as cursor:
            cursor.execute('''
                SELECT name, location, division FROM Users_user_bot 
                WHERE tab_number = ?
            ''', (tab_number,))
            user_data = cursor.fetchone()
    except Exception as e:
        logger.error(f"Ошибка получения данных пользователя: {e}")
        query.edit_message_text("Ошибка: не удалось получить данные пользователя.")
        return ConversationHandler.END
    
    if not user_data:
        query.edit_message_text("Ошибка: пользователь не найден.")
        return ConversationHandler.END
        
    name, location, division = user_data
    
    # Создаем шаблон с обязательными колонками
    columns = ['№ п/п', 'Гос. номер', 'Инв. №', 'Счётчик', 'Показания', 'Комментарий']
    template_df = pd.DataFrame(columns=columns)
    
    # Получаем список оборудования и заполняем шаблон
    validator = MeterValidator()
    equipment_df = validator._get_equipment_for_location_division(location, division)
    
    if equipment_df.empty:
        query.edit_message_text("Для вашей локации нет оборудования.")
        return ConversationHandler.END
    
    for idx, row in equipment_df.iterrows():
        template_df.loc[idx] = [
            idx + 1,  # № п/п
            row['Гос. номер'],
            row['Инв. №'],
            row['Счётчик'],
            '',  # Пустые показания
            ''   # Пустой комментарий
        ]
    
    # Сохраняем в буфер
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        template_df.to_excel(writer, index=False, sheet_name='Показания')
    output.seek(0)
    
    # Отправляем пользователю
    query.edit_message_text("Шаблон Excel создан. Заполните столбцы 'Показания' и 'Комментарий' и отправьте файл обратно.")
    
    context.bot.send_document(
        chat_id=query.message.chat_id,
        document=InputFile(output, filename=f'Показания_{location}_{division}.xlsx'),
        caption="Заполните все обязательные колонки перед отправкой"
    )
    
    return WAITING_FOR_FILE


def start_manual_input(update: Update, context: CallbackContext, message=None):
    try:
        if message is None:
            if update.callback_query:
                message = update.callback_query.message
            else:
                message = update.message

        user_type = 'user'
        if context.user_data.get('is_admin_submit'):
            user_type = 'admin'
        elif context.user_data.get('is_manager_submit'):
            user_type = 'manager'

        tab_number = context.user_data.get('tab_number')
        if not tab_number:
            if update.callback_query:
                update.callback_query.answer("Ошибка: не удалось определить ваш табельный номер.")
            else:
                message.reply_text("Ошибка: не удалось определить ваш табельный номер.")
            return ConversationHandler.END
        
        try:
            with db_transaction() as cursor:
                cursor.execute('''
                    SELECT location, division FROM Users_user_bot WHERE tab_number = ?
                ''', (tab_number,))
                user_data = cursor.fetchone()
        except Exception as e:
            logger.error(f"Ошибка получения данных пользователя: {e}")
            if update.callback_query:
                update.callback_query.answer("Ошибка: не удалось получить данные пользователя.")
            else:
                message.reply_text("Ошибка: не удалось получить данные пользователя.")
            return ConversationHandler.END
        
        if not user_data:
            if update.callback_query:
                update.callback_query.answer("Ошибка: пользователь не найден в базе данных.")
            else:
                message.reply_text("Ошибка: пользователь не найден в базе данных.")
            return ConversationHandler.END
            
        location, division = user_data
        
        validator = MeterValidator()
        equipment_df = validator._get_equipment_for_location_division(location, division)
        
        if equipment_df.empty:
            if update.callback_query:
                update.callback_query.answer("Для вашей локации нет оборудования.")
            else:
                message.reply_text("Для вашей локации нет оборудования.")
            return ConversationHandler.END
        
        # Инициализация данных
        context.user_data[f'equipment_{user_type}'] = equipment_df.to_dict('records')
        context.user_data[f'current_index_{user_type}'] = 0
        context.user_data[f'readings_{user_type}'] = []
        
        # Показываем первое оборудование
        return show_next_equipment(update, context, user_type)
            
    except Exception as e:
        logger.error(f"Error in start_manual_input: {e}")
        error_msg = "Ошибка при загрузке оборудования. Попробуйте позже."
        if update.callback_query:
            try:
                update.callback_query.edit_message_text(error_msg)
            except Exception as edit_error:
                logger.error(f"Ошибка при редактировании сообщения: {edit_error}")
                update.callback_query.answer(error_msg)
        else:
            message.reply_text(error_msg)
        return ConversationHandler.END

def show_next_equipment(update: Update, context: CallbackContext, user_type='user'):
    """Универсальная функция для отображения оборудования"""
    try:
        current_idx = context.user_data.get(f'current_index_{user_type}', 0)
        equipment_list = context.user_data.get(f'equipment_{user_type}', [])
        
        # Проверяем, что оборудование закончилось
        if current_idx >= len(equipment_list):
            if user_type == 'admin':
                return finish_admin_readings(update, context)
            elif user_type == 'manager':
                return finish_manager_readings(update, context)
            else:
                return finish_manual_input(update, context)
                
        equipment = equipment_list[current_idx]

        # Получаем последние показания
        validator = MeterValidator()
        inv_num = equipment['Инв. №']
        meter_type = equipment['Счётчик']
        last_reading = validator._get_last_reading(inv_num, meter_type)
        
        # Форматирование сообщения
        message = (
            f"Оборудование {current_idx+1}/{len(equipment_list)}\n"
            f"Гос.номер: {equipment['Гос. номер']}\n"
            f"Инв.№: {inv_num}\n"
            f"Счётчик: {meter_type}\n"
        )
        
        if last_reading and last_reading['reading'] is not None:
            message += f"Последнее показание: {last_reading['reading']} ({last_reading['reading_date']})\n"
        
        # Создаем клавиатуру
        buttons = [
            [InlineKeyboardButton("В ремонте", callback_data=f'repair_{user_type}'),
            InlineKeyboardButton("Пропустить", callback_data=f'skip_{user_type}')],
            [InlineKeyboardButton("Убыло", callback_data=f'ubylo_{user_type}')]
        ]
        
        reply_markup = InlineKeyboardMarkup(buttons)
        
        # Отправляем или редактируем сообщение
        if update.callback_query:
            try:
                update.callback_query.edit_message_text(
                    text=message,
                    reply_markup=reply_markup
                )
            except Exception as edit_error:
                if "message is not modified" in str(edit_error):
                    # Сообщение не изменилось - можно игнорировать
                    pass
                else:
                    raise edit_error
        else:
            update.message.reply_text(
                text=message,
                reply_markup=reply_markup
            )

        return ENTER_ADMIN_READING if user_type == 'admin' else ENTER_READING_VALUE

    except Exception as e:
        logger.error(f"Ошибка show_next_equipment: {e}")
        error_msg = "❌ Ошибка загрузки оборудования. Попробуйте позже."
        if update.callback_query:
            try:
                update.callback_query.edit_message_text(error_msg)
            except:
                update.callback_query.answer(error_msg)
        else:
            update.message.reply_text(error_msg)
        return ConversationHandler.END
    
def back_to_choice(update: Update, context: CallbackContext):
    # Очищаем данные текущего ввода
    if 'readings' in context.user_data:
        del context.user_data['readings']
    if 'current_index' in context.user_data:
        del context.user_data['current_index']
    
    # Создаем клавиатуру с выбором способа
    keyboard = [
        [InlineKeyboardButton("Загрузить Excel файл", callback_data='upload_excel')],
        [InlineKeyboardButton("Ввести показания вручную", callback_data='enter_readings')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        "Выберите способ подачи показаний:",
        reply_markup=reply_markup
    )
    
    return WAITING_FOR_CHOICE
    
def handle_reading_input(update: Update, context: CallbackContext):
    try:
        value = float(update.message.text)
        current_index = context.user_data['current_index']
        equipment = context.user_data['equipment'][current_index]
        
        # Базовая проверка на отрицательные значения
        if value < 0:
            update.message.reply_text("Ошибка: показание не может быть отрицательным.")
            return ENTER_READING_VALUE
        
        validator = MeterValidator()
        last_reading = validator._get_last_reading(equipment['Инв. №'], equipment['Счётчик'])
        
        # Если предыдущее показание None или отсутствует - принимаем любое неотрицательное
        if last_reading is None or last_reading['reading'] is None:
            pass  # Пропускаем проверку сравнения
        else:
            # Если есть предыдущее показание - проверяем корректность
            if value < last_reading['reading']:
                update.message.reply_text(
                    f"Ошибка: новое показание ({value}) меньше предыдущего ({last_reading['reading']})."
                )
                return ENTER_READING_VALUE
        
        # Сохраняем корректное показание
        context.user_data['readings'].append({
            'equipment': equipment,
            'value': value,
            'comment': ''
        })
        
        # Переход к следующему оборудованию
        context.user_data['current_index'] += 1
        if context.user_data['current_index'] < len(context.user_data['equipment']):
            return show_next_equipment(update, context)
        else:
            return finish_manual_input(update, context)
            
    except ValueError:
        update.message.reply_text("Пожалуйста, введите числовое значение.")
        return ENTER_READING_VALUE
    except Exception as e:
        logger.error(f"Ошибка в handle_reading_input: {e}")
        update.message.reply_text("❌ Произошла ошибка. Пожалуйста, попробуйте снова.")
        return ConversationHandler.END
    
def readings_choice_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    
    if query.data == 'upload_excel':
        return generate_excel_template(update, context)
    elif query.data == 'enter_readings':
        return start_manual_input(update, context)
    
def finish_manual_input(update: Update, context: CallbackContext):
    try:
        # Получаем показания из всех возможных ключей
        readings = (
            context.user_data.get('readings') or 
            context.user_data.get('readings_data') or 
            context.user_data.get('readings_user') or 
            context.user_data.get('readings_admin') or  # для случая, когда администратор вводит
            context.user_data.get('readings_manager') or # для случая, когда руководитель вводит
            []
        )
        
        # Логируем для отладки
        logger.info(f"Попытка сохранения показаний. Найдены данные в ключах: {[k for k in context.user_data.keys() if 'readings' in k]}")
        logger.info(f"Всего показаний для сохранения: {len(readings)}")
        
        if not readings:
            logger.error("❌ Нет данных для сохранения. Ключи в user_data: %s", context.user_data.keys())
            update.message.reply_text("❌ Нет данных для сохранения.")
            return ConversationHandler.END
            
        # Получаем информацию о пользователе
        tab_number = context.user_data.get('tab_number')
        if not tab_number:
            logger.error("Не найден tab_number в context.user_data")
            update.message.reply_text("❌ Ошибка: не удалось определить пользователя.")
            return ConversationHandler.END
        
        # Исправленный запрос к БД
        try:
            with db_transaction() as cursor:
                cursor.execute('''
                    SELECT name, location, division FROM Users_user_bot WHERE tab_number = ?
                ''', (tab_number,))
                user_data = cursor.fetchone()
        except Exception as e:
            logger.error(f"Ошибка получения данных пользователя: {e}")
            update.message.reply_text("❌ Ошибка: не удалось получить данные пользователя.")
            return ConversationHandler.END
        
        if not user_data:
            logger.error(f"Пользователь с tab_number {tab_number} не найден в базе")
            update.message.reply_text("❌ Ошибка: пользователь не найден.")
            return ConversationHandler.END
            
        name, location, division = user_data
        
        # Создаем DataFrame с обязательными колонками
        data = []
        for reading in readings:
            equipment = reading.get('equipment', {})
            data.append({
                '№ п/п': len(data) + 1,
                'Гос. номер': equipment.get('Гос. номер', ''),
                'Инв. №': equipment.get('Инв. №', ''),
                'Счётчик': equipment.get('Счётчик', ''),
                'Показания': reading.get('value'),
                'Комментарий': reading.get('comment', '')
            })
        
        # Проверяем, что есть хотя бы одно показание
        if not any(d['Показания'] is not None for d in data):
            logger.error("Все показания пустые")
            update.message.reply_text("❌ Нет действительных показаний для сохранения.")
            return ConversationHandler.END
        
        # Создаем DataFrame
        columns = ['№ п/п', 'Гос. номер', 'Инв. №', 'Счётчик', 'Показания', 'Комментарий']
        df = pd.DataFrame(data, columns=columns)
        
        # Добавляем метаданные пользователя
        df['name'] = name
        df['location'] = location
        df['division'] = division
        df['tab_number'] = tab_number
        df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Создаем папку для отчетов
        current_week = datetime.now().strftime('%Y-W%U')
        report_folder = f'meter_readings/week_{current_week}'
        os.makedirs(report_folder, exist_ok=True)
        
        # Сохраняем файл
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_path = os.path.join(report_folder, 
                               f'meters_{location}_{division}_{tab_number}_{timestamp}.xlsx')
        
        # Сохраняем в Excel
        df.to_excel(file_path, index=False, columns=columns + ['name', 'location', 'division', 'tab_number', 'timestamp'])
        
        # Валидация файла
        validator = MeterValidator()
        save_result = validator.save_to_final_report(df)
        
        if save_result.get('status') != 'success':
            error_msg = save_result.get('message', 'Неизвестная ошибка при сохранении')
            update.message.reply_text(f"❌ Ошибка: {error_msg}")
            return ConversationHandler.END
            
        update.message.reply_text(
            "✅ Показания успешно сохранены и добавлены в отчет. Спасибо!"
        )
        
        # Очищаем данные показаний
        for key in ['readings', 'readings_data', 'readings_user']:
            if key in context.user_data:
                del context.user_data[key]
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Ошибка завершения ввода: {str(e)}", exc_info=True)
        update.message.reply_text(
            "❌ Произошла ошибка при сохранении показаний. Пожалуйста, попробуйте позже."
        )
        return ConversationHandler.END

def show_equipment_for_input(update: Update, context: CallbackContext, index: int):
    equipment = context.user_data['equipment'][index]
    context.user_data['current_equipment_index'] = index
    
    # Получаем последние показания
    from check import MeterValidator
    validator = MeterValidator()
    last_reading = validator._get_last_reading(equipment['Инв. №'], equipment['Счётчик'])
    
    last_reading_text = ""
    if last_reading:
        last_reading_text = f"\nПоследнее показание: {last_reading['reading']} ({last_reading['reading_date']})"
    
    # Создаем клавиатуру
    keyboard = [
        [InlineKeyboardButton("Пропустить", callback_data=f"skip_{index}")],
        [
            InlineKeyboardButton("Неисправен", callback_data=f"faulty_{index}"),
            InlineKeyboardButton("В ремонте", callback_data=f"repair_{index}")
        ],
        [InlineKeyboardButton("Назад к списку", callback_data="back_to_list")]
    ]
    
    if query := update.callback_query:
        query.edit_message_text(
            f"Введите показания для:\n"
            f"Гос. номер: {equipment['Гос. номер']}\n"
            f"Инв. №: {equipment['Инв. №']}\n"
            f"Счётчик: {equipment['Счётчик']}{last_reading_text}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        update.message.reply_text(
            f"Введите показания для:\n"
            f"Гос. номер: {equipment['Гос. номер']}\n"
            f"Инв. №: {equipment['Инв. №']}\n"
            f"Счётчик: {equipment['Счётчик']}{last_reading_text}",
            reply_markup=InlineKeyboardMarkup(keyboard))
    
    return ENTER_READING_VALUE

# Обработчик выбора оборудования для ввода показаний
def select_equipment_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    
    if query.data == "finish_readings":
        # Проверяем, что есть хотя бы одно введенное показание
        if not context.user_data.get('readings_data'):
            query.edit_message_text("Вы не ввели ни одного показания. Процесс отменен.")
            return ConversationHandler.END
            
        # Переходим к подтверждению и отправке показаний
        return confirm_readings(update, context)
    
    # Получаем индекс выбранного оборудования
    equip_index = int(query.data.split('_')[1])
    equipment = context.user_data['equipment'][equip_index]
    
    # Сохраняем текущий выбор в контексте
    context.user_data['current_equipment'] = equipment
    context.user_data['current_equip_index'] = equip_index
    
    # Получаем последнее показание для этого счетчика
    from check import MeterValidator
    validator = MeterValidator()
    last_reading = validator._get_last_reading(equipment['Инв. №'], equipment['Счётчик'])
    
    last_reading_info = ""
    if last_reading:
        last_reading_info = f"\n\nПоследнее показание: {last_reading['reading']} ({last_reading['reading_date']})"
    
    # Создаем опции для ввода показаний
    keyboard = [
        [InlineKeyboardButton("Ввести показание", callback_data="enter_value")],
        [
            InlineKeyboardButton("Неисправен", callback_data="comment_Неисправен"),
            InlineKeyboardButton("В ремонте", callback_data="comment_В ремонте")
        ],
        [
            InlineKeyboardButton("Убыло", callback_data="comment_Убыло"),
            InlineKeyboardButton("« Назад", callback_data="back_to_list")
        ]
    ]
    
    query.edit_message_text(
        f"Оборудование:\n"
        f"Гос. номер: {equipment['Гос. номер']}\n"
        f"Инв. №: {equipment['Инв. №']}\n"
        f"Счётчик: {equipment['Счётчик']}{last_reading_info}\n\n"
        f"Выберите действие:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return ENTER_VALUE


def process_reading_input(update: Update, context: CallbackContext):
    try:
        user_type = 'user'
        if context.user_data.get('is_admin_submit'):
            user_type = 'admin'
        elif context.user_data.get('is_manager_submit'):
            user_type = 'manager'

        current_index = context.user_data.get(f'current_index_{user_type}', 0)
        equipment_list = context.user_data.get(f'equipment_{user_type}', [])
        
        # Проверяем, что оборудование закончилось
        if current_index >= len(equipment_list):
            return finish_manual_input(update, context)
            
        equipment = equipment_list[current_index]
        value = float(update.message.text)
        
        # Базовая проверка на отрицательные значения
        if value < 0:
            update.message.reply_text("Ошибка: показание не может быть отрицательным.")
            return ENTER_READING_VALUE
        
        validator = MeterValidator()
        last_reading = validator._get_last_reading(equipment['Инв. №'], equipment['Счётчик'])
        
        # Проверяем только если есть предыдущее показание
        if last_reading and last_reading['reading'] is not None:
            if value < last_reading['reading']:
                update.message.reply_text(
                    f"Ошибка: новое показание ({value}) меньше предыдущего ({last_reading['reading']}).\n"
                    "Введите корректное значение"
                )
                return ENTER_READING_VALUE
        
        # Сохраняем показание
        context.user_data.setdefault(f'readings_{user_type}', []).append({
            'equipment': equipment,
            'value': value,
            'comment': ''
        })
        
        context.user_data[f'current_index_{user_type}'] = current_index + 1
        
        # Проверяем, закончилось ли оборудование
        if context.user_data[f'current_index_{user_type}'] >= len(equipment_list):
            return finish_manual_input(update, context)
        else:
            return show_next_equipment(update, context, user_type)
            
    except ValueError:
        update.message.reply_text("Пожалуйста, введите числовое значение:")
        return ENTER_READING_VALUE
    except Exception as e:
        logger.error(f"Ошибка в process_reading_input: {e}")
        update.message.reply_text("❌ Произошла ошибка. Пожалуйста, попробуйте снова.")
        return ConversationHandler.END
    

def handle_reading_button(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    
    # Определяем тип пользователя
    query = update.callback_query
    query.answer()

    user_type = 'user'
    if context.user_data.get('is_admin_submit'):
        user_type = 'admin'
    elif context.user_data.get('is_manager_submit'):
        user_type = 'manager'

    current_index = context.user_data.get(f'current_index_{user_type}', 0)
    equipment_list = context.user_data.get(f'equipment_{user_type}', [])
    
    # Add bounds checking
    if not equipment_list or current_index >= len(equipment_list):
        logger.error(f"НЕ найдено оборудование (index: {current_index}, счет: {len(equipment_list)})")
        query.edit_message_text("Ошибка: оборудование не найдено.")
        return ConversationHandler.END
        
    equipment = equipment_list[current_index]
        
    equipment = equipment_list[current_index]
    validator = MeterValidator()
    
    if query.data.startswith(f'repair_{user_type}'):
        # Для "В ремонте" последнее показание
        last_reading = validator._get_last_reading(equipment['Инв. №'], equipment['Счётчик'])
        
        if last_reading:
            context.user_data.setdefault(f'readings_{user_type}', []).append({
                'equipment': equipment,
                'value': last_reading['reading'],
                'comment': 'В ремонте'
            })
            message = f"✅ Оборудование {equipment['Инв. №']} ({equipment['Счётчик']}) отмечено как 'В ремонте'"
        else:
            context.user_data.setdefault(f'readings_{user_type}', []).append({
                'equipment': equipment,
                'value': None,
                'comment': 'В ремонте'
            })
            message = f"✅ Оборудование {equipment['Инв. №']} ({equipment['Счётчик']}) отмечено как 'В ремонте'"
        
    elif query.data.startswith(f'ubylo_{user_type}'):
        # Обработка "Убыло"
        result = validator.handle_ubylo_status(
            context,
            equipment['Инв. №'],
            equipment['Счётчик'],
            {
                'tab_number': context.user_data['tab_number'],
                'name': context.user_data['name'],
                'chat_id': query.message.chat_id,
                'location': context.user_data.get('location', ''),
                'division': context.user_data.get('division', '')
            }
        )
        
        if result and result.get('status') == 'pending':
            context.user_data.setdefault('pending_ubylo', []).append({
                'equipment': equipment,
                'request_id': result['request_id']
            })
            message = "✅ Запрос на 'Убыло' отправлен"
            
            # Добавляем запись с комментарием "Убыло"
            context.user_data.setdefault(f'readings_{user_type}', []).append({
                'equipment': equipment,
                'value': None,
                'comment': 'Убыло'
            })
        else:
            error_msg = result.get('message', 'Неизвестная ошибка')
            query.edit_message_text(f"❌ Ошибка: {error_msg}")
            return ENTER_READING_VALUE
    
    elif query.data.startswith(f'skip_{user_type}'):
        # Пропуск оборудования
        context.user_data.setdefault(f'readings_{user_type}', []).append({
            'equipment': equipment,
            'value': None,
            'comment': 'Пропущено'
        })
        message = f"⏭ Оборудование {equipment['Инв. №']} пропущено"

    # Переход к следующему оборудованию
    context.user_data[f'current_index_{user_type}'] = current_index + 1
    
    if context.user_data[f'current_index_{user_type}'] < len(equipment_list):
        query.edit_message_text(f"{message}\n\nПереход к следующему оборудованию...")
        return show_next_equipment(update, context, user_type)
    else:
        query.edit_message_text(f"{message}\n\nВсе оборудование обработано.")
        if user_type == 'admin':
            return finish_admin_readings(update, context)
        elif user_type == 'manager':
            return finish_manager_readings(update, context)
        else:
            return finish_manual_input(update, context)

# Обработчик ввода значения или комментария
def enter_value_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    
    if not query:  # Если это текстовое сообщение (а не нажатие кнопки)
        try:
            value = float(update.message.text)
            if value < 0:
                update.message.reply_text("Показание не может быть отрицательным. Пожалуйста, введите положительное число.")
                return ENTER_VALUE
                
            # Сохраняем введенное значение
            equipment = context.user_data['current_equipment']
            equip_index = context.user_data['current_equip_index']
            
            # Проверяем, что значение не меньше предыдущего
            from check import MeterValidator
            validator = MeterValidator()
            last_reading = validator._get_last_reading(equipment['Инв. №'], equipment['Счётчик'])
            
            if last_reading and value < last_reading['reading']:
                update.message.reply_text(
                    f"Ошибка: введенное показание ({value}) меньше предыдущего ({last_reading['reading']}). "
                    f"Пожалуйста, введите корректное значение."
                )
                return ENTER_VALUE
            
            # Проверки по типу счетчика
            if last_reading:
                days_between = validator._get_days_between(last_reading['reading_date'])
                if days_between > 0:
                    daily_change = (value - last_reading['reading']) / days_between
                    
                    if equipment['Счётчик'].startswith('PM') and daily_change > 24:
                        update.message.reply_text(
                            f"Предупреждение: Слишком большое изменение для счетчика PM ({daily_change:.2f} в сутки). "
                            f"Максимально допустимое изменение: 24 в сутки."
                        )
                        
                    if equipment['Счётчик'].startswith('KM') and daily_change > 500:
                        update.message.reply_text(
                            f"Предупреждение: Слишком большое изменение для счетчика KM ({daily_change:.2f} в сутки). "
                            f"Максимально допустимое изменение: 500 в сутки."
                        )
            
            context.user_data['readings_data'][equip_index] = {
                'value': value,
                'comment': '',
                'equipment': equipment
            }
            
            # Возвращаемся к списку оборудования
            equipment_keyboard = []
            for i, equip in enumerate(context.user_data['equipment']):
                # Отмечаем оборудование, для которого уже введены данные
                prefix = "✅ " if i in context.user_data['readings_data'] else ""
                
                label = f"{prefix}{equip['Гос. номер']} | {equip['Инв. №']} | {equip['Счётчик']}"
                if len(label) > 30:
                    label = label[:27] + "..."
                    
                equipment_keyboard.append([
                    InlineKeyboardButton(label, callback_data=f"equip_{i}")
                ])
            
            equipment_keyboard.append([InlineKeyboardButton("🔄 Завершить и отправить", callback_data="finish_readings")])
            
            update.message.reply_text(
                f"Показание {value} для {equipment['Инв. №']} ({equipment['Счётчик']}) сохранено.\n\n"
                f"Выберите следующее оборудование или завершите ввод:",
                reply_markup=InlineKeyboardMarkup(equipment_keyboard)
            )
            return SELECT_EQUIPMENT
            
        except ValueError:
            update.message.reply_text("Пожалуйста, введите числовое значение.")
            return ENTER_VALUE
    else:
        query.answer()
        
        if query.data == "back_to_list":
            # Возвращаемся к списку оборудования
            equipment_keyboard = []
            for i, equip in enumerate(context.user_data['equipment']):
                # Отмечаем оборудование, для которого уже введены данные
                prefix = "✅ " if i in context.user_data['readings_data'] else ""
                
                label = f"{prefix}{equip['Гос. номер']} | {equip['Инв. №']} | {equip['Счётчик']}"
                if len(label) > 30:
                    label = label[:27] + "..."
                    
                equipment_keyboard.append([
                    InlineKeyboardButton(label, callback_data=f"equip_{i}")
                ])
            
            equipment_keyboard.append([InlineKeyboardButton("🔄 Завершить и отправить", callback_data="finish_readings")])
            
            query.edit_message_text(
                "Выберите оборудование для ввода показаний:",
                reply_markup=InlineKeyboardMarkup(equipment_keyboard)
            )
            return SELECT_EQUIPMENT
        elif query.data == "enter_value":
            # Запрашиваем ввод числового значения
            query.edit_message_text(
                f"Оборудование: {context.user_data['current_equipment']['Инв. №']} ({context.user_data['current_equipment']['Счётчик']})\n\n"
                f"Введите числовое значение показания:"
            )
            return ENTER_VALUE
        elif query.data.startswith("comment_"):
            # Сохраняем комментарий без значения показания
            comment = query.data.split('_', 1)[1]
            equipment = context.user_data['current_equipment']
            equip_index = context.user_data['current_equip_index']
            
            # Если выбран "В ремонте", автоматически подставляем последнее показание
            value = None
            auto_value_message = ""
            
            if comment == "В ремонте":
                from check import MeterValidator
                validator = MeterValidator()
                last_reading = validator._get_last_reading(equipment['Инв. №'], equipment['Счётчик'])
                
                if last_reading:
                    value = last_reading['reading']
                    auto_value_message = f" (автоматически использовано последнее показание: {value})"
            
            context.user_data['readings_data'][equip_index] = {
                'value': value,
                'comment': comment,
                'equipment': equipment
            }
            
            # Возвращаемся к списку оборудования
            equipment_keyboard = []
            for i, equip in enumerate(context.user_data['equipment']):
                # Отмечаем оборудование, для которого уже введены данные
                prefix = "✅ " if i in context.user_data['readings_data'] else ""
                
                label = f"{prefix}{equip['Гос. номер']} | {equip['Инв. №']} | {equip['Счётчик']}"
                if len(label) > 30:
                    label = label[:27] + "..."
                    
                equipment_keyboard.append([
                    InlineKeyboardButton(label, callback_data=f"equip_{i}")
                ])
            
            equipment_keyboard.append([InlineKeyboardButton("🔄 Завершить и отправить", callback_data="finish_readings")])
            
            query.edit_message_text(
                f"Комментарий '{comment}' для {equipment['Инв. №']} ({equipment['Счётчик']}) сохранен{auto_value_message}.\n\n"
                f"Выберите следующее оборудование или завершите ввод:",
                reply_markup=InlineKeyboardMarkup(equipment_keyboard)
            )
            return SELECT_EQUIPMENT

# Подтверждение и отправка показаний
def confirm_readings(update: Update, context: CallbackContext):
    query = update.callback_query
    if query:
        query.answer()
    
    # Формируем данные для отображения и сохранения
    readings_data = context.user_data.get('readings_data', {})
    
    if not readings_data:
        if query:
            query.edit_message_text("Нет данных для отправки. Процесс отменен.")
        else:
            update.message.reply_text("Нет данных для отправки. Процесс отменен.")
        return ConversationHandler.END
    
    # Формируем таблицу показаний
    df = pd.DataFrame(columns=['№ п/п', 'Гос. номер', 'Инв. №', 'Счётчик', 'Показания', 'Комментарий'])
    
    row_index = 1
    for equip_index, data in readings_data.items():
        equipment = data['equipment']
        df.loc[row_index] = [
            row_index,
            equipment['Гос. номер'],
            equipment['Инв. №'],
            equipment['Счётчик'],
            data['value'] if data['value'] is not None else '',
            data['comment']
        ]
        row_index += 1
    
    # Получаем данные пользователя
    tab_number = context.user_data.get('tab_number')
    with db_transaction() as cursor:
        
        cursor.execute('''
            SELECT name, location, division FROM Users_user_bot 
            WHERE tab_number = ?
        ''', (tab_number,))
    user_data = cursor.fetchone()
    name, location, division = user_data
    
    # Создаем директорию для отчетов, если не существует
    os.makedirs('meter_readings', exist_ok=True)
    
    # Создаем папку для отчетов текущей недели, если не существует
    current_week = datetime.now().strftime('%Y-W%U')  # Год-Номер недели
    report_folder = f'meter_readings/week_{current_week}'
    os.makedirs(report_folder, exist_ok=True)
    
    # Формируем имя файла
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_path = f'{report_folder}/meters_{location}_{division}_{tab_number}_{timestamp}.xlsx'
    
    # Добавляем метаданные
    user_info = {
        'name': name,
        'location': location,
        'division': division,
        'tab_number': tab_number,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    for key, value in user_info.items():
        df[key] = value
    
    # Сохраняем файл
    df.to_excel(file_path, index=False)
    
    # Валидируем созданный файл
    validator = MeterValidator()
    validation_result = validator.validate_file(file_path, user_info)
    
    if not validation_result['is_valid']:
        errors_text = "\n".join(validation_result['errors'])
        error_message = f"Ошибки при проверке введенных показаний:\n\n{errors_text}\n\nПожалуйста, исправьте и попробуйте снова."
        
        if query:
            query.edit_message_text(error_message)
        else:
            update.message.reply_text(error_message)
        
        # Удаляем файл с ошибками
        try:
            os.remove(file_path)
        except:
            pass
        
        return ConversationHandler.END
    
    # Уведомляем пользователя об успешной отправке
    moscow_tz = pytz.timezone('Europe/Moscow')
    moscow_now = datetime.now(moscow_tz)
    moscow_time_str = moscow_now.strftime('%H:%M %d.%m.%Y')
    
    # Проверяем, является ли день пятницей (4) и время до 14:00
    is_on_time = moscow_now.weekday() == 4 and moscow_now.hour < 14
    
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
    
    if query:
        query.edit_message_text(message_text)
    else:
        update.message.reply_text(message_text)
    
    # Уведомляем администраторов и руководителей
    from meters_handler import notify_admins_and_managers
    notify_admins_and_managers(context, tab_number, name, location, division, file_path)
    
    # Удаляем пользователя из списка тех, кому отправлено напоминание
    if 'missing_reports' in context.bot_data and tab_number in context.bot_data['missing_reports']:
        del context.bot_data['missing_reports'][tab_number]
        logger.info(f"Пользователь {name} удален из списка неотправивших отчеты")
    
    # Очищаем данные показаний
    if 'readings_data' in context.user_data:
        del context.user_data['readings_data']
    
    return ConversationHandler.END

def handle_ubylo_confirmation(update: Update, context: CallbackContext):
    """Подтверждение запроса 'Убыло' с обновлением файла пользователя"""
    query = update.callback_query
    query.answer()
    
    request_id = query.data.replace('confirm_ubylo_', '')
    
    try:
        # Use db_transaction context manager instead of raw cursor
        with db_transaction() as cursor:
            # 1. Получаем данные запроса
            cursor.execute('''
                SELECT inv_num, meter_type, user_tab, user_name, location, division, user_chat_id 
                FROM pending_requests 
                WHERE request_id = ? AND status = 'pending'
            ''', (request_id,))
            request_data = cursor.fetchone()
            
            if not request_data:
                query.edit_message_text("❌ Запрос не найден или уже обработан")
                return
                
            inv_num, meter_type, user_tab, user_name, location, division, user_chat_id = request_data
            
            # 2. Находим последний файл пользователя
            current_week = datetime.now().strftime('%Y-W%U')
            report_folder = f'meter_readings/week_{current_week}'
            import glob
            user_files = glob.glob(f'{report_folder}/*_{location}_{division}_{user_tab}_*.xlsx')
            
            if not user_files:
                logger.error(f"Файлы пользователя {user_name} не найдены")
                query.edit_message_text("❌ Файл показаний пользователя не найден")
                return
                
            # Берем самый свежий файл
            latest_file = max(user_files, key=os.path.getctime)
            
            # 3. Читаем файл и находим нужную строку
            df = pd.read_excel(latest_file)
            
            # Нормализуем данные для сравнения
            df['Инв. №'] = df['Инв. №'].astype(str).str.strip()
            search_inv_num = str(inv_num).strip()
            search_meter_type = str(meter_type).strip().upper()
            
            mask = (
                (df['Инв. №'] == search_inv_num) & 
                (df['Счётчик'].str.strip().str.upper() == search_meter_type)
            )
            
            if not df[mask].empty:
                # 4. Обновляем данные в файле
                df.loc[mask, 'Показания'] = None
                df.loc[mask, 'Комментарий'] = 'Убыло (подтверждено)'
                
                # Сохраняем обновленный файл
                df.to_excel(latest_file, index=False)
                
                # 5. Обновляем БД
                cursor.execute('''
                    UPDATE pending_requests
                    SET status = 'confirmed', 
                        processed_by = ?,
                        processed_at = ?
                    WHERE request_id = ?
                ''', (query.from_user.id, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), request_id))
                
                # 6. Сохраняем в final_report только после подтверждения
                validator = MeterValidator()
                save_result = validator.save_to_final_report(df)
                
                if save_result.get('status') != 'success':
                    error_msg = save_result.get('message', 'Неизвестная ошибка')
                    logger.error(f"Ошибка сохранения в final_report: {error_msg}")
                
                # 7. Уведомляем пользователя
                try:
                    context.bot.send_message(
                        chat_id=user_chat_id,
                        text=f"✅ Статус 'Убыло' подтверждён для:\n"
                             f"Инв. №: {inv_num}\n"
                             f"Счётчик: {meter_type}\n\n"
                             f"Файл показаний обновлен."
                    )
                except Exception as e:
                    logger.error(f"Ошибка уведомления пользователя: {e}")
                
                query.edit_message_text(
                    f"✅ Подтверждено 'Убыло' для:\n"
                    f"Инв. №: {inv_num}\n"
                    f"Счётчик: {meter_type}\n"
                    f"Пользователь: {user_name}"
                )
            else:
                logger.error(f"Не найдена строка: Инв.№ {inv_num}, Счетчик {meter_type}")
                logger.info(f"Содержимое файла:\n{df[['Инв. №', 'Счётчик']].to_string()}")
                
                query.edit_message_text(
                    f"❌ Оборудование не найдено в файле пользователя:\n"
                    f"Инв. №: {inv_num}\n"
                    f"Счётчик: {meter_type}\n\n"
                    f"Пользователь мог изменить файл после отправки запроса."
                )
                
    except Exception as e:
        logger.error(f"Ошибка подтверждения 'Убыло': {e}")
        query.edit_message_text("❌ Ошибка при обработке запроса")

def handle_ubylo_rejection(update: Update, context: CallbackContext):
    """Отклонение запроса 'Убыло' с уведомлением пользователя"""
    query = update.callback_query
    query.answer()
    
    request_id = query.data.replace('reject_ubylo_', '')
    
    try:
        with db_transaction() as cursor:
            # Получаем данные запроса
            cursor.execute('''
                SELECT inv_num, meter_type, user_tab, user_name, location, division, user_chat_id 
                FROM pending_requests 
                WHERE request_id = ? AND status = 'pending'
            ''', (request_id,))
            request_data = cursor.fetchone()
            
            if not request_data:
                query.edit_message_text("❌ Запрос не найден или уже обработан")
                return
                
            inv_num, meter_type, user_tab, user_name, location, division, user_chat_id = request_data
            
            # Обновляем статус запроса
            cursor.execute('''
                UPDATE pending_requests
                SET status = 'rejected', 
                    processed_by = ?,
                    processed_at = ?
                WHERE request_id = ? AND status = 'pending'
            ''', (query.from_user.id, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), request_id))
            
            # Уведомляем пользователя
            try:
                context.bot.send_message(
                    chat_id=user_chat_id,
                    text=f"❌ Ваш запрос на отметку 'Убыло' отклонён:\n"
                         f"Инв. №: {inv_num}\n"
                         f"Счётчик: {meter_type}\n\n"
                         f"Пожалуйста, отправьте показания заново без статуса 'Убыло'."
                )
            except Exception as e:
                logger.error(f"Ошибка уведомления пользователя {user_chat_id}: {e}")
            
            query.edit_message_text(
                f"❌ Запрос 'Убыло' отклонён:\n"
                f"Инв. №: {inv_num}\n"
                f"Счётчик: {meter_type}\n"
                f"Пользователь: {user_name}"
            )
            
    except Exception as e:
        logger.error(f"Ошибка отклонения 'Убыло': {e}")
        query.edit_message_text("❌ Ошибка при обработке запроса")


def handle_admin_view(update: Update, context: CallbackContext):
    """Обработка команды просмотра показаний для администратора"""
    if not check_access(update, context):
        return
    
    role = context.user_data.get('role')
    if role != 'Администратор':
        update.message.reply_text("Эта команда доступна только администраторам.")
        return
    
    tab_number = context.user_data.get('tab_number')
    
    # Получаем информацию о подразделении администратора
    with db_transaction() as cursor:
        
        cursor.execute('''
            SELECT location, division FROM Users_admin_bot WHERE tab_number = ?
        ''', (tab_number,))
    admin_info = cursor.fetchone()
    
    if not admin_info:
        update.message.reply_text("Ошибка: администратор не найден.")
        return
    
    location, division = admin_info
    
    # Получаем текущую неделю
    current_week = datetime.now().strftime('%Y-W%U')
    report_folder = f'meter_readings/week_{current_week}'
    
    if not os.path.exists(report_folder):
        update.message.reply_text("За эту неделю еще нет показаний.")
        return
    
    # Собираем все файлы для данного подразделения
    reports = []
    for filename in os.listdir(report_folder):
        if f"_{location}_{division}_" in filename:
            reports.append(filename)
    
    if not reports:
        update.message.reply_text(f"Нет показаний для вашего подразделения ({location}, {division}) за эту неделю.")
        return
    
    # Создаем сводный отчет
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for report in reports:
            df = pd.read_excel(os.path.join(report_folder, report))
            df.to_excel(writer, sheet_name=report[:30], index=False)
    
    output.seek(0)
    update.message.reply_document(
        document=InputFile(output, filename=f'Показания_{location}_{division}_{current_week}.xlsx'),
        caption=f"Показания за неделю {current_week} (локация: {location}, подразделение: {division})"
    )


def handle_view_readings(update: Update, context: CallbackContext):
    if not check_access(update, context):
        return
    
    tab_number = context.user_data.get('tab_number')
    with db_transaction() as cursor:
        
        cursor.execute('SELECT location, division FROM Users_admin_bot WHERE tab_number = ?', (tab_number,))
    admin_info = cursor.fetchone()
    
    if not admin_info:
        update.message.reply_text("Ошибка: администратор не найден.")
        return
    
    location, division = admin_info
    
    # Получаем текущую неделю
    current_week = datetime.now().strftime('%Y-W%U')
    report_folder = f'meter_readings/week_{current_week}'
    
    if not os.path.exists(report_folder):
        update.message.reply_text("За эту неделю еще нет показаний.")
        return
    
    # Собираем все файлы для данного подразделения
    reports = []
    for filename in os.listdir(report_folder):
        if f"_{location}_{division}_" in filename:
            reports.append(filename)
    
    if not reports:
        update.message.reply_text(f"Нет показаний для вашего подразделения ({location}, {division}) за эту неделю.")
        return
    
    # Создаем сводный отчет
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for report in reports:
            df = pd.read_excel(f"{report_folder}/{report}")
            df.to_excel(writer, sheet_name=report[:30], index=False)
    
    output.seek(0)
    update.message.reply_document(
        document=InputFile(output, filename=f'Показания_{location}_{division}_{current_week}.xlsx'),
        caption=f"Показания за неделю {current_week} (локация: {location}, подразделение: {division})"
    )


def get_available_users_by_role(role):
    """Получает список доступных пользователей по роли"""
    with sqlite3.cursorect('Users_bot.db') as cursor:
        
        if role == 'Администратор':
            cursor.execute('SELECT name, chat_id FROM Users_admin_bot')
        elif role == 'Руководитель':
            cursor.execute('SELECT name, chat_id FROM Users_dir_bot')
        else:
            cursor.execute('SELECT name, chat_id FROM Users_user_bot')
        return cursor.fetchall()

def handle_disagree_with_errors(update: Update, context: CallbackContext):
    """Обработка нажатия кнопки 'Я не согласен с ошибками'"""
    query = update.callback_query
    query.answer()
    
    if 'validation_result' not in context.user_data or 'file_path' not in context.user_data:
        query.edit_message_text("Ошибка: данные валидации не найдены.")
        return ConversationHandler.END
    
    validation_result = context.user_data['validation_result']
    file_path = context.user_data['file_path']
    
    if not file_path or not os.path.exists(file_path):
        query.edit_message_text("Ошибка: файл с показаниями не найден.")
        return ConversationHandler.END
    
    # Получаем информацию о пользователе
    user_info = {
        'tab_number': context.user_data['tab_number'],
        'name': context.user_data['name'],
        'location': context.user_data['location'],
        'division': context.user_data['division'],
        'chat_id': query.message.chat_id
    }
    
    # Уведомляем администратора
    notify_admin_about_disagreement(context, user_info, file_path, validation_result['errors'])
    
    query.edit_message_text(
        "✅ Ваше несогласие с ошибками отправлено администратору. "
        "Он проверит и при необходимости отправит показания."
    )
    return ConversationHandler.END

def notify_admin_about_disagreement(context: CallbackContext, user_info: dict, file_path: str, errors: list):
    """Уведомление администратора о несогласии пользователя с ошибками"""
    try:
        from check import MeterValidator
        validator = MeterValidator()
        
        # Получаем администраторов с реальными chat_id
        admins = validator._get_admins_for_division(user_info.get('division', ''))
        
        if not admins:
            logger.error(f"Не найдены администраторы для подразделения {user_info.get('division', '')}")
            return
            
        errors_text = "\n".join(errors)
        
        for admin_tab, admin_name, admin_chat_id in admins:
            try:
                if not admin_chat_id:
                    logger.warning(f"Администратор {admin_name} не имеет chat_id")
                    continue
                
                keyboard = [
                    [InlineKeyboardButton("Отправить показания за пользователя", 
                                     callback_data=f"admin_submit_{user_info['tab_number']}")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                # Отправляем сообщение администратору
                context.bot.send_message(
                    chat_id=admin_chat_id,
                    text=f"⚠️ Пользователь не согласен с ошибками\n\n"
                         f"👤 Пользователь: {user_info['name']}\n"
                         f"📍 Локация: {user_info['location']}\n"
                         f"🏢 Подразделение: {user_info['division']}\n\n"
                         f"Обнаруженные ошибки:\n{errors_text}\n\n"
                         f"Вы можете отправить показания за пользователя:",
                    reply_markup=reply_markup
                )
                
                # Проверяем существование файла перед отправкой
                if os.path.exists(file_path):
                    # Отправляем файл администратору
                    with open(file_path, 'rb') as f:
                        context.bot.send_document(
                            chat_id=admin_chat_id,
                            document=f,
                            caption=f"Файл с показаниями от {user_info['name']}"
                        )
                else:
                    logger.error(f"Файл не найден: {file_path}")
                    context.bot.send_message(
                        chat_id=admin_chat_id,
                        text=f"⚠️ Файл с показаниями не найден или был удалён."
                    )
                    
                logger.info(f"Уведомление отправлено администратору {admin_name} (chat_id: {admin_chat_id})")
                
            except Exception as e:
                logger.error(f"Ошибка уведомления администратора {admin_name}: {e}")
    except Exception as e:
        logger.error(f"Ошибка уведомления администратора о несогласии: {e}")


def handle_admin_correct(update: Update, context: CallbackContext):
    """Обработка нажатия кнопки 'Исправить и отправить' администратором"""
    query = update.callback_query
    query.answer()
    
    # Получаем tab_number пользователя из callback_data
    tab_number = int(query.data.split('_')[2])
    
    # Сохраняем информацию о том, что администратор исправляет показания
    context.user_data['admin_correcting'] = True
    context.user_data['user_tab_number'] = tab_number
    
    # Получаем информацию о пользователе
    with db_transaction() as cursor:
        
        cursor.execute('''
            SELECT name, location, division FROM Users_user_bot WHERE tab_number = ?
        ''', (tab_number,))
    user_data = cursor.fetchone()
    
    if not user_data:
        query.edit_message_text("Ошибка: пользователь не найден.")
        return
        
    name, location, division = user_data
    
    # Отправляем сообщение администратору с инструкциями
    query.edit_message_text(
        f"Вы исправляете показания для пользователя:\n"
        f"👤 {name}\n"
        f"📍 {location}\n"
        f"🏢 {division}\n\n"
        f"Пожалуйста, отправьте исправленный файл Excel с показаниями."
    )
    
    return WAIT_EXCEL_FILE

def handle_admin_submit(update: Update, context: CallbackContext):
    """Обработка нажатия кнопки администратора 'Отправить показания за пользователя'"""
    query = update.callback_query
    query.answer()
    
    # Получаем tab_number пользователя из callback_data
    user_tab = int(query.data.split('_')[2])
    
    # Получаем информацию о пользователе
    with db_transaction() as cursor:
        cursor.execute('''
            SELECT name, location, division FROM Users_user_bot WHERE tab_number = ?
        ''', (user_tab,))
        user_data = cursor.fetchone()
    
    if not user_data:
        query.edit_message_text("Ошибка: пользователь не найден.")
        return
        
    name, location, division = user_data

    # Ищем последний файл, отправленный пользователем
    current_week = datetime.now().strftime('%Y-W%U')
    report_folder = f'meter_readings/week_{current_week}'
    import glob
    user_files = glob.glob(f'{report_folder}/*_{location}_{division}_{user_tab}_*.xlsx')
    
    if not user_files:
        query.edit_message_text("Пользователь еще не отправлял показания.")
        return
    
    # Берем самый свежий файл
    latest_file = max(user_files, key=os.path.getctime)
    
    try:
        # Отправляем файл администратору
        with open(latest_file, 'rb') as f:
            context.bot.send_document(
                chat_id=query.message.chat_id,
                document=InputFile(f, filename=f'Показания_{name}.xlsx'),
                caption=f"Файл показаний пользователя {name}"
            )
        
        # Сохраняем данные пользователя в контексте
        context.user_data.update({
            'admin_submit': True,
            'user_tab': user_tab,
            'user_name': name,
            'user_location': location,
            'user_division': division,
            'file_path': latest_file  # Сохраняем путь к файлу для дальнейшей обработки
        })
        
        # Сразу переходим к ожиданию исправленного файла
        return WAIT_ADMIN_EXCEL
        
    except Exception as e:
        logger.error(f"Ошибка отправки файла администратору: {e}")
        query.edit_message_text("Ошибка при получении файла пользователя.")
        return ConversationHandler.END

def handle_admin_excel_file(update: Update, context: CallbackContext):
    """Обработка Excel файла от администратора"""
    try:
        if not context.user_data.get('admin_submit'):
            return ConversationHandler.END

        if not update.message.document:
            update.message.reply_text("Пожалуйста, отправьте файл Excel.")
            return WAIT_ADMIN_EXCEL

        file = update.message.document
        file_id = file.file_id
        new_file = context.bot.get_file(file_id)

        # Получаем данные пользователя из контекста
        user_tab = context.user_data.get('user_tab')
        if not user_tab:
            update.message.reply_text("Ошибка: не удалось определить пользователя.")
            return ConversationHandler.END

        # Создаем папку для отчетов
        current_week = datetime.now().strftime('%Y-W%U')
        report_folder = f'meter_readings/week_{current_week}'
        os.makedirs(report_folder, exist_ok=True)

        # Сохраняем файл с пометкой, что отправлено администратором
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_path = os.path.join(
            report_folder,
            f'meters_admin_{user_tab}_{timestamp}.xlsx'
        )
        new_file.download(file_path)

        # Валидация и сохранение файла
        validator = MeterValidator()
        save_result = validator.save_to_final_report(file_path, user_tab)
        
        if save_result.get('status') != 'success':
            error_msg = save_result.get('message', 'Неизвестная ошибка')
            update.message.reply_text(f"❌ Ошибка сохранения: {error_msg}")
            return

        # Уведомляем пользователя
        try:
            with db_transaction() as cursor:
                cursor.execute('''
                    SELECT name, chat_id FROM Users_user_bot WHERE tab_number = ?
                ''', (user_tab,))
                user_data = cursor.fetchone()
                
                if user_data:
                    user_name, user_chat_id = user_data
                    context.bot.send_message(
                        chat_id=user_chat_id,
                        text=f"✅ Администратор отправил показания за вас"
                    )
        except Exception as e:
            logger.error(f"Ошибка уведомления пользователя: {e}")

        update.message.reply_text("✅ Показания успешно сохранены")
        
        # Очищаем данные
        context.user_data.clear()

        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Ошибка обработки файла администратора: {e}")
        update.message.reply_text(
            "❌ Произошла ошибка при обработке файла. Пожалуйста, попробуйте снова."
        )
        return WAIT_ADMIN_EXCEL


def update_admin_chat_ids(context: CallbackContext):
    """Обновление chat_id администраторов в базе данных"""
    try:
        with db_transaction() as cursor:
            
            cursor.execute('SELECT tab_number, name FROM Users_admin_bot')
        admins = cursor.fetchall()
        
        updated_count = 0
        for admin_tab, admin_name in admins:
            try:
                chat = context.bot.get_chat(admin_tab)
                cursor.execute('''
                    UPDATE Users_admin_bot 
                    SET chat_id = ? 
                    WHERE tab_number = ? AND (chat_id IS NULL OR chat_id != ?)
                ''', (chat.id, admin_tab, chat.id))
                if cursor.rowcount > 0:
                    updated_count += 1
                    logger.info(f"Обновлен chat_id для администратора {admin_name}")
            except Exception as e:
                logger.error(f"Ошибка обновления chat_id для администратора {admin_name}: {e}")
        logger.info(f"Обновлено chat_id для {updated_count} администраторов")
    except Exception as e:
        logger.error(f"Ошибка при массовом обновлении chat_id администраторов: {e}")


def handle_admin_action(update: Update, context: CallbackContext):
    """Обработка действий администратора по несогласию пользователя"""
    query = update.callback_query
    query.answer()
    
    try:
        # Разбираем callback_data: admin_[action]_[request_id]
        parts = query.data.split('_')
        if len(parts) < 3:
            raise ValueError("Неверный формат callback_data")
            
        action = parts[1]
        request_id = '_'.join(parts[2:])
        
        # Получаем данные запроса
        request_data = context.bot_data.get(request_id)
        if not request_data or request_data.get('type') != 'admin_submit':
            query.edit_message_text("Ошибка: запрос не найден или устарел.")
            return ConversationHandler.END
        
        # Сохраняем данные в user_data администратора
        context.user_data.update({
            'admin_action': True,
            'request_id': request_id,
            'user_tab': request_data['user_tab'],
            'user_name': request_data['user_name'],
            'user_location': request_data['user_location'],
            'user_division': request_data['user_division'],
            'user_chat_id': request_data['user_chat_id'],
            'original_file_path': request_data['original_file_path']
        })
        
        if action == 'manual':
            # Начинаем ручной ввод показаний
            from check import MeterValidator
            validator = MeterValidator()
            equipment = validator._get_equipment_for_location_division(
                request_data['user_location'], 
                request_data['user_division']
            )
            
            if equipment.empty:
                query.edit_message_text(f"Для локации {request_data['user_location']} и подразделения {request_data['user_division']} нет оборудования.")
                return
                
            context.user_data['equipment'] = equipment.to_dict('records')
            context.user_data['current_index'] = 0
            context.user_data['readings'] = []
            
            return show_next_equipment(update, context)
            
        elif action == 'excel':
            # Запрашиваем Excel файл
            query.edit_message_text(
                f"Пожалуйста, загрузите исправленный Excel файл с показаниями для пользователя:\n"
                f"👤 {request_data['user_name']}\n"
                f"📍 {request_data['user_location']}\n"
                f"🏢 {request_data['user_division']}"
            )
            return WAIT_ADMIN_EXCEL
            
        elif action == 'reject':
            # Отклоняем несогласие
            try:
                context.bot.send_message(
                    chat_id=request_data['user_chat_id'],
                    text=f"❌ Администратор отклонил ваше несогласие с ошибками.\n\n"
                         f"Пожалуйста, проверьте данные и отправьте показания заново."
                )
                query.edit_message_text(
                    f"Вы отклонили несогласие пользователя {request_data['user_name']}.\n"
                    f"Пользователь уведомлен о необходимости проверить данные."
                )
                
                # Удаляем данные запроса
                context.bot_data.pop(request_id, None)
            except Exception as e:
                logger.error(f"Ошибка уведомления пользователя: {e}")
                query.edit_message_text("Ошибка при отправке уведомления пользователю.")
                
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Ошибка обработки действия администратора: {e}")
        query.edit_message_text("Произошла ошибка. Пожалуйста, попробуйте снова.")
        return ConversationHandler.END

def handle_admin_reading_input(update: Update, context: CallbackContext):
    try:
        if not context.user_data.get('is_admin_submit'):
            return ConversationHandler.END

        value = float(update.message.text)
        current_index = context.user_data.get('current_index_admin', 0)
        equipment_list = context.user_data.get('equipment_admin', [])
        
        if current_index >= len(equipment_list):
            logger.error(f"Admin reading input: index {current_index} out of range (max {len(equipment_list)})")
            update.message.reply_text("Ошибка: не найдено оборудование для ввода показаний.")
            return ConversationHandler.END
            
        equipment = equipment_list[current_index]
        
        if value < 0:
            update.message.reply_text("Ошибка: показание не может быть отрицательным.")
            return ENTER_ADMIN_READING
        
        validator = MeterValidator()
        last_reading = validator._get_last_reading(equipment['Инв. №'], equipment['Счётчик'])
        
        if last_reading and value < last_reading['reading']:
            update.message.reply_text(f"Ошибка: новое показание меньше предыдущего ({last_reading['reading']}).")
            return ENTER_ADMIN_READING
        
        context.user_data.setdefault('readings_admin', []).append({
            'equipment': equipment,
            'value': value,
            'comment': ''
        })
        
        context.user_data['current_index_admin'] = current_index + 1
        
        if context.user_data['current_index_admin'] < len(equipment_list):
            return show_next_equipment(update, context, 'admin')
        else:
            return finish_admin_readings(update, context)
            
    except ValueError:
        update.message.reply_text("Пожалуйста, введите числовое значение.")
        return ENTER_ADMIN_READING
    except Exception as e:
        logger.error(f"Ошибка в handle_admin_reading_input: {e}")
        update.message.reply_text("❌ Произошла ошибка. Пожалуйста, попробуйте снова.")
        return ConversationHandler.END
    

def handle_admin_reading_button(update: Update, context: CallbackContext):
    """Обработка кнопок при вводе показаний администратором"""
    query = update.callback_query
    query.answer()

    current_index = context.user_data['current_index']
    equipment = context.user_data['equipment'][current_index]
    validator = MeterValidator()

    if query.data == 'repair':
        # Для "В ремонте" используем последнее показание
        last_reading = validator._get_last_reading(equipment['Инв. №'], equipment['Счётчик'])
        
        if last_reading:
            context.user_data['readings'].append({
                'equipment': equipment,
                'value': last_reading['reading'],
                'comment': 'В ремонте'
            })
            message = f"✅ Оборудование {equipment['Инв. №']} отмечено как 'В ремонте'"
        else:
            context.user_data['readings'].append({
                'equipment': equipment,
                'value': None,
                'comment': 'В ремонте'
            })
            message = f"✅ Оборудование {equipment['Инв. №']} отмечено как 'В ремонте' (нет предыдущих показаний)"

    elif query.data == 'ubylo':
        # Обработка "Убыло"
        result = validator.handle_ubylo_status(
            context,
            equipment['Инв. №'],
            equipment['Счётчик'],
            {
                'tab_number': context.user_data['user_tab'],
                'name': context.user_data['user_name'],
                'chat_id': query.message.chat_id,
                'location': context.user_data['user_location'],
                'division': context.user_data['user_division']
            }
        )
        
        if result and result.get('status') == 'pending':
            context.user_data.setdefault('pending_ubylo', []).append({
                'equipment': equipment,
                'request_id': result['request_id']
            })
            message = "✅ Запрос на 'Убыло' отправлен"
            
            # Добавляем запись с комментарием "Убыло"
            context.user_data['readings'].append({
                'equipment': equipment,
                'value': None,
                'comment': 'Убыло'
            })
        else:
            error_msg = result.get('message', 'Неизвестная ошибка')
            query.edit_message_text(f"❌ Ошибка: {error_msg}")
            return ENTER_ADMIN_READING

    elif query.data == 'skip':
        # Пропуск оборудования
        context.user_data['readings'].append({
            'equipment': equipment,
            'value': None,
            'comment': 'Пропущено'
        })
        message = f"⏭ Оборудование {equipment['Инв. №']} пропущено"

    # Переход к следующему оборудованию
    context.user_data['current_index'] += 1
    if context.user_data['current_index'] < len(context.user_data['equipment']):
        query.edit_message_text(f"{message}\n\nПереход к следующему оборудованию...")
        return show_next_equipment(update, context)
    else:
        query.edit_message_text(f"{message}\n\nВсе оборудование обработано.")
        return finish_admin_readings(update, context)


def finish_admin_readings(update: Update, context: CallbackContext):
    try:
        if not context.user_data.get('admin_action'):
            return ConversationHandler.END

        # Get admin info
        admin_tab = update.effective_user.id
        with db_transaction() as cursor:
            
            cursor.execute('SELECT name FROM Users_admin_bot WHERE tab_number = ?', (admin_tab,))
        admin_name = cursor.fetchone()[0] if cursor.fetchone() else "Администратор"

        # Get user info from context
        user_tab = context.user_data['user_tab']
        user_name = context.user_data['user_name']
        location = context.user_data['user_location']
        division = context.user_data['user_division']

        # Create report folder
        current_week = datetime.now().strftime('%Y-W%U')
        report_folder = f'meter_readings/week_{current_week}'
        os.makedirs(report_folder, exist_ok=True)

        # Prepare filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'meters_{location}_{division}_{user_tab}_admin_{timestamp}.xlsx'
        file_path = os.path.join(report_folder, filename)

        # Create DataFrame from readings
        readings = context.user_data.get('readings_admin', [])
        if not readings:
            raise ValueError("No readings data found")

        data = []
        for reading in readings:
            equipment = reading['equipment']
            data.append({
                '№ п/п': len(data) + 1,
                'Гос. номер': equipment['Гос. номер'],
                'Инв. №': equipment['Инв. №'],
                'Счётчик': equipment['Счётчик'],
                'Показания': reading['value'],
                'Комментарий': reading['comment']
            })

        df = pd.DataFrame(data)
        
        # Add metadata
        df['user_name'] = user_name
        df['location'] = location
        df['division'] = division
        df['tab_number'] = user_tab
        df['submitted_by'] = admin_name
        df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Save file
        df.to_excel(file_path, index=False)

        # Validate file
        validator = MeterValidator()
        validation_result = validator.validate_file(file_path, {
            'name': user_name,
            'location': location,
            'division': division,
            'tab_number': user_tab
        })

        if not validation_result['is_valid']:
            errors = "\n".join(validation_result['errors'])
            update.message.reply_text(f"Ошибки при проверке:\n{errors}")
            os.remove(file_path)
            return

        # Notify user
        try:
            context.bot.send_message(
                chat_id=user_tab,
                text=f"✅ Администратор {admin_name} отправил показания за вас\n"
                     f"📍 Локация: {location}\n"
                     f"🏢 Подразделение: {division}"
            )
        except Exception as e:
            logger.error(f"Error notifying user: {e}")

        # Confirm to admin
        update.message.reply_text(
            f"✅ Показания успешно сохранены за пользователя {user_name}"
        )

        # Clean up
        for key in ['admin_action', 'user_tab', 'user_name', 'user_location', 
                   'user_division', 'equipment_admin', 'current_index_admin', 
                   'readings_admin']:
            if key in context.user_data:
                del context.user_data[key]

        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Error in finish_admin_readings: {e}")
        update.message.reply_text("❌ Ошибка при сохранении показаний")
        return ConversationHandler.END

def handle_view_week_report(update: Update, context: CallbackContext):
    """Обработка запроса на просмотр показаний за неделю"""
    if not check_access(update, context):
        return
        
    role = context.user_data.get('role')
    if role not in ['Администратор', 'Руководитель']:
        update.message.reply_text("Эта команда доступна только администраторам и руководителям.")
        return
        
    try:
        # Получаем данные из final_report
        with db_transaction() as cursor:
            cursor.execute('''
                SELECT 
                    gov_number, inv_number, meter_type, reading, comment,
                    name, date, division, location, sender
                FROM final_report
                WHERE date >= date('now', 'weekday 0', '-5 days')
                ORDER BY date DESC
            ''')
            report_data = cursor.fetchall()
            
        if not report_data:
            update.message.reply_text("За эту неделю нет данных в отчете.")
            return
            
        # Создаем DataFrame
        columns = [
            'Гос. номер', 'Инв. №', 'Счётчик', 'Показания', 'Комментарий',
            'Наименование', 'Дата', 'Подразделение', 'Локация', 'Отправитель'
        ]
        df = pd.DataFrame(report_data, columns=columns)
        
        # Сохраняем в Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Показания')
        output.seek(0)
        
        # Формируем имя файла
        current_week = datetime.now().strftime('%Y-W%U')
        filename = f'final_report_{current_week}.xlsx'
        
        # Отправляем пользователю
        update.message.reply_document(
            document=InputFile(output, filename=filename),
            caption=f"Финальный отчет за неделю {current_week}"
        )
        
    except Exception as e:
        logger.error(f"Ошибка формирования отчета: {e}")
        update.message.reply_text("Произошла ошибка при формировании отчета.")

def get_accessible_reports(location: str, division: str, role: str) -> list:
    """Возвращает список доступных отчетов"""
    current_week = datetime.now().strftime('%Y-W%U')
    report_folder = f'meter_readings/week_{current_week}'
    
    if not os.path.exists(report_folder):
        return []
        
    accessible = []
    for filename in os.listdir(report_folder):
        parts = filename.split('_')
        if len(parts) < 4:
            continue
            
        file_loc = parts[1]
        file_div = parts[2]
        
        if role == 'Администратор' and file_loc == location and file_div == division:
            accessible.append(filename)
        elif role == 'Руководитель' and file_loc == location:
            accessible.append(filename)
    
    return accessible

def handle_manager_submit(update: Update, context: CallbackContext):
    """Обработка отправки показаний руководителем за пользователя"""
    query = update.callback_query
    query.answer()
    
    # Разбираем callback_data: manager_submit_<tab_number>
    tab_number = int(query.data.split('_')[2])
    
    # Получаем информацию о пользователе
    with db_transaction() as cursor:
        
        cursor.execute('''
            SELECT name, location, division, chat_id FROM Users_user_bot WHERE tab_number = ?
        ''', (tab_number,))
    user_data = cursor.fetchone()
    
    if not user_data:
        query.edit_message_text("Ошибка: пользователь не найден.")
        return ConversationHandler.END
        
    name, location, division, user_chat_id = user_data
    
    # Сохраняем данные в контексте
    context.user_data.update({
        'user_tab_number': tab_number,
        'user_name': name,
        'user_location': location,
        'user_division': division,
        'user_chat_id': user_chat_id,
        'is_manager_submit': True
    })
    
    # Предлагаем руководителю выбрать способ ввода
    keyboard = [
        [InlineKeyboardButton("Загрузить Excel файл", callback_data='manager_upload_excel')],
        [InlineKeyboardButton("Ввести показания вручную", callback_data='manager_enter_readings')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(
        f"Вы отправляете показания за пользователя:\n"
        f"👤 {name}\n"
        f"📍 {location}\n"
        f"🏢 {division}\n\n"
        f"Выберите способ ввода показаний:",
        reply_markup=reply_markup
    )
    
    return WAITING_FOR_MANAGER_CHOICE

def handle_manager_excel_file(update: Update, context: CallbackContext):
    """Обработка Excel файла от руководителя"""
    try:
        if not context.user_data.get('is_manager_submit'):
            return ConversationHandler.END

        if not update.message.document:
            update.message.reply_text("Пожалуйста, отправьте файл Excel.")
            return WAIT_MANAGER_EXCEL

        file = update.message.document
        file_id = file.file_id
        new_file = context.bot.get_file(file_id)

        # Получаем данные пользователя
        user_tab = context.user_data['user_tab_number']
        user_name = context.user_data['user_name']
        location = context.user_data['user_location']
        division = context.user_data['user_division']
        user_chat_id = context.user_data['user_chat_id']

        # Создаем папку для отчетов
        current_week = datetime.now().strftime('%Y-W%U')
        report_folder = f'meter_readings/week_{current_week}'
        os.makedirs(report_folder, exist_ok=True)

        # Сохраняем файл с пометкой, что отправлено руководителем
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_path = os.path.join(
            report_folder,
            f'meters_{location}_{division}_{user_tab}_manager_{timestamp}.xlsx'
        )
        new_file.download(file_path)

        # Валидация файла
        validator = MeterValidator()
        validation_result = validator.validate_file(file_path, {
            'name': user_name,
            'location': location,
            'division': division,
            'tab_number': user_tab
        })

        if not validation_result['is_valid']:
            errors = "\n".join(validation_result['errors'])
            update.message.reply_text(
                f"Ошибки в файле:\n{errors}\n\n"
                "Пожалуйста, исправьте и отправьте файл снова."
            )
            os.remove(file_path)
            return WAIT_MANAGER_EXCEL

        # Уведомляем пользователя
        try:
            context.bot.send_message(
                chat_id=user_chat_id,
                text=f"✅ Руководитель отправил показания за вас:\n\n"
                     f"📍 Локация: {location}\n"
                     f"🏢 Подразделение: {division}\n\n"
                     f"Показания успешно приняты."
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления пользователя: {e}")

        update.message.reply_text(
            f"✅ Файл с показаниями за пользователя {user_name} успешно сохранен."
        )

        # Очищаем данные
        context.user_data.clear()

        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Ошибка обработки файла руководителя: {e}")
        update.message.reply_text(
            "❌ Произошла ошибка при обработке файла. Пожалуйста, попробуйте снова."
        )
        return WAIT_MANAGER_EXCEL
    
def finish_manager_readings(update: Update, context: CallbackContext):
    """Завершение ввода показаний руководителем за пользователя"""
    try:
        # Получаем данные пользователя
        user_tab = context.user_data['user_tab_number']
        user_name = context.user_data['user_name']
        location = context.user_data['user_location']
        division = context.user_data['user_division']
        user_chat_id = context.user_data['user_chat_id']
        
        # Создаем DataFrame из введенных данных
        data = []
        for reading in context.user_data.get('readings_manager', []):
            equipment = reading['equipment']
            data.append({
                '№ п/п': len(data) + 1,
                'Гос. номер': equipment['Гос. номер'],
                'Инв. №': equipment['Инв. №'],
                'Счётчик': equipment['Счётчик'],
                'Показания': reading['value'],
                'Комментарий': reading['comment']
            })

        df = pd.DataFrame(data)

        # Добавляем метаданные
        df['name'] = user_name
        df['location'] = location
        df['division'] = division
        df['tab_number'] = user_tab
        df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['submitted_by_manager'] = update.effective_user.id  # ID руководителя

        # Создаем папку для отчетов
        current_week = datetime.now().strftime('%Y-W%U')
        report_folder = f'meter_readings/week_{current_week}'
        os.makedirs(report_folder, exist_ok=True)

        # Сохраняем файл
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_path = os.path.join(
            report_folder,
            f'meters_{location}_{division}_{user_tab}_manager_{timestamp}.xlsx'
        )

        df.to_excel(file_path, index=False)

        # Валидация файла
        validator = MeterValidator()
        validation_result = validator.validate_file(file_path, {
            'name': user_name,
            'location': location,
            'division': division,
            'tab_number': user_tab
        })

        if not validation_result['is_valid']:
            errors = "\n".join(validation_result['errors'])
            update.message.reply_text(
                f"Ошибки при проверке показаний:\n{errors}\n\n"
                "Пожалуйста, попробуйте снова."
            )
            os.remove(file_path)
            return ConversationHandler.END

        # Уведомляем пользователя
        try:
            context.bot.send_message(
                chat_id=user_chat_id,
                text=f"✅ Руководитель отправил показания за вас:\n\n"
                     f"📍 Локация: {location}\n"
                     f"🏢 Подразделение: {division}\n\n"
                     f"Показания успешно приняты."
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления пользователя: {e}")

        update.message.reply_text(
            f"✅ Показания успешно сохранены за пользователя {user_name}."
        )

        # Очищаем данные
        context.user_data.clear()

        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Ошибка завершения ввода руководителем: {e}")
        update.message.reply_text(
            "❌ Произошла ошибка при сохранении показаний. Пожалуйста, попробуйте позже."
        )
        return ConversationHandler.END
    
def schedule_cleanup_jobs(context: CallbackContext):
    """Планирование заданий по очистке старых данных"""
    try:
        # Очистка старых запросов каждые 5 дней в 00:00
        context.job_queue.run_daily(
            callback=cleanup_old_requests,
            time=time(hour=0, minute=0),
            days=(0, 1, 2, 3, 4, 5, 6),
            name="daily_cleanup_requests"
        )
    except Exception as e:
        logger.error(f"Ошибка планирования заданий очистки: {e}")

def cleanup_old_requests(context: CallbackContext):
    """Очистка запросов старше 5 дней"""
    try:
        cursor = sqlite3.cursorect('Users_bot.db', check_same_thread=False)
        
        
        cursor.execute('''
            DELETE FROM pending_requests 
            WHERE timestamp < datetime('now', '-5 days')
        ''')
        
        deleted_count = cursor.rowcount
        logger.info(f"Удалено {deleted_count} старых запросов")
        
    except Exception as e:
        logger.error(f"Ошибка очистки старых запросов: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
    
    
def main():
    # Инициализация бота
    updater = Updater(token=os.getenv('BOT_TOKEN'), use_context=True)
    dp = updater.dispatcher
    job_queue = updater.job_queue
    
    logger.info("Бот запущен")
    logger.info("Зарегистрирован обработчик команды /start")
    
    # Настройка ежедневного обновления в 8:00 по Москве
    moscow_tz = pytz.timezone('Europe/Moscow')
    
    # Ежедневное обновление данных и отправка уведомлений
    job_queue.run_once(check_admin_chat_ids, when=5)
    job_queue.run_daily(
        daily_update, 
        time=time(hour=8, minute=0, tzinfo=moscow_tz),
        days=(0, 1, 2, 3, 4, 5, 6)
    )
    logger.info("Настроено ежедневное обновление")

    job_queue.run_daily(
        update_admin_chat_ids,
        time=time(hour=8, minute=0, tzinfo=moscow_tz),
        days=(0, 1, 2, 3, 4, 5, 6),
        name="daily_admin_chat_id_update"
    )

    job_queue.run_daily(
        update_admin_chat_ids,
        time=time(hour=8, minute=0, tzinfo=moscow_tz),
        days=(0, 1, 2, 3, 4, 5, 6),
        name="daily_admin_chat_id_update"
    )
    
    # Обработчик ввода табельного номера
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('start', start),
            MessageHandler(Filters.regex('^В начало$'), return_to_start)
        ],
        states={
            ENTER_TAB_NUMBER: [
                MessageHandler(
                    Filters.text & ~Filters.command & ~Filters.regex('^В начало$') & ~Filters.regex('^Отмена$'),
                    handle_tab_number,
                    run_async=True
                )
            ]
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            MessageHandler(Filters.regex('^Отмена$'), cancel),
            MessageHandler(Filters.command, lambda u, c: u.message.reply_text("Пожалуйста, сначала введите табельный номер или нажмите /cancel для отмены."))
        ],
        per_chat=True,
        per_message=False,
        allow_reentry=True,
        name="main_conversation",
        persistent=False
    )

    dp.add_handler(ConversationHandler(
    entry_points=[
        CallbackQueryHandler(handle_admin_submit, pattern='^admin_submit_')
    ],
    states={
        ENTER_ADMIN_READING: [
            MessageHandler(Filters.text & ~Filters.command, handle_admin_reading_input),
            CallbackQueryHandler(handle_reading_button, pattern='^(repair_admin|skip_admin|ubylo_admin)$')
        ],
    },
    fallbacks=[
        CommandHandler('cancel', cancel),
        MessageHandler(Filters.regex('^Отмена$'), cancel)
    ],
    per_user=True
    ))
    
    dp.add_handler(conv_handler)
    logger.info("Зарегистрирован обработчик диалога ввода табельного номера")
    
    dp.add_handler(MessageHandler(Filters.regex('^(В начало)$'), handle_button))
    dp.add_handler(MessageHandler(Filters.regex('^Загрузить показания$'), handle_upload_readings))
    dp.add_handler(MessageHandler(Filters.regex('^(Посмотреть показания за эту неделю)$'), handle_button))

    dp.add_error_handler(error_handler)
    dp.add_handler(CallbackQueryHandler(handle_admin_action, pattern='^admin_(manual|excel|reject)_'))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_admin_reading_input), group=ENTER_ADMIN_READING)
    dp.add_handler(CallbackQueryHandler(handle_admin_reading_button, pattern='^(repair|ubylo|skip)$'), group=ENTER_ADMIN_READING)
    dp.add_handler(CallbackQueryHandler(readings_choice_handler, pattern='^(upload_excel|enter_readings)$'))
    dp.add_handler(CallbackQueryHandler(handle_admin_action, pattern='^admin_(manual|excel|reject)_'))

    dp.add_handler(CommandHandler('back', back_to_choice))
    dp.add_handler(CommandHandler('view_readings', handle_admin_view))
    dp.add_handler(CallbackQueryHandler(handle_reading_button, pattern='^(repair|ubylo|skip)$'))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, process_reading_input))
    dp.add_handler(MessageHandler(Filters.regex('^Загрузить показания$'), handle_upload_readings))
    dp.add_handler(CommandHandler('view_week', handle_view_week_report))

    dp.add_handler(CallbackQueryHandler(
        handle_disagree_with_errors,
        pattern='^disagree_with_errors$'
    ))
    
    dp.add_handler(CallbackQueryHandler(
        handle_admin_submit,
        pattern='^admin_submit_'
    ))

    dp.add_handler(ConversationHandler(
        entry_points=[
            CallbackQueryHandler(handle_admin_submit, pattern='^admin_submit_')
        ],
        states={
            WAITING_FOR_ADMIN_CHOICE: [
                CallbackQueryHandler(generate_excel_template, pattern='^admin_upload_excel'),
                CallbackQueryHandler(start_manual_input, pattern='^admin_enter_readings')
            ],
            WAIT_ADMIN_EXCEL: [
                MessageHandler(Filters.document.file_extension("xls") | Filters.document.file_extension("xlsx"), 
                handle_admin_excel_file)
            ],
            ENTER_ADMIN_READING: [
                MessageHandler(Filters.text & ~Filters.command, handle_admin_reading_input),
                CallbackQueryHandler(handle_reading_button, pattern='^(repair_admin|skip_admin|ubylo_admin)$')
            ]
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            MessageHandler(Filters.regex('^Отмена$'), cancel)
        ],
        per_user=True
    ))
    dp.add_handler(CallbackQueryHandler(
        handle_ubylo_confirmation,
        pattern='^confirm_ubylo_'
    ))
    dp.add_handler(CallbackQueryHandler(
        handle_ubylo_rejection,
        pattern='^reject_ubylo_'
    ))
    dp.add_handler(CallbackQueryHandler(
        handle_admin_correct,
        pattern='^admin_correct_'
    ))
    dp.add_handler(MessageHandler(
        Filters.document.file_extension("xls") | Filters.document.file_extension("xlsx"),
        handle_admin_excel_file), group=WAIT_EXCEL_FILE)
    
    # Обработчики для ввода показаний
    readings_conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(readings_choice_handler, pattern='^(upload_excel|enter_readings)$')
        ],
        states={
            WAITING_FOR_CHOICE: [
                CallbackQueryHandler(readings_choice_handler)
            ],
            ENTER_READING_VALUE: [
                MessageHandler(Filters.text & ~Filters.command, process_reading_input),
                CallbackQueryHandler(handle_reading_button)
            ],
            CONFIRM_READINGS: [
                CallbackQueryHandler(confirm_readings)
            ]
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            MessageHandler(Filters.regex('^Отмена$'), cancel)
        ],
        per_chat=True,
        per_user=True
    )

    dp.add_handler(CallbackQueryHandler(
        handle_manager_submit,
        pattern='^manager_submit_'
    ))

    manager_readings_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(handle_manager_submit, pattern='^manager_submit_')
        ],
        states={
            WAITING_FOR_MANAGER_CHOICE: [
                CallbackQueryHandler(generate_excel_template, pattern='^manager_upload_excel'),
                CallbackQueryHandler(start_manual_input, pattern='^manager_enter_readings')
            ],
            WAIT_MANAGER_EXCEL: [
                MessageHandler(Filters.document.file_extension("xls") | Filters.document.file_extension("xlsx"), 
                            handle_manager_excel_file)
            ]
        },
        fallbacks=[
            CommandHandler('cancel', cancel)
        ]
    )
    dp.add_handler(manager_readings_conv)
    dp.add_handler(readings_conv_handler)
    logger.info("Зарегистрирован обработчик ввода показаний")
    
    # Настройка обработчиков для работы с показаниями счетчиков
    from meters_handler import setup_meters_handlers
    setup_meters_handlers(dp)
    logger.info("Настроены обработчики для работы с показаниями счетчиков")
    
    # Настройка ежедневного обновления в 8:00 по Москве
    job_queue = updater.job_queue
    moscow_tz = pytz.timezone('Europe/Moscow')
    
    # Ежедневное обновление данных и отправка уведомлений
    job_queue.run_daily(
        daily_update, 
        time=time(hour=8, minute=0, tzinfo=moscow_tz),
        days=(0, 1, 2, 3, 4, 5, 6)
    )
    logger.info("Настроено ежедневное обновление")
    
    # Первоначальная загрузка данных из табеля
    shifts_handler.load_tabel()
    logger.info("Загружены данные из табеля")

    dp.add_handler(CallbackQueryHandler(
    handle_reading_button,
    pattern='^(repair|ubylo|finish|skip|faulty)'
))
    dp.add_handler(CallbackQueryHandler(
        handle_ubylo_rejection,
        pattern='^reject_ubylo_'
    ))
    
    # Запуск бота
    logger.info("Запуск бота...")
    updater.start_polling()
    logger.info("Бот успешно запущен и ожидает сообщений")
    updater.idle()

# Инициализация базы данных
def init_database():
    try:
        logger.info("Инициализация базы данных")
        with db_transaction() as cursor:
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Users_admin_bot (
                    tab_number INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    role TEXT DEFAULT 'Администратор',
                    chat_id INTEGER NOT NULL,
                    location TEXT,
                    division TEXT
                )''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Users_user_bot (
                    tab_number INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    role TEXT DEFAULT 'Пользователь',
                    chat_id INTEGER NOT NULL,
                    location TEXT,
                    division TEXT
                )''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Users_dir_bot (
                    tab_number INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    role TEXT DEFAULT 'Руководитель',
                    chat_id INTEGER NOT NULL,
                    location TEXT,
                    division TEXT
                )''')
            
            # Создаем таблицу для смен
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS shifts (
                    tab_number INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    is_on_shift TEXT DEFAULT 'НЕТ',
                    FOREIGN KEY (tab_number) REFERENCES Users_user_bot(tab_number)
                )
            ''')
            
            # Создаем таблицу для ежедневных статусов
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS daily_shifts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    employee_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    UNIQUE(date, employee_name)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS equipment (
                    inventory_number TEXT NOT NULL,
                    meter_type TEXT NOT NULL,
                    location TEXT,
                    division TEXT,
                    status TEXT DEFAULT 'active',
                    PRIMARY KEY (inventory_number, meter_type)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS final_report (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    gov_number TEXT NOT NULL,
                    inv_number TEXT NOT NULL,
                    meter_type TEXT NOT NULL,
                    reading REAL,
                    comment TEXT,
                    name TEXT NOT NULL,
                    date DATETIME NOT NULL,
                    division TEXT NOT NULL,
                    location TEXT NOT NULL,
                    sender TEXT NOT NULL,
                    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(gov_number, inv_number, meter_type, date)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS pending_requests (
                    request_id TEXT PRIMARY KEY,
                    inv_num TEXT NOT NULL,
                    meter_type TEXT NOT NULL,
                    user_tab INTEGER NOT NULL,
                    user_name TEXT NOT NULL,
                    location TEXT NOT NULL,
                    division TEXT NOT NULL,
                    timestamp DATETIME NOT NULL,
                    status TEXT DEFAULT 'pending',
                    processed_by INTEGER,
                    processed_at DATETIME,
                    user_chat_id INTEGER NOT NULL
                )
            ''')
        logger.info("База данных успешно инициализирована")
        
        # Выполняем миграцию, если необходимо
    except Exception as e:
        logger.error(f"Ошибка при инициализации базы данных: {e}")

# Вызываем инициализацию при запуске
init_database()

if __name__ == '__main__':
    main()