import io
import pandas as pd
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, ConversationHandler, CallbackContext, CallbackQueryHandler
import sqlite3
import pytz
from datetime import time, datetime
from shifts_handler import ShiftsHandler
import os
import logging
from dotenv import load_dotenv

# Загрузка переменных окружения из файла .env
load_dotenv()

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Состояния для ConversationHandler
ENTER_TAB_NUMBER, ENTER_READINGS, SELECT_EQUIPMENT, ENTER_VALUE, CONFIRM_READINGS = range(5)
CONTACT_MESSAGE = 7  # Новое состояние для ввода сообщения

# Инициализация обработчика табеля
shifts_handler = ShiftsHandler()

conn = sqlite3.connect('Users_bot.db', check_same_thread=False)
cursor = conn.cursor()

# Создание таблиц, если они не существуют
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

cursor.execute('''
CREATE TABLE IF NOT EXISTS shifts (
    tab_number INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    is_on_shift TEXT DEFAULT 'НЕТ',
    FOREIGN KEY (tab_number) REFERENCES Users_user_bot(tab_number)
)''')

conn.commit()

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
        
    user_id = update.effective_user.id
    logger.info(f"Получена команда /start от пользователя {user_id}")
    
    # Очищаем данные пользователя при новом старте
    context.user_data.clear()
    context.user_data['started'] = True  # Отмечаем, что приветствие было
    logger.info("Очищены предыдущие данные пользователя")

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

# Обработка введенного табельного номера
def handle_tab_number(update: Update, context: CallbackContext) -> int:
    try:
        tab_number = int(update.message.text)
        chat_id = update.effective_chat.id  # Get the Telegram chat ID
        
        # Проверяем существование табельного номера в Excel
        user = check_tab_number_exists_in_excel(tab_number)
        
        if user is not None:
                name = user['ФИО'].values[0]
                role = determine_role(user)
                location = user['Локация'].values[0]
                division = user['Подразделение'].values[0] if 'Подразделение' in user.columns else ""
                t_number = user['Номер телефона'].values[0] if 'Номер телефона' in user.columns else None
                
            # Добавляем пользователя в базу данных с chat_id
                add_user_to_db(tab_number, name, role, chat_id, location, division)
                
                # Сохраняем данные пользователя в контексте
                context.user_data.update({
                    'tab_number': tab_number,
                    'name': name,
                    'role': role,
                    'chat_id': chat_id,
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
        # Руководители всегда имеют доступ
        if role == 'Руководитель':
            return True
            
        # Получаем ФИО сотрудника
        cursor.execute('SELECT name FROM Users_user_bot WHERE tab_number = ?', (tab_number,))
        result = cursor.fetchone()
        
        if not result and role == 'Администратор':
            cursor.execute('SELECT name FROM Users_admin_bot WHERE tab_number = ?', (tab_number,))
            result = cursor.fetchone()
            if result:
                return shifts_handler.check_admin_status(result[0]) == "ДА"
            return False
            
        if not result:
            return False
            
        employee_name = result[0]
        # Проверяем текущий статус в табеле
        status = shifts_handler.check_employee_status(employee_name)
        
        if not status:
            return False
            
        # Проверяем различные статусы
        if status == 'О':  # Отпуск
            return False
        elif status == 'Б':  # Больничный
            return False
        elif status == 'НЕТ':  # Не на вахте
            return False
            
        return status == 'ДА'  # На вахте
    except Exception as e:
        logger.error(f"Ошибка проверки доступности: {e}")
        return False

def check_access(update: Update, context: CallbackContext) -> bool:
    # Проверка доступа перед выполнением команд
    if 'tab_number' not in context.user_data or 'role' not in context.user_data:
        update.message.reply_text("Пожалуйста, сначала введите ваш табельный номер через /start")
        return False
    
    tab_number = context.user_data['tab_number']
    role = context.user_data['role']
    
    if not is_user_available(tab_number, role):
        update.message.reply_text("⛔ В настоящее время бот недоступен для вас (вы не на смене или в отпуске)")
        return False
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
            ['Связаться с оператором'],
            ['В начало']
        ]
    elif role == 'Руководитель':
        keyboard = [
            ['Загрузить показания', 'Мой профиль'],
            ['Связаться с администратором'],
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
    elif text == 'Связаться с оператором':
        return start_contact_operator(update, context)

# Удаление пользователя из базы данных
def delete_user(tab_number, role):
    try:
        if role == 'Администратор':
            cursor.execute('DELETE FROM Users_admin_bot WHERE tab_number = ?', (tab_number,))
        elif role == 'Руководитель':
            cursor.execute('DELETE FROM Users_dir_bot WHERE tab_number = ?', (tab_number,))
        else:
            cursor.execute('DELETE FROM Users_user_bot WHERE tab_number = ?', (tab_number,))
        
        # Также удаляем из таблицы смен
        cursor.execute('DELETE FROM shifts WHERE tab_number = ?', (tab_number,))
        conn.commit()
        return True
    except Exception as e:
        print(f"Ошибка при удалении пользователя: {e}")
        return False

# Проверка, существует ли пользователь в базе данных
def is_user_in_db(tab_number, role):
    try:
        if role == 'Администратор':
            cursor.execute('SELECT * FROM Users_admin_bot WHERE tab_number = ?', (tab_number,))
        elif role == 'Руководитель':
            cursor.execute('SELECT * FROM Users_dir_bot WHERE tab_number = ?', (tab_number,))
        else:
            cursor.execute('SELECT * FROM Users_user_bot WHERE tab_number = ?', (tab_number,))
        
        return cursor.fetchone() is not None
    except Exception as e:
        print(f"Ошибка при проверке пользователя в БД: {e}")
        return False

# Добавление пользователя в соответствующую таблицу базы данных
def add_user_to_db(tab_number, name, role, chat_id, location, division):
    """Добавление пользователя в базу данных"""
    try:
        if role == 'Администратор':
            cursor.execute('''
                INSERT OR REPLACE INTO Users_admin_bot 
                (tab_number, name, role, chat_id, location, division) 
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (tab_number, name, role, chat_id, location, division))
        elif role == 'Руководитель':
            cursor.execute('''
                INSERT OR REPLACE INTO Users_dir_bot 
                (tab_number, name, role, chat_id, location, division) 
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (tab_number, name, role, chat_id, location, division))
        else:
            cursor.execute('''
                INSERT OR REPLACE INTO Users_user_bot 
                (tab_number, name, role, chat_id, location, division) 
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (tab_number, name, role, chat_id, location, division))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Ошибка добавления пользователя в БД: {e}")
        return False

def update_shifts_from_excel():
    try:
        df = load_shifts_table()
        if not df.empty:
            # Очистка таблицы перед обновлением
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
            
            conn.commit()
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
            
            conn.commit()
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

# Обработчик для кнопки "Загрузить показания"
def handle_upload_readings(update: Update, context: CallbackContext):
    if not check_access(update, context):
        return ConversationHandler.END
        
    tab_number = context.user_data.get('tab_number')
    
    # Получаем информацию о пользователе
    cursor.execute('''
        SELECT name, location, division FROM Users_user_bot 
        WHERE tab_number = ?
    ''', (tab_number,))
    user_data = cursor.fetchone()
    
    if not user_data:
        update.message.reply_text("Ошибка: пользователь не найден в базе данных.")
        return ConversationHandler.END
        
    name, location, division = user_data
    
    keyboard = [
        [InlineKeyboardButton("Загрузить Excel файл", callback_data='upload_excel')],
        [InlineKeyboardButton("Ввести показания вручную", callback_data='enter_readings')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        f"Выберите способ подачи показаний счетчиков:\n\n"
        f"📍 Локация: {location}\n"
        f"🏢 Подразделение: {division}",
        reply_markup=reply_markup
    )
    return ENTER_READINGS

# Обработчик выбора способа загрузки показаний
def readings_choice_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    
    if query.data == 'upload_excel':
        query.edit_message_text(
            "Пожалуйста, отправьте заполненный Excel файл с показаниями.\n\n"
            "Файл должен содержать столбцы:\n"
            "№ п/п, Гос. номер, Инв. №, Счётчик, Показания, Комментарий"
        )
        # Здесь не возвращаем следующее состояние, так как файл будет обрабатываться отдельным обработчиком
        return ConversationHandler.END
    elif query.data == 'enter_readings':
        # Получаем список оборудования для данного пользователя
        tab_number = context.user_data.get('tab_number')
        
        cursor.execute('''
            SELECT location, division FROM Users_user_bot 
            WHERE tab_number = ?
        ''', (tab_number,))
        user_location = cursor.fetchone()
        
        if not user_location:
            query.edit_message_text("Ошибка: не удалось получить информацию о пользователе")
            return ConversationHandler.END
            
        location, division = user_location
        
        # Получаем список оборудования для данной локации и подразделения
        try:
            from check import MeterValidator
            validator = MeterValidator()
            equipment_df = validator._get_equipment_for_location_division(location, division)
            
            if equipment_df.empty:
                query.edit_message_text(
                    f"На вашей локации ({location}, {division}) нет оборудования для ввода показаний. "
                    f"Обратитесь к администратору."
                )
                return ConversationHandler.END
            
            # Сохраняем список оборудования в контексте пользователя
            context.user_data['equipment'] = equipment_df.to_dict('records')
            
            # Создаем клавиатуру с оборудованием
            keyboard = []
            for index, row in equipment_df.iterrows():
                inv_num = row['Инв. №']
                meter_type = row['Счётчик']
                gos_number = row['Гос. номер'] if 'Гос. номер' in row else "N/A"
                
                # Ограничиваем длину для корректного отображения
                label = f"{gos_number} | {inv_num} | {meter_type}"
                if len(label) > 30:
                    label = label[:27] + "..."
                
                keyboard.append([
                    InlineKeyboardButton(
                        label, 
                        callback_data=f"equip_{index}"
                    )
                ])
            
            # Добавляем кнопку завершения
            keyboard.append([InlineKeyboardButton("🔄 Завершить и отправить", callback_data="finish_readings")])
            
            # Создаем таблицу для сбора показаний в контексте пользователя
            if 'readings_data' not in context.user_data:
                context.user_data['readings_data'] = {}
                
            query.edit_message_text(
                "Выберите оборудование для ввода показаний:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return SELECT_EQUIPMENT
            
        except Exception as e:
            logger.error(f"Ошибка при получении списка оборудования: {e}")
            query.edit_message_text(f"Ошибка при получении списка оборудования: {str(e)}")
            return ConversationHandler.END

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
    from check import MeterValidator
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

def handle_view_readings(update: Update, context: CallbackContext):
    if not check_access(update, context):
        return
    
    tab_number = context.user_data.get('tab_number')
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

# Обработчик для кнопки "Связаться с оператором"
def handle_contact_operator(update: Update, context: CallbackContext):
    if not check_access(update, context):
        return
    
    tab_number = context.user_data.get('tab_number')
    name = context.user_data.get('name')
    role = context.user_data.get('role')
    
    # Получаем список операторов
    cursor.execute('''
        SELECT u.tab_number, u.name, u.location, u.division 
        FROM Users_user_bot u
        JOIN shifts s ON u.tab_number = s.tab_number
        WHERE s.is_on_shift = "ДА"
    ''')
    operators = cursor.fetchall()
    
    if not operators:
        update.message.reply_text("В данный момент нет доступных операторов на смене.")
        return
    
    # Группируем операторов по локации и подразделению
    operators_by_location = {}
    for op_tab, op_name, op_location, op_division in operators:
        if op_location not in operators_by_location:
            operators_by_location[op_location] = {}
        
        if op_division not in operators_by_location[op_location]:
            operators_by_location[op_location][op_division] = []
        
        operators_by_location[op_location][op_division].append((op_tab, op_name))
    
    # Создаем клавиатуру для выбора локации
    keyboard = []
    for location in operators_by_location.keys():
        keyboard.append([InlineKeyboardButton(location, callback_data=f"select_location_{location}")])
    
    # Добавляем кнопку отмены
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="cancel_contact")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Сохраняем данные операторов для использования в следующих шагах
    context.user_data['operators_by_location'] = operators_by_location
    
    update.message.reply_text(
        "Выберите локацию оператора для связи:",
        reply_markup=reply_markup
    )

# Обработчик для выбора локации оператора
def handle_select_location(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    
    # Получаем выбранную локацию
    location = query.data.replace("select_location_", "")
    
    # Получаем подразделения для выбранной локации
    operators_by_location = context.user_data.get('operators_by_location', {})
    divisions = operators_by_location.get(location, {})
    
    if not divisions:
        query.edit_message_text("Для выбранной локации нет доступных операторов.")
        return
    
    # Создаем клавиатуру для выбора подразделения
    keyboard = []
    for division in divisions.keys():
        keyboard.append([InlineKeyboardButton(division, callback_data=f"select_division_{location}_{division}")])
    
    # Добавляем кнопку отмены
    keyboard.append([InlineKeyboardButton("Назад", callback_data="back_to_locations")])
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="cancel_contact")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(
        f"Выберите подразделение для локации {location}:",
        reply_markup=reply_markup
    )

# Обработчик для выбора подразделения оператора
def handle_select_division(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    
    # Получаем выбранную локацию и подразделение
    data_parts = query.data.replace("select_division_", "").split("_")
    location = data_parts[0]
    division = "_".join(data_parts[1:])  # На случай, если в названии подразделения есть символ _
    
    # Получаем операторов для выбранной локации и подразделения
    operators_by_location = context.user_data.get('operators_by_location', {})
    operators = operators_by_location.get(location, {}).get(division, [])
    
    if not operators:
        query.edit_message_text("Для выбранного подразделения нет доступных операторов.")
        return
    
    # Создаем клавиатуру для выбора оператора
    keyboard = []
    for op_tab, op_name in operators:
        keyboard.append([InlineKeyboardButton(op_name, callback_data=f"contact_operator_{op_tab}")])
    
    # Добавляем кнопку отмены
    keyboard.append([InlineKeyboardButton("Назад", callback_data=f"select_location_{location}")])
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="cancel_contact")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(
        f"Выберите оператора для связи (локация: {location}, подразделение: {division}):",
        reply_markup=reply_markup
    )

# Обработчик для выбора локации руководителя
def handle_select_mgr_location(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    
    # Получаем выбранную локацию
    location = query.data.replace("select_mgr_location_", "")
    
    # Получаем подразделения для выбранной локации
    managers_by_location = context.user_data.get('managers_by_location', {})
    divisions = managers_by_location.get(location, {})
    
    if not divisions:
        query.edit_message_text("Для выбранной локации нет доступных руководителей.")
        return
    
    # Создаем клавиатуру для выбора подразделения
    keyboard = []
    for division in divisions.keys():
        # Ensure division name is safe for callback_data by replacing spaces with underscores
        safe_division = division.replace(" ", "_")
        callback_data = f"select_mgr_division_{location}_{safe_division}"
        
        # Ensure callback_data doesn't exceed 64 bytes
        if len(callback_data.encode('utf-8')) > 64:
            # If too long, use a hash or shorter identifier
            callback_data = f"div_{hash(safe_division)}"
            
        keyboard.append([InlineKeyboardButton(division, callback_data=callback_data)])
    
    # Добавляем кнопку отмены
    keyboard.append([InlineKeyboardButton("Назад", callback_data="back_to_mgr_locations")])
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="cancel_contact")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(
        f"Выберите подразделение для локации {location}:",
        reply_markup=reply_markup
    )

# Обработчик для отправки сообщения оператору
def handle_contact_operator_selected(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    
    # Получаем ID выбранного оператора
    operator_tab = query.data.replace("contact_operator_", "")
    
    # Получаем информацию об операторе
    cursor.execute('SELECT name, chat_id FROM Users_user_bot WHERE tab_number = ?', (operator_tab,))
    operator_info = cursor.fetchone()
    
    if not operator_info:
        query.edit_message_text("Ошибка: оператор не найден.")
        return ConversationHandler.END
    
    operator_name, operator_chat_id = operator_info
    
    # Сохраняем данные оператора для последующей отправки сообщения
    context.user_data['contact_operator_name'] = operator_name
    context.user_data['contact_operator_chat_id'] = operator_chat_id
    
    query.edit_message_text(
        f"Вы выбрали оператора: {operator_name}\n\n"
        f"Введите ваше сообщение для отправки. Для отмены введите /cancel"
    )
    
    return CONTACT_MESSAGE

# Обработчик для отмены выбора контакта
def handle_cancel_contact(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    
    query.edit_message_text("Операция отменена.")
    
    # Очищаем данные контакта
    for key in ['contact_admin_tab', 'contact_admin_name', 'contact_operator_tab', 
               'contact_operator_name', 'contact_manager_tab', 'contact_manager_name',
               'waiting_for_message_to_admin', 'waiting_for_message_to_operator',
               'waiting_for_message_to_manager', 'operators_by_location', 'managers_by_location']:
        if key in context.user_data:
            del context.user_data[key]

# Обработчик для возврата к выбору локации
def handle_back_to_locations(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    
    # Получаем данные операторов
    operators_by_location = context.user_data.get('operators_by_location', {})
    
    # Создаем клавиатуру для выбора локации
    keyboard = []
    for location in operators_by_location.keys():
        keyboard.append([InlineKeyboardButton(location, callback_data=f"select_location_{location}")])
    
    # Добавляем кнопку отмены
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="cancel_contact")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(
        "Выберите локацию оператора для связи:",
        reply_markup=reply_markup
    )

# Обработчик для возврата к выбору локации руководителя
def handle_back_to_mgr_locations(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    
    # Получаем данные руководителей
    managers_by_location = context.user_data.get('managers_by_location', {})
    
    # Создаем клавиатуру для выбора локации
    keyboard = []
    for location in managers_by_location.keys():
        keyboard.append([InlineKeyboardButton(location, callback_data=f"select_mgr_location_{location}")])
    
    # Добавляем кнопку отмены
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="cancel_contact")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(
        "Выберите локацию руководителя для связи:",
        reply_markup=reply_markup
    )

# Обработчик для отправки сообщения
def handle_message_input(update: Update, context: CallbackContext):
    # Skip if we're in a conversation state
    if context.user_data.get('state') == ENTER_TAB_NUMBER:
        return
        
    # Проверяем, ожидается ли ввод сообщения
    if not any([context.user_data.get('waiting_for_message_to_admin'),
                context.user_data.get('waiting_for_message_to_operator'),
                context.user_data.get('waiting_for_message_to_manager')]):
        return
    
    # Получаем введенное сообщение
    message_text = update.message.text
    
    # Получаем информацию о пользователе
    tab_number = context.user_data.get('tab_number')
    name = context.user_data.get('name')
    role = context.user_data.get('role')
    location = context.user_data.get('location')
    division = context.user_data.get('division')
    
    # Определяем получателя сообщения
    recipient_tab = None
    recipient_name = None
    recipient_role = None
    
    if context.user_data.get('waiting_for_message_to_admin'):
        recipient_tab = context.user_data.get('contact_admin_tab')
        recipient_name = context.user_data.get('contact_admin_name')
        recipient_role = "Администратор"
    elif context.user_data.get('waiting_for_message_to_operator'):
        recipient_tab = context.user_data.get('contact_operator_tab')
        recipient_name = context.user_data.get('contact_operator_name')
        recipient_role = "Оператор"
    
    # Отправляем сообщение получателю
    try:
        # Формируем сообщение для получателя
        recipient_message = f"📨 *Новое сообщение*\n\n" \
                          f"От: {name} ({role})\n" \
                          f"Локация: {location}\n" \
                          f"Подразделение: {division}\n\n" \
                          f"Сообщение:\n{message_text}\n\n" \
                          f"Для ответа используйте кнопку 'Связаться с {role.lower()}' в вашем меню."
        
        # Отправляем сообщение получателю
        context.bot.send_message(
            chat_id=recipient_tab,
            text=recipient_message,
            parse_mode='Markdown'
        )
        
        # Отправляем подтверждение отправителю
        update.message.reply_text(
            f"✅ Ваше сообщение успешно отправлено {recipient_role.lower()}у {recipient_name}."
        )
        
        # Очищаем данные контакта
        for key in ['contact_admin_tab', 'contact_admin_name', 'contact_operator_tab', 
                   'contact_operator_name', 'contact_manager_tab', 'contact_manager_name',
                   'waiting_for_message_to_admin', 'waiting_for_message_to_operator',
                   'waiting_for_message_to_manager', 'operators_by_location', 'managers_by_location']:
            if key in context.user_data:
                del context.user_data[key]
                
    except Exception as e:
        logger.error(f"Ошибка отправки сообщения: {e}")
        update.message.reply_text(
            f"❌ Произошла ошибка при отправке сообщения. Пожалуйста, попробуйте позже."
        )

def get_available_users_by_role(role):
    """Получает список доступных пользователей по роли"""
    with sqlite3.connect('Users_bot.db') as conn:
        cursor = conn.cursor()
        if role == 'Администратор':
            cursor.execute('SELECT name, chat_id FROM Users_admin_bot')
        elif role == 'Руководитель':
            cursor.execute('SELECT name, chat_id FROM Users_dir_bot')
        else:
            cursor.execute('SELECT name, chat_id FROM Users_user_bot')
        return cursor.fetchall()

def create_user_selection_keyboard(users):
    """Создает клавиатуру с списком пользователей"""
    keyboard = []
    for name, _ in users:
        keyboard.append([InlineKeyboardButton(name, callback_data=f"user_{name}")])
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="cancel_contact")])
    return InlineKeyboardMarkup(keyboard)

def start_contact_operator(update: Update, context: CallbackContext):
    """Начинает процесс связи с оператором"""
    if not check_access(update, context):
        return ConversationHandler.END
    
    operators = get_available_users_by_role('Пользователь')
    if not operators:
        update.message.reply_text("К сожалению, сейчас нет доступных операторов.")
        return ConversationHandler.END
    
    context.user_data['contact_type'] = 'operator'
    keyboard = create_user_selection_keyboard(operators)
    update.message.reply_text("Выберите оператора для связи:", reply_markup=keyboard)
    return CONTACT_MESSAGE

def handle_user_selection(update: Update, context: CallbackContext):
    """Обрабатывает выбор пользователя для связи"""
    query = update.callback_query
    query.answer()
    
    if query.data == "cancel_contact":
        query.edit_message_text("Отправка сообщения отменена.")
        return ConversationHandler.END
    
    selected_user = query.data.replace("user_", "")
    context.user_data['selected_user'] = selected_user
    
    query.edit_message_text(
        f"Вы выбрали пользователя: {selected_user}\n"
        "Пожалуйста, введите ваше сообщение:"
    )
    return CONTACT_MESSAGE

def handle_contact_message(update: Update, context: CallbackContext):
    """Обрабатывает введенное сообщение и отправляет его выбранному пользователю"""
    message_text = update.message.text
    selected_user = context.user_data.get('selected_user')
    contact_type = context.user_data.get('contact_type')
    
    if not selected_user or not contact_type:
        update.message.reply_text("Произошла ошибка. Пожалуйста, попробуйте снова.")
        return ConversationHandler.END
    
    # Получаем информацию об отправителе
    sender_tab_number = context.user_data.get('tab_number')
    cursor.execute('''
        SELECT name, role, location, division FROM (
            SELECT name, role, location, division FROM Users_admin_bot WHERE tab_number = ?
            UNION ALL
            SELECT name, role, location, division FROM Users_dir_bot WHERE tab_number = ?
            UNION ALL
            SELECT name, role, location, division FROM Users_user_bot WHERE tab_number = ?
        )
    ''', (sender_tab_number, sender_tab_number, sender_tab_number))
    sender_info = cursor.fetchone()
    
    if not sender_info:
        update.message.reply_text("Ошибка: не удалось найти информацию о вас.")
        return ConversationHandler.END
    
    sender_name, sender_role, sender_location, sender_division = sender_info
    
    # Получаем chat_id получателя
    if contact_type == 'admin':
        cursor.execute('SELECT chat_id FROM Users_admin_bot WHERE name = ?', (selected_user,))
    elif contact_type == 'operator':
        cursor.execute('SELECT chat_id FROM Users_user_bot WHERE name = ?', (selected_user,))
    else:  # manager
        cursor.execute('SELECT chat_id FROM Users_dir_bot WHERE name = ?', (selected_user,))
    
    recipient = cursor.fetchone()
    if not recipient:
        update.message.reply_text("Ошибка: не удалось найти получателя.")
        return ConversationHandler.END
    
    recipient_chat_id = recipient[0]
    
    # Формируем и отправляем сообщение
    formatted_message = (
        f"📨 Новое сообщение\n"
        f"От: {sender_name} ({sender_role})\n"
        f"Локация: {sender_location}\n"
        f"Подразделение: {sender_division}\n"
        f"------------------\n"
        f"{message_text}\n"
        f"------------------\n"
        f"Для ответа используйте кнопку 'Связаться с {sender_role.lower()}'"
    )
    
    try:
        context.bot.send_message(
            chat_id=recipient_chat_id,
            text=formatted_message,
            parse_mode='HTML'
        )
        update.message.reply_text("✅ Ваше сообщение успешно отправлено!")
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения: {e}")
        update.message.reply_text(
            "❌ Произошла ошибка при отправке сообщения. Пожалуйста, попробуйте позже."
        )
    
    # Очищаем данные контакта
    for key in ['contact_admin_tab', 'contact_admin_name', 'contact_operator_tab', 
               'contact_operator_name', 'contact_manager_tab', 'contact_manager_name',
               'waiting_for_message_to_admin', 'waiting_for_message_to_operator',
               'waiting_for_message_to_manager', 'operators_by_location', 'managers_by_location']:
        if key in context.user_data:
            del context.user_data[key]
    
    return ConversationHandler.END

def main():
    # Инициализация бота
    updater = Updater(token=os.getenv('BOT_TOKEN'), use_context=True)
    dp = updater.dispatcher
    
    logger.info("Бот запущен")
    logger.info("Зарегистрирован обработчик команды /start")
    
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
    
    # Add handlers in order of priority
    # Main conversation handler (highest priority)
    dp.add_handler(conv_handler, group=1)
    logger.info("Зарегистрирован обработчик диалога ввода табельного номера")
    
    # Command handlers (medium priority)
    # dp.add_handler(CommandHandler('admin_command', admin_command), group=2)
    # dp.add_handler(CommandHandler('manager_command', manager_command), group=2)
    # dp.add_handler(CommandHandler('user_command', user_command), group=2)
    # logger.info("Зарегистрированы обработчики команд для разных ролей")
    
    # Button handlers (medium priority)
    dp.add_handler(MessageHandler(Filters.regex('^(В начало)$'), handle_button), group=2)
    dp.add_handler(MessageHandler(Filters.regex('^Загрузить показания$'), handle_upload_readings), group=2)
    
    # Contact button handlers (medium priority)
    dp.add_handler(MessageHandler(Filters.regex('^Связаться с оператором$'), handle_contact_operator), group=2)
    logger.info("Зарегистрированы обработчики кнопок связи")
    
    # Callback query handlers (medium priority)
    dp.add_handler(CallbackQueryHandler(handle_select_location, pattern='^select_location_'), group=2)
    dp.add_handler(CallbackQueryHandler(handle_select_division, pattern='^select_division_'), group=2)
    dp.add_handler(CallbackQueryHandler(handle_select_mgr_location, pattern='^select_mgr_location_'), group=2)
    dp.add_handler(CallbackQueryHandler(handle_back_to_locations, pattern='^back_to_locations$'), group=2)
    dp.add_handler(CallbackQueryHandler(handle_back_to_mgr_locations, pattern='^back_to_mgr_locations$'), group=2)
    dp.add_handler(CallbackQueryHandler(handle_cancel_contact, pattern='^cancel_contact$'), group=2)
    logger.info("Зарегистрированы обработчики callback-запросов")
    
    # General message handler (lowest priority)
    dp.add_handler(MessageHandler(
        Filters.text & ~Filters.command & ~Filters.regex('^(В начало)$') & 
        ~Filters.regex('^Загрузить показания$') & 
        ~Filters.regex('^Связаться с оператором$'),
        handle_message_input
    ), group=3)
    
    # Обработчики для ввода показаний
    readings_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(Filters.regex('^Загрузить показания$'), handle_upload_readings)],
        states={
            ENTER_READINGS: [
                CallbackQueryHandler(readings_choice_handler),
                MessageHandler(Filters.text & ~Filters.command, lambda u, c: u.message.reply_text("Пожалуйста, используйте кнопки меню"))
            ],
            SELECT_EQUIPMENT: [
                CallbackQueryHandler(select_equipment_handler),
                MessageHandler(Filters.text & ~Filters.command, lambda u, c: u.message.reply_text("Пожалуйста, используйте кнопки меню"))
            ],
            ENTER_VALUE: [
                CallbackQueryHandler(enter_value_handler),
                MessageHandler(Filters.text & ~Filters.command, enter_value_handler)
            ],
            CONFIRM_READINGS: [
                CallbackQueryHandler(confirm_readings),
                MessageHandler(Filters.text & ~Filters.command, lambda u, c: u.message.reply_text("Пожалуйста, используйте кнопки меню"))
            ]
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            MessageHandler(Filters.regex('^Отмена$'), cancel)
        ],
        per_chat=True,
        per_message=True,
        name="readings_conversation"
    )
    dp.add_handler(readings_conv_handler)
    logger.info("Зарегистрирован обработчик ввода показаний")
    
    # Обработчик контактов между пользователями
    contact_handler = ConversationHandler(
        entry_points=[
            MessageHandler(Filters.regex('^Связаться с (администратором|оператором|руководителем)$'),
                         lambda update, context: handle_button(update, context))
        ],
        states={
            CONTACT_MESSAGE: [
                CallbackQueryHandler(handle_user_selection, pattern='^user_|^cancel_contact'),
                MessageHandler(Filters.text & ~Filters.command, handle_contact_message)
            ]
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            MessageHandler(Filters.regex('^Отмена$'), cancel),
            CallbackQueryHandler(handle_cancel_contact, pattern='^cancel_contact$')
        ],
        per_chat=True,
        per_message=True,
        name="contact_conversation"
    )
    dp.add_handler(contact_handler)
    logger.info("Зарегистрирован обработчик контактов между пользователями")
    
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
    
    # Запуск бота
    logger.info("Запуск бота...")
    updater.start_polling()
    logger.info("Бот успешно запущен и ожидает сообщений")
    updater.idle()

# Инициализация базы данных
def init_database():
    try:
        logger.info("Инициализация базы данных")
        # Создаем таблицу для администраторов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Users_admin_bot (
                tab_number INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                location TEXT,
                division TEXT
            )
        ''')
        
        # Создаем таблицу для руководителей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Users_dir_bot (
                tab_number INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                location TEXT,
                division TEXT
            )
        ''')
        
        # Создаем таблицу для обычных пользователей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Users_user_bot (
                tab_number INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                location TEXT,
                division TEXT
            )
        ''')
        
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
        
        conn.commit()
        logger.info("База данных успешно инициализирована")
        
        # Выполняем миграцию, если необходимо
        migrate_database()
    except Exception as e:
        logger.error(f"Ошибка при инициализации базы данных: {e}")

def migrate_database():
    """Миграция базы данных для использования chat_id"""
    try:
        # Создаем временные таблицы с новой схемой
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS Users_admin_bot_new (
            tab_number INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            role TEXT DEFAULT 'Администратор',
            chat_id INTEGER NOT NULL,
            location TEXT,
            division TEXT
        )''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS Users_user_bot_new (
            tab_number INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            role TEXT DEFAULT 'Пользователь',
            chat_id INTEGER NOT NULL,
            location TEXT,
            division TEXT
        )''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS Users_dir_bot_new (
            tab_number INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            role TEXT DEFAULT 'Руководитель',
            chat_id INTEGER NOT NULL,
            location TEXT,
            division TEXT
        )''')

        # Копируем данные из старых таблиц, используя tab_number как chat_id
        cursor.execute('''
        INSERT INTO Users_admin_bot_new (tab_number, name, role, chat_id, location, division)
        SELECT tab_number, name, role, tab_number, location, division
        FROM Users_admin_bot
        ''')

        cursor.execute('''
        INSERT INTO Users_user_bot_new (tab_number, name, role, chat_id, location, division)
        SELECT tab_number, name, role, tab_number, location, division
        FROM Users_user_bot
        ''')

        cursor.execute('''
        INSERT INTO Users_dir_bot_new (tab_number, name, role, chat_id, location, division)
        SELECT tab_number, name, role, tab_number, location, division
        FROM Users_dir_bot
        ''')

        # Удаляем старые таблицы
        cursor.execute('DROP TABLE IF EXISTS Users_admin_bot')
        cursor.execute('DROP TABLE IF EXISTS Users_user_bot')
        cursor.execute('DROP TABLE IF EXISTS Users_dir_bot')

        # Переименовываем новые таблицы
        cursor.execute('ALTER TABLE Users_admin_bot_new RENAME TO Users_admin_bot')
        cursor.execute('ALTER TABLE Users_user_bot_new RENAME TO Users_user_bot')
        cursor.execute('ALTER TABLE Users_dir_bot_new RENAME TO Users_dir_bot')

        conn.commit()
        logger.info("Миграция базы данных успешно завершена")
    except Exception as e:
        logger.error(f"Ошибка при миграции базы данных: {e}", exc_info=True)
        conn.rollback()
        raise

# Вызываем инициализацию при запуске
init_database()

if __name__ == '__main__':
    main()