import unittest
import sqlite3
import os
from datetime import datetime
import pandas as pd
from telegram import Update, Chat, Message, User, InlineKeyboardMarkup
from telegram.ext import CallbackContext, ConversationHandler
from unittest.mock import MagicMock, patch
import main  # импортируем основной файл бота

class TestBot(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Подготовка тестовой среды"""
        # Создаем тестовую базу данных
        cls.test_db = 'test_Users_bot.db'
        cls.conn = sqlite3.connect(cls.test_db)
        cls.cursor = cls.conn.cursor()
        
        # Создаем тестовые таблицы
        cls.cursor.executescript('''
            CREATE TABLE IF NOT EXISTS Users_admin_bot (
                tab_number INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                role TEXT DEFAULT 'Администратор',
                chat_id INTEGER NOT NULL,
                location TEXT,
                division TEXT
            );
            
            CREATE TABLE IF NOT EXISTS Users_user_bot (
                tab_number INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                role TEXT DEFAULT 'Пользователь',
                chat_id INTEGER NOT NULL,
                location TEXT,
                division TEXT
            );
            
            CREATE TABLE IF NOT EXISTS Users_dir_bot (
                tab_number INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                role TEXT DEFAULT 'Руководитель',
                chat_id INTEGER NOT NULL,
                location TEXT,
                division TEXT
            );
            
            CREATE TABLE IF NOT EXISTS shifts (
                tab_number INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                is_on_shift TEXT DEFAULT 'НЕТ'
            );

            CREATE TABLE IF NOT EXISTS daily_shifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                employee_name TEXT NOT NULL,
                status TEXT NOT NULL
            );
        ''')
        
        # Добавляем тестовые данные
        cls.test_data = {
            'admin': (345667, 'Алексей Романович Орлов', 'Администратор', 89773214789, 'Томск', 'ТОМСК. ОБЛ Бериевский'),
            'manager': (23456, 'София Аркадиевна Смола', 'Руководитель', 34567890, 'Томск', 'ТОМСК. ОБЛ Рокоссовский'),
            'user': (87346, 'Порфирий Петрович Глушков', 'Пользователь', 99999999, 'Томск', 'ТОМСК. ОБЛ Бериевский')
        }
        
        # Очищаем таблицы перед вставкой
        cls.cursor.execute('DELETE FROM Users_admin_bot')
        cls.cursor.execute('DELETE FROM Users_dir_bot')
        cls.cursor.execute('DELETE FROM Users_user_bot')
        cls.cursor.execute('DELETE FROM shifts')
        cls.cursor.execute('DELETE FROM daily_shifts')
        
        # Вставляем тестовые данные
        cls.cursor.execute('''
            INSERT INTO Users_admin_bot (tab_number, name, role, chat_id, location, division) 
            VALUES (?, ?, ?, ?, ?, ?)
        ''', cls.test_data['admin'])
        
        cls.cursor.execute('''
            INSERT INTO Users_dir_bot (tab_number, name, role, chat_id, location, division) 
            VALUES (?, ?, ?, ?, ?, ?)
        ''', cls.test_data['manager'])
        
        cls.cursor.execute('''
            INSERT INTO Users_user_bot (tab_number, name, role, chat_id, location, division) 
            VALUES (?, ?, ?, ?, ?, ?)
        ''', cls.test_data['user'])
        
        # Добавляем тестовые смены для всех пользователей
        current_date = datetime.now().strftime('%d.%m.%Y')
        for user_type, data in cls.test_data.items():
            # Добавляем в таблицу shifts
            cls.cursor.execute('''
                INSERT INTO shifts (tab_number, name, is_on_shift) 
                VALUES (?, ?, ?)
            ''', (data[0], data[1], 'ДА'))
            
            # Добавляем в таблицу daily_shifts
            cls.cursor.execute('''
                INSERT INTO daily_shifts (date, employee_name, status) 
                VALUES (?, ?, ?)
            ''', (current_date, data[1], 'ДА'))
        
        cls.conn.commit()
        
        # Создаем тестовые Excel файлы
        cls.create_test_excel()

    @classmethod
    def create_test_excel(cls):
        """Создание тестового Excel файла"""
        data = {
            'Табельный номер': [345667, 23456, 87346],
            'ФИО': ['Алексей Романович Орлов', 'София Аркадиевна Смола', 'Порфирий Петрович Глушков'],
            'Роль': ['Администратор', 'Руководитель', 'Пользователь'],
            'Номер телефона': [89773214789, 34567890, 99999999],
            'Локация': ['Томск', 'Томск', 'Томск'],
            'Подразделение': ['ТОМСК. ОБЛ Бериевский', 'ТОМСК. ОБЛ Рокоссовский', 'ТОМСК. ОБЛ Бериевский']
        }
        df = pd.DataFrame(data)
        df.to_excel('Users.xlsx', index=False)

        # Создаем тестовый файл с табелями
        shifts_data = {
            'Дата': [datetime.now().strftime('%d.%m.%Y')] * 3,
            'ФИО': ['Алексей Романович Орлов', 'София Аркадиевна Смола', 'Порфирий Петрович Глушков'],
            'Статус': ['ДА', 'ДА', 'ДА']
        }
        shifts_df = pd.DataFrame(shifts_data)
        shifts_df.to_excel('tabels.xlsx', index=False)

    def setUp(self):
        """Подготовка каждого теста"""
        # Создаем базовые моки
        self.update = MagicMock(spec=Update)
        self.context = MagicMock(spec=CallbackContext)
        
        # Создаем моки для Update
        self.update.effective_chat = MagicMock(spec=Chat)
        self.update.effective_chat.id = 12345
        self.update.message = MagicMock(spec=Message)
        self.update.effective_user = MagicMock(spec=User)
        self.update.effective_user.id = 12345
        
        # Настраиваем message.reply_text
        self.update.message.reply_text = MagicMock()
        
        # Создаем моки для Context
        self.context.user_data = {}
        self.context.bot = MagicMock()
        self.context.bot.send_message = MagicMock()
        
        # Патчим глобальные переменные main.py
        self.patcher = patch('main.conn', self.conn)
        self.patcher.start()
        
        # Патчим shifts_handler
        self.shifts_handler_patcher = patch('main.shifts_handler')
        self.mock_shifts_handler = self.shifts_handler_patcher.start()
        self.mock_shifts_handler.check_employee_status.return_value = 'ДА'
        self.mock_shifts_handler.is_user_available.return_value = True
        
        # Устанавливаем базовые данные пользователя
        self.context.user_data.update({
            'tab_number': self.test_data['user'][0],
            'name': self.test_data['user'][1],
            'role': 'Пользователь',
            'chat_id': self.test_data['user'][3],
            'location': self.test_data['user'][4],
            'division': self.test_data['user'][5]
        })

    def tearDown(self):
        """Очистка после каждого теста"""
        self.patcher.stop()
        self.shifts_handler_patcher.stop()

    def test_start_command(self):
        """Тест команды /start"""
        self.update.message.text = "/start"
        result = main.start(self.update, self.context)
        self.assertEqual(result, main.ENTER_TAB_NUMBER)

    def test_handle_tab_number(self):
        """Тест ввода табельного номера"""
        # Тест существующего пользователя
        self.update.message.text = "345667"  # Тестовый админ
        result = main.handle_tab_number(self.update, self.context)
        self.assertEqual(result, main.ConversationHandler.END)
        
        # Тест несуществующего пользователя
        self.update.message.text = "999999"
        result = main.handle_tab_number(self.update, self.context)
        self.assertEqual(result, main.ENTER_TAB_NUMBER)

    def test_check_access(self):
        """Тест проверки доступа"""
        # Тест администратора
        self.context.user_data.update({
            'tab_number': self.test_data['admin'][0],
            'role': 'Администратор'
        })
        self.assertTrue(main.check_access(self.update, self.context))
        
        # Тест оператора на смене
        self.context.user_data.update({
            'tab_number': self.test_data['user'][0],
            'role': 'Пользователь'
        })
        self.mock_shifts_handler.check_employee_status.return_value = 'ДА'
        self.assertTrue(main.check_access(self.update, self.context))
        
        # Тест оператора не на смене
        self.mock_shifts_handler.check_employee_status.return_value = 'НЕТ'
        self.assertFalse(main.check_access(self.update, self.context))

    def test_contact_admin(self):
        """Тест функции связи с администратором"""
        # Подготовка контекста
        self.context.user_data.update({
            'tab_number': self.test_data['user'][0],
            'name': self.test_data['user'][1],
            'role': 'Пользователь',
            'location': 'Томск',
            'division': 'ТОМСК. ОБЛ Бериевский'
        })
        
        # Добавляем администратора в базу данных для этой локации и подразделения
        self.cursor.execute('DELETE FROM Users_admin_bot')
        self.cursor.execute('''
            INSERT INTO Users_admin_bot 
            (tab_number, name, role, chat_id, location, division) 
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            345667,  # tab_number
            'Алексей Романович Орлов',  # name
            'Администратор',  # role
            89773214789,  # chat_id
            'Томск',  # location
            'ТОМСК. ОБЛ Бериевский'  # division
        ))
        self.conn.commit()
        
        # Проверяем, что администратор добавлен
        self.cursor.execute('SELECT * FROM Users_admin_bot')
        admin = self.cursor.fetchone()
        self.assertIsNotNone(admin, "Администратор не был добавлен в базу данных")
        
        # Вызываем функцию
        result = main.handle_contact_admin(self.update, self.context)
        
        # Проверяем результат
        self.assertEqual(result, main.CONTACT_MESSAGE)
        
        # Проверяем, что сообщение было отправлено
        self.update.message.reply_text.assert_called()
        
        # Проверяем содержимое отправленного сообщения
        call_args = self.update.message.reply_text.call_args
        self.assertIsNotNone(call_args)
        message_text = call_args[0][0]
        self.assertEqual(message_text, "Выберите администратора для связи:")

    def test_contact_operator(self):
        """Тест функции связи с оператором"""
        # Подготовка контекста
        self.context.user_data.update({
            'tab_number': self.test_data['admin'][0],
            'name': self.test_data['admin'][1],
            'role': 'Администратор',
            'location': 'Томск',
            'division': 'ТОМСК. ОБЛ Бериевский'
        })
        
        # Вызываем функцию
        result = main.start_contact_operator(self.update, self.context)
        
        # Проверяем результат
        self.assertEqual(result, main.CONTACT_MESSAGE)
        
        # Проверяем, что сообщение было отправлено
        self.update.message.reply_text.assert_called()
        
        # Проверяем содержимое отправленного сообщения
        call_args = self.update.message.reply_text.call_args
        self.assertIsNotNone(call_args)
        message_text = call_args[0][0]
        self.assertIn('Выберите оператора', message_text)

    def test_contact_manager(self):
        """Тест функции связи с руководителем"""
        # Подготовка контекста
        self.context.user_data.update({
            'tab_number': self.test_data['user'][0],
            'name': self.test_data['user'][1],
            'role': 'Пользователь',
            'location': 'Томск',
            'division': 'ТОМСК. ОБЛ Бериевский'
        })
        
        # Вызываем функцию
        result = main.start_contact_manager(self.update, self.context)
        
        # Проверяем результат
        self.assertEqual(result, main.CONTACT_MESSAGE)
        
        # Проверяем, что сообщение было отправлено
        self.update.message.reply_text.assert_called()
        
        # Проверяем содержимое отправленного сообщения
        call_args = self.update.message.reply_text.call_args
        self.assertIsNotNone(call_args)
        message_text = call_args[0][0]
        self.assertIn('Выберите руководителя', message_text)

    def test_message_sending(self):
        """Тест отправки сообщений"""
        # Подготовка данных
        message_text = "Тестовое сообщение"
        self.update.message.text = message_text
        
        # Подготовка контекста
        self.context.user_data.update({
            'tab_number': self.test_data['user'][0],
            'name': self.test_data['user'][1],
            'role': 'Пользователь',
            'selected_user': 'Алексей Романович Орлов',
            'contact_type': 'admin'
        })
        
        # Вызываем функцию
        result = main.handle_contact_message(self.update, self.context)
        
        # Проверяем результат
        self.assertEqual(result, main.ConversationHandler.END)
        
        # Проверяем, что бот пытался отправить сообщение
        self.context.bot.send_message.assert_called()
        
        # Проверяем параметры отправленного сообщения
        call_args = self.context.bot.send_message.call_args
        self.assertIsNotNone(call_args)
        kwargs = call_args[1]
        self.assertIn('text', kwargs)
        self.assertIn('Тестовое сообщение', kwargs['text'])
        self.assertIn('Порфирий Петрович Глушков', kwargs['text'])

    def test_full_contact_flow(self):
        """Тест полного процесса отправки сообщения"""
        # Подготовка контекста
        self.context.user_data.update({
            'tab_number': self.test_data['user'][0],
            'name': self.test_data['user'][1],
            'role': 'Пользователь',
            'location': 'Томск',
            'division': 'ТОМСК. ОБЛ Бериевский',
            'selected_user': 'Алексей Романович Орлов',
            'contact_type': 'admin'
        })
        
        # Подготовка сообщения
        self.update.message.text = "Тестовое сообщение"
        
        # Вызываем функцию отправки сообщения
        result = main.handle_contact_message(self.update, self.context)
        
        # Проверяем результат
        self.assertEqual(result, main.ConversationHandler.END)
        
        # Проверяем, что бот пытался отправить сообщение
        self.context.bot.send_message.assert_called()
        
        # Проверяем параметры отправленного сообщения
        call_args = self.context.bot.send_message.call_args
        self.assertIsNotNone(call_args)
        kwargs = call_args[1]
        self.assertIn('text', kwargs)
        self.assertIn('Тестовое сообщение', kwargs['text'])
        self.assertIn('Порфирий Петрович Глушков', kwargs['text'])

    def test_handle_upload_readings(self):
        """Тест функции загрузки показаний"""
        # Подготовка контекста
        self.context.user_data.update({
            'tab_number': self.test_data['user'][0],
            'name': self.test_data['user'][1],
            'role': 'Пользователь',
            'location': self.test_data['user'][4],
            'division': self.test_data['user'][5]
        })
        
        # Вызываем функцию
        result = main.handle_upload_readings(self.update, self.context)
        
        # Проверяем результат
        self.assertEqual(result, main.ENTER_READINGS)
        self.update.message.reply_text.assert_called()
        
        # Проверяем содержимое сообщения
        call_args = self.update.message.reply_text.call_args
        self.assertIsNotNone(call_args)
        message_text = call_args[0][0]
        self.assertIn('Выберите способ подачи показаний счетчиков', message_text)
        self.assertIn(self.test_data['user'][4], message_text)  # Проверяем локацию
        self.assertIn(self.test_data['user'][5], message_text)  # Проверяем подразделение

    def test_readings_validation(self):
        """Тест валидации показаний счетчиков"""
        from check import MeterValidator
        validator = MeterValidator()
        
        # Создаем тестовый файл оборудования только если его нет
        if not os.path.exists('Equipment.xlsx'):
            equipment_data = {
                'Локация': ['Томск', 'Томск'],
                'Подразделение': ['ТОМСК. ОБЛ Бериевский', 'ТОМСК. ОБЛ Бериевский'],
                'Гос. номер': ['A123BC', 'B456DE'],
                'Инв. №': ['INV001', 'INV002'],
                'Счётчик': ['PM123', 'KM456'],
                'Тип счетчика': ['Моточасы', 'Километраж']
            }
            equipment_df = pd.DataFrame(equipment_data)
            equipment_df.to_excel('Equipment.xlsx', index=False)
        
        # Создаем тестовый файл с показаниями
        test_readings = {
            '№ п/п': [1, 2],
            'Гос. номер': ['A123BC', 'B456DE'],
            'Инв. №': ['INV001', 'INV002'],
            'Счётчик': ['PM123', 'KM456'],
            'Показания': [100, 500],
            'Комментарий': ['', '']
        }
        readings_df = pd.DataFrame(test_readings)
        readings_df.to_excel('test_readings.xlsx', index=False)
        
        try:
            # Тестируем валидацию
            validation_result = validator.validate_file('test_readings.xlsx', {
                'location': 'Томск',
                'division': 'ТОМСК. ОБЛ Бериевский',
                'tab_number': '12345',
                'name': 'Тестовый пользователь'
            })
            
            self.assertTrue(validation_result['is_valid'])
            self.assertEqual(len(validation_result['errors']), 0)
            
        finally:
            # Удаляем только временный файл с показаниями
            if os.path.exists('test_readings.xlsx'):
                os.remove('test_readings.xlsx')

    def test_daily_shifts_update(self):
        """Тест обновления ежедневных смен"""
        # Создаем тестовый файл табеля
        shifts_data = {
            'ФИО': [self.test_data['user'][1]],
            datetime.now().strftime('%d.%m.%Y'): ['ДА']
        }
        df = pd.DataFrame(shifts_data)
        df.to_excel('tabels.xlsx', index=False)
        
        try:
            # Вызываем обновление смен
            main.shifts_handler.load_tabel()
            
            # Проверяем статус в базе
            current_date = datetime.now().strftime('%d.%m.%Y')
            self.cursor.execute('''
                SELECT status FROM daily_shifts 
                WHERE date = ? AND employee_name = ?
            ''', (current_date, self.test_data['user'][1]))
            
            result = self.cursor.fetchone()
            self.assertIsNotNone(result)
            self.assertEqual(result[0], 'ДА')
        finally:
            # Удаляем тестовый файл
            if os.path.exists('tabels.xlsx'):
                os.remove('tabels.xlsx')

    def test_equipment_list(self):
        """Тест получения списка оборудования"""
        # Создаем тестовый файл оборудования
        equipment_data = {
            'Локация': [self.test_data['user'][4]],
            'Подразделение': [self.test_data['user'][5]],
            'Гос. номер': ['A123BC'],
            'Инв. №': ['INV001'],
            'Счётчик': ['PM123']
        }
        df = pd.DataFrame(equipment_data)
        df.to_excel('Equipment.xlsx', index=False)
        
        try:
            from check import MeterValidator
            validator = MeterValidator()
            
            # Получаем список оборудования
            equipment_df = validator._get_equipment_for_location_division(
                self.test_data['user'][4],
                self.test_data['user'][5]
            )
            
            # Проверяем результат
            self.assertFalse(equipment_df.empty)
            self.assertEqual(len(equipment_df), 1)
            self.assertEqual(equipment_df.iloc[0]['Гос. номер'], 'A123BC')
            self.assertEqual(equipment_df.iloc[0]['Инв. №'], 'INV001')
            self.assertEqual(equipment_df.iloc[0]['Счётчик'], 'PM123')
        finally:
            # Удаляем тестовый файл
            if os.path.exists('Equipment.xlsx'):
                os.remove('Equipment.xlsx')

    def test_notifications(self):
        """Тест отправки уведомлений"""
        # Создаем тестовый файл с показаниями
        readings_data = {
            '№ п/п': [1],
            'Гос. номер': ['A123BC'],
            'Инв. №': ['INV001'],
            'Счётчик': ['PM123'],
            'Показания': [100],
            'Комментарий': ['']
        }
        df = pd.DataFrame(readings_data)
        
        # Создаем директории для отчетов
        current_week = datetime.now().strftime('%Y-W%U')
        report_folder = f'meter_readings/week_{current_week}'
        os.makedirs(report_folder, exist_ok=True)
        
        file_path = f'{report_folder}/test_readings.xlsx'
        df.to_excel(file_path, index=False)
        
        try:
            # Тестируем отправку уведомлений
            from meters_handler import notify_admins_and_managers
            notify_admins_and_managers(
                self.context,
                self.test_data['user'][0],
                self.test_data['user'][1],
                self.test_data['user'][4],
                self.test_data['user'][5],
                file_path
            )
            
            # Проверяем, что бот пытался отправить сообщение
            self.context.bot.send_message.assert_called()
            
            # Проверяем параметры отправленного сообщения
            call_args = self.context.bot.send_message.call_args
            self.assertIsNotNone(call_args)
            kwargs = call_args[1]
            self.assertIn('text', kwargs)
            self.assertIn(self.test_data['user'][1], kwargs['text'])
            self.assertIn(self.test_data['user'][4], kwargs['text'])
            self.assertIn(self.test_data['user'][5], kwargs['text'])
        finally:
            # Удаляем тестовый файл и директорию
            if os.path.exists(file_path):
                os.remove(file_path)
            if os.path.exists(report_folder):
                os.rmdir(report_folder)
            if os.path.exists('meter_readings'):
                os.rmdir('meter_readings')

    @classmethod
    def tearDownClass(cls):
        """Очистка после всех тестов"""
        cls.conn.close()
        os.remove(cls.test_db)

if __name__ == '__main__':
    unittest.main(verbosity=2) 