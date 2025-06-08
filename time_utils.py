# time_utils.py
import pytz
from datetime import datetime, time
import logging

logger = logging.getLogger(__name__)

# Часовые пояса России
RUSSIAN_TIMEZONES = {
    # Центральный федеральный округ (UTC+3)
    'Белго': 'Europe/Moscow',
    'Брянс': 'Europe/Moscow',
    'Влади': 'Europe/Moscow',
    'Ворон': 'Europe/Moscow',
    'Ивано': 'Europe/Moscow',
    'Калуж': 'Europe/Moscow',
    'Костр': 'Europe/Moscow',
    'Курск': 'Europe/Moscow',
    'Липец': 'Europe/Moscow',
    'Москв': 'Europe/Moscow',
    'Орлов': 'Europe/Moscow',
    'Рязан': 'Europe/Moscow',
    'Смоле': 'Europe/Moscow',
    'Тамбо': 'Europe/Moscow',
    'Тверс': 'Europe/Moscow',
    'Тульс': 'Europe/Moscow',
    'Яросл': 'Europe/Moscow',
    
    # Северо-Западный федеральный округ
    'Архан': 'Europe/Moscow',
    'Волог': 'Europe/Moscow',
    'Калин': 'Europe/Kaliningrad',  # UTC+2
    'Карел': 'Europe/Moscow',
    'Коми': 'Europe/Moscow',
    'Ленин': 'Europe/Moscow',
    'Мурма': 'Europe/Moscow',
    'Ненец': 'Europe/Moscow',
    'Новго': 'Europe/Moscow',
    'Псков': 'Europe/Moscow',
    'Санкт': 'Europe/Moscow',
    
    # Южный и Северо-Кавказский федеральные округа
    'Адыге': 'Europe/Moscow',
    'Астра': 'Europe/Samara',  # UTC+4
    'Волго': 'Europe/Moscow',
    'Дагес': 'Europe/Moscow',
    'Ингуш': 'Europe/Moscow',
    'Кабар': 'Europe/Moscow',
    'Калмы': 'Europe/Moscow',
    'Карач': 'Europe/Moscow',
    'Красн': 'Europe/Moscow',  # Краснодарский край
    'Крым': 'Europe/Moscow',
    'Росто': 'Europe/Moscow',
    'Север': 'Europe/Moscow',
    'Ставр': 'Europe/Moscow',
    'Чечня': 'Europe/Moscow',
    
    # Приволжский федеральный округ
    'Башко': 'Asia/Yekaterinburg',  # UTC+5
    'Киров': 'Europe/Moscow',
    'Марий': 'Europe/Moscow',
    'Мордо': 'Europe/Moscow',
    'Нижег': 'Europe/Moscow',
    'Оренб': 'Asia/Yekaterinburg',  # UTC+5
    'Пензе': 'Europe/Moscow',
    'Пермс': 'Asia/Yekaterinburg',  # UTC+5
    'Самар': 'Europe/Samara',  # UTC+4
    'Сарат': 'Europe/Samara',  # UTC+4
    'Татар': 'Europe/Moscow',
    'Удмур': 'Europe/Samara',  # UTC+4
    'Ульян': 'Europe/Samara',  # UTC+4
    'Чуваш': 'Europe/Moscow',
    
    # Уральский федеральный округ
    'Курга': 'Asia/Yekaterinburg',  # UTC+5
    'Сверд': 'Asia/Yekaterinburg',  # UTC+5
    'Тюмен': 'Asia/Yekaterinburg',  # UTC+5
    'Ханты': 'Asia/Yekaterinburg',  # UTC+5
    'Челяб': 'Asia/Yekaterinburg',  # UTC+5
    'Ямало': 'Asia/Yekaterinburg',  # UTC+5
    
    # Сибирский федеральный округ
    'Алтай': 'Asia/Krasnoyarsk',  # UTC+7
    'Бурят': 'Asia/Irkutsk',  # UTC+8
    'Забай': 'Asia/Yakutsk',  # UTC+9
    'Иркут': 'Asia/Irkutsk',  # UTC+8
    'Кемер': 'Asia/Krasnoyarsk',  # UTC+7
    'Красн': 'Asia/Krasnoyarsk',  # UTC+7 - Красноярский край
    'Новос': 'Asia/Krasnoyarsk',  # UTC+7
    'Омска': 'Asia/Omsk',  # UTC+6
    'Томск': 'Asia/Krasnoyarsk',  # UTC+7
    'Тыва': 'Asia/Krasnoyarsk',  # UTC+7
    'Хакас': 'Asia/Krasnoyarsk',  # UTC+7
    
    # Дальневосточный федеральный округ
    'Амурс': 'Asia/Yakutsk',  # UTC+9
    'Еврей': 'Asia/Vladivostok',  # UTC+10
    'Камча': 'Asia/Kamchatka',  # UTC+12
    'Магад': 'Asia/Magadan',  # UTC+11
    'Примо': 'Asia/Vladivostok',  # UTC+10
    'Саха': 'Asia/Yakutsk',  # UTC+9
    'Сахал': 'Asia/Magadan',  # UTC+11
    'Хабар': 'Asia/Vladivostok',  # UTC+10
    'Чукот': 'Asia/Kamchatka'  # UTC+12
}