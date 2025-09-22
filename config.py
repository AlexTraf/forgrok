import asyncio
import json
import logging
from logging import StreamHandler
import os
from datetime import datetime
from os import listdir
from aiogram import Bot, Dispatcher, F
from aiogram.filters.callback_data import CallbackData
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from telethon.tl.types import ReactionEmoji
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz

# Логгер
rootLogger = logging.getLogger('root')
rootLogger.setLevel(logging.DEBUG)

formatter = logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s]: %(message)s (%(filename)s:%(lineno)d)')

fileHandler = logging.FileHandler("app.log", "w", encoding="utf-8")
fileHandler.setLevel(logging.DEBUG)
fileHandler.setFormatter(formatter)
rootLogger.addHandler(fileHandler)

consoleHandler = StreamHandler()
consoleHandler.setLevel(logging.INFO)
consoleHandler.setFormatter(formatter)
rootLogger.addHandler(consoleHandler)

today = datetime.now().strftime("%Y-%m-%d")
daily_log_file = f"daily_log_{today}.txt"
dailyLogHandler = logging.FileHandler(daily_log_file, "a", encoding="utf-8")
dailyLogHandler.setLevel(logging.INFO)
dailyLogHandler.setFormatter(formatter)
rootLogger.addHandler(dailyLogHandler)

logging.getLogger('telethon').setLevel(logging.WARNING)

# Переменные
liking_tasks = []
company_stats = {}
stats_lock = asyncio.Lock()
chat_lock = asyncio.Lock()
current_company = None
company_active = {}
config = {}
with open("config.json", "r", encoding='utf-8') as config_file:
    config = json.load(config_file)
scheduler = AsyncIOScheduler()
bot = Bot(config["token"])
dp = Dispatcher()
owner_id = config["owner_id"]
chat_id = config["chat_id"]
default_company_config: dict = {}
sessions = []
selected_company = None
company_configs = {}
created_channels = {}
users_paused = {}
api_id = 28229033
api_hash = "00dda765bff2ad74e70cc0bb68eb6e6b"

POSITIVE_REACTIONS = ["❤️", "👍", "🔥", "😍", "🎉"]

TRANS_TABLE = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e', 'ж': 'zh',
    'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o',
    'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'h', 'ц': 'ts',
    'ч': 'ch', 'ш': 'sh', 'щ': 'sch', 'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu',
    'я': 'ya'
}

DEVICES_INFO = [
    {"model": "iPhone 13 Pro Max", "system_version": "18.2", "app_version": "11.2"},
    {"model": "iPhone XR", "system_version": "18.1", "app_version": "11.2"},
    {"model": "iPhone 16 Pro", "system_version": "17.2", "app_version": "11.2"}
]

# Клавиатуры
kb_menu = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Добавить чаты/каналы", callback_data="add_chats")],
    [InlineKeyboardButton(text="Добавить сессии", callback_data="add_sessions")],
    [InlineKeyboardButton(text="Изменить детали сессий", callback_data="change_sessions")],
    [InlineKeyboardButton(text="Выбрать другую компанию", callback_data="change_company")],
    [InlineKeyboardButton(text="Статистика аккаунтов", callback_data="account_stats")],
    [InlineKeyboardButton(text="Проверить состояние аккаунтов", callback_data="check_account_status")],
    [InlineKeyboardButton(text="Переключить лайкинг сторис", callback_data="toggle_story_liking")],
    [InlineKeyboardButton(text="Отправить ЛС", callback_data="send_pm")]
])

kb_change_settings = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Изменить имя аккаунтов", callback_data="change_fname")],
    [InlineKeyboardButton(text="Изменить био аккаунтов", callback_data="change_bio")],
    [InlineKeyboardButton(text="Изменить фамилию аккаунтов", callback_data="change_lname")],
    [InlineKeyboardButton(text="Удалить фото аккаунтов", callback_data="delete_avatar")],
    [InlineKeyboardButton(text="Поставить новое фото аккаунтов", callback_data="change_avatar")],
    [InlineKeyboardButton(text="Добавить канал", callback_data="add_channel")],
    [InlineKeyboardButton(text="Собрать статистику просмотров", callback_data="collect_created_views_stats")],
    [InlineKeyboardButton(text="Изменить реакции аккаунтов", callback_data="change_story_reactions")],
    [InlineKeyboardButton(text="Назад", callback_data="back_to_menu")]
])

kb_company_config = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Назад", callback_data="back_to_menu")]
])

kb_all_or_select = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Все аккаунты", callback_data="select_all")],
    [InlineKeyboardButton(text="Выбранные аккаунты", callback_data="select_selective")],
    [InlineKeyboardButton(text="Назад", callback_data="back_to_channel")]
])

kb_add_users = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Добавить пользователей из файла Excel", callback_data="add_users_excel")],
    [InlineKeyboardButton(text="Добавить пользователей текстом", callback_data="add_users_text")],
    [InlineKeyboardButton(text="Назад", callback_data="back_to_change")]
])

# Классы состояний
class CompanyToggleState(StatesGroup):
    selected_companies = State()

class AddChatsState(StatesGroup):
    chats = State()

class BlacklistState(StatesGroup):
    company = State()
    usernames = State()

class SendPMState(StatesGroup):
    username = State()
    message = State()

class CollectViewsStatsState(StatesGroup):
    channel_name = State()

class CreateCompanyState(StatesGroup):
    name = State()

class CompanyConfigChangeState(StatesGroup):
    change = State()
    value = State()

class AddPrivateChannelState(StatesGroup):
    name = State()
    avatar = State()
    posts = State()
    select = State()
    confirm = State()

class AddSessionState(StatesGroup):
    add = State()

class ChangeState(StatesGroup):
    change = State()
    value = State()
    select = State()

class CompanyToggleCallback(CallbackData, prefix="company_toggle"):
    action: str
    company_name: str