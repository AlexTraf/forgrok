import asyncio
from datetime import datetime, timedelta
import pytz
import sys
import os
from aiogram.types import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from aiogram.filters.callback_data import CallbackData
from os import listdir, path, makedirs, rename
from telethon import TelegramClient, events, functions, types
from telethon.errors import ChannelPrivateError, FloodWaitError, ForbiddenError, UserNotParticipantError, UsernameOccupiedError
from telethon.tl.types import ChannelParticipantBanned, ReactionEmoji
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, DeleteHistoryRequest
from aiogram import Bot, Dispatcher, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram import types as atypes
import zipfile
import sqlite3
import logging
import random
import json
import tempfile
import shutil
from telethon.tl.functions.stories import GetPeerStoriesRequest, ReadStoriesRequest, SendReactionRequest
from typing import Union
import re
# Логгер
rootLogger = logging.getLogger('root')
rootLogger.setLevel(logging.DEBUG)

formatter = logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s]: %(message)s (%(filename)s:%(lineno)d)')

fileHandler = logging.FileHandler("app.log", "w", encoding="utf-8")
fileHandler.setLevel(logging.DEBUG)
fileHandler.setFormatter(formatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setLevel(logging.INFO)
consoleHandler.setFormatter(formatter)
rootLogger.addHandler(consoleHandler)

FROZE_CHECK_BOT_USERNAME: str = "@vpilotnotifybot"

LIKES_PER_ACCOUNT: int = 50
LIKES_WAIT_SECONDS: int = 100

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
kb_menu = atypes.InlineKeyboardMarkup(inline_keyboard=[
    [atypes.InlineKeyboardButton(text="Добавить чаты/каналы", callback_data="add_chats")],
    [atypes.InlineKeyboardButton(text="Добавить сессии", callback_data="add_sessions")],
    [atypes.InlineKeyboardButton(text="Изменить детали сессий", callback_data="change_sessions")],
    [atypes.InlineKeyboardButton(text="Выбрать другую компанию", callback_data="change_company")],
    [atypes.InlineKeyboardButton(text="Статистика аккаунтов", callback_data="account_stats")],
    [atypes.InlineKeyboardButton(text="Проверить состояние аккаунтов", callback_data="check_account_status")],
    [atypes.InlineKeyboardButton(text="Переключить лайкинг сторис", callback_data="toggle_story_liking")],  # Исправлено
    [atypes.InlineKeyboardButton(text="Отправить ЛС", callback_data="send_pm")]
])

kb_change_settings = atypes.InlineKeyboardMarkup(inline_keyboard=[
    [atypes.InlineKeyboardButton(text="Изменить имя аккаунтов", callback_data="change_fname")],
    [atypes.InlineKeyboardButton(text="Изменить био аккаунтов", callback_data="change_bio")],
    [atypes.InlineKeyboardButton(text="Изменить фамилию аккаунтов", callback_data="change_lname")],
    [atypes.InlineKeyboardButton(text="Удалить фото аккаунтов", callback_data="delete_avatar")],
    [atypes.InlineKeyboardButton(text="Поставить новое фото аккаунтов", callback_data="change_avatar")],
    [atypes.InlineKeyboardButton(text="Добавить канал", callback_data="add_channel")],
    [atypes.InlineKeyboardButton(text="Собрать статистику просмотров", callback_data="collect_created_views_stats")],
    [atypes.InlineKeyboardButton(text="Изменить реакции аккаунтов", callback_data="change_story_reactions")],
    [atypes.InlineKeyboardButton(text="Назад", callback_data="back_to_menu")]
])

kb_company_config = atypes.InlineKeyboardMarkup(inline_keyboard=[
    [atypes.InlineKeyboardButton(text="Назад", callback_data="back_to_menu")]
])

kb_all_or_select = atypes.InlineKeyboardMarkup(inline_keyboard=[
    [atypes.InlineKeyboardButton(text="Все аккаунты", callback_data="select_all")],
    [atypes.InlineKeyboardButton(text="Выбранные аккаунты", callback_data="select_selective")],
    [atypes.InlineKeyboardButton(text="Назад", callback_data="back_to_channel")]
])

kb_add_users = atypes.InlineKeyboardMarkup(inline_keyboard=[
    [atypes.InlineKeyboardButton(text="Добавить пользователей из файла Excel", callback_data="add_users_excel")],
    [atypes.InlineKeyboardButton(text="Добавить пользователей текстом", callback_data="add_users_text")],
    [atypes.InlineKeyboardButton(text="Назад", callback_data="back_to_change")]
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

class CompanyToggleCallback(CallbackData, prefix="company_toggle"):
    action: str
    company_name: str

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

# Класс сессии
class Session:
    def __init__(self, client, filename: str, company: str) -> None:
        self.app = client
        self.id = None
        self.me = None
        self.unblocked_at: datetime | None = None
        self.blocked = False
        self.sent_appelation = False
        self.filename = filename
        self.company = company
        self.limit = 0
        self.flood_wait_until: datetime | None = None
        self.rest_until: datetime | None = None
        self.story_likes_today = 0
        self.stopping = False

def init_db():
    conn = sqlite3.connect("database.db", check_same_thread=False)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            link TEXT NOT NULL UNIQUE,
            last_processed TIMESTAMP,
            status TEXT DEFAULT 'pending'  -- 'pending', 'processing', 'processed'
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            last_processed TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS blacklist (
            username TEXT NOT NULL,
            company TEXT NOT NULL,
            PRIMARY KEY (username, company)
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_channels_status ON channels(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_processed_users_last ON processed_users(last_processed)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_blacklist_username_company ON blacklist(username, company)")

    conn.commit()
    return conn

conn = init_db()

def is_user_in_blacklist(username: str, company: str) -> bool:
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM blacklist WHERE username = ? AND company = ?", (username, company))
    return cursor.fetchone() is not None

def load_sessions():
    global sessions
    sessions = []
    for company in company_configs.keys():
        sessions_dir = f"./companies/{company}/sessions"
        if os.path.exists(sessions_dir):
            for filename in os.listdir(sessions_dir):
                if filename.endswith('.session'):
                    full_path = os.path.join(sessions_dir, filename)
                    client = make_client(full_path)
                    session = Session(client, filename, company)
                    sessions.append(session)
    log_msg = f"Загружено {len(sessions)} сессий для проверки."
    rootLogger.info(log_msg)
    write_daily_log(log_msg)

def load_channels_to_db(company):
    cursor = conn.cursor()
    total_channels = 0
    
    users_txt_path = f"./companies/{company}/channels.txt"
    if path.exists(users_txt_path):
        with open(users_txt_path, "r", encoding='utf-8') as users_file:
            channels = [line.strip() for line in users_file.read().replace("\r", "").replace("https://t.me/", "@").split("\n") if line.strip()]
            total_channels += len(channels)
            for channel in channels:
                cursor.execute("""
                    INSERT OR IGNORE INTO channels (link, status, last_processed)
                    VALUES (?, 'pending', NULL)
                """, (channel,))

    users_sub_txt_path = f"./companies/{company}/channels_sub.txt"
    if path.exists(users_sub_txt_path):
        with open(users_sub_txt_path, "r", encoding='utf-8') as users_sub_file:
            channels = [line.strip() for line in users_sub_file.read().replace("\r", "").replace("https://t.me/", "@").split("\n") if line.strip()]
            total_channels += len(channels)
            for channel in channels:
                cursor.execute("""
                    INSERT OR IGNORE INTO channels (link, status, last_processed)
                    VALUES (?, 'processed', ?)
                """, (channel, datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S.%f")))
    
    conn.commit()
    rootLogger.info(f"Загружено {total_channels} каналов для компании {company} в базу данных")

with open(f"proxies.txt", "r", encoding='utf-8') as proxy_file:
    global proxies
    proxy_content = proxy_file.read().replace("\r", "")
    proxies = list(map(lambda x: ("http", *x.split(":")), proxy_content.split("\n")))

for company_path in listdir("./companies"):
    if path.exists(f"./companies/{company_path}/config.json"):
        with open(f"./companies/{company_path}/config.json", "r", encoding='utf-8') as config_file:
            company_configs[company_path] = json.load(config_file)
    else:
        company_configs[company_path] = default_company_config

for company_path in listdir("./companies"):
    company_stats[company_path] = {
        "stories_viewed": 0,
        "likes_set": 0,
        "unique_users": set(),
        "channels_processed": 0,
        "chats_processed": 0,
        "unique_users_with_stories": set()
    }
    stats_file = f"./companies/{company_path}/company_stats.json"
    if os.path.exists(stats_file):
        with open(stats_file, "r", encoding='utf-8') as f:
            stats = json.load(f)
            company_stats[company_path] = {
                "stories_viewed": stats["stories_viewed"],
                "likes_set": stats["likes_set"],
                "unique_users": set(stats["unique_users"]),
                "channels_processed": stats["channels_processed"],
                "chats_processed": stats["chats_processed"],
                "unique_users_with_stories": set(stats.get("unique_users_with_stories", []))
            }
        rootLogger.info(f"Загружена статистика для компании {company_path} из {stats_file}")
    else:
        rootLogger.info(f"Файл статистики для компании {company_path} не найден, инициализируем с нуля")

    load_channels_to_db(company_path)


def save_stats(company):
    stats = company_stats.get(company, {
        "stories_viewed": 0,
        "likes_set": 0,
        "unique_users": set(),
        "channels_processed": 0,
        "chats_processed": 0,
        "unique_users_with_stories": set()
    })
    stats_file = f"./companies/{company}/company_stats.json"
    try:
        data_to_save = {
            "stories_viewed": stats["stories_viewed"],
            "likes_set": stats["likes_set"],
            "unique_users": list(stats["unique_users"]),
            "channels_processed": stats["channels_processed"],
            "chats_processed": stats["chats_processed"],
            "unique_users_with_stories": list(stats["unique_users_with_stories"])
        }
        rootLogger.info(f"Сохранение для {company}: "
                       f"stories_viewed={stats['stories_viewed']}, "
                       f"likes_set={stats['likes_set']}, "
                       f"unique_users_count={len(stats['unique_users'])}, "
                       f"unique_users_with_stories={len(stats['unique_users_with_stories'])}, "
                       f"channels_processed={stats['channels_processed']}, "
                       f"chats_processed={stats['chats_processed']}")
        with open(stats_file, "w", encoding='utf-8') as f:
            json.dump(data_to_save, f, ensure_ascii=False)
        rootLogger.info(f"Успешно сохранено для {company} в {stats_file}")
    except Exception as e:
        rootLogger.error(f"Ошибка сохранения для {company}: {str(e)}")

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "back_to_channel"))
async def back_to_channel(callback: atypes.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Выберите действие ниже:", reply_markup=kb_change_settings)
    await callback.answer()

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "show_stats"))
async def handle_show_stats(callback: atypes.CallbackQuery):
    await show_stats(callback)

async def show_stats(callback: atypes.CallbackQuery):
    global selected_company
    if selected_company is None:
        await callback.message.edit_text("Сначала выберите компанию!")
        return
    stats_text = f"Статистика для {selected_company}:\n" + get_stats_text(selected_company).replace("\n\nВыберите действие ниже:", "")
    await callback.message.edit_text(stats_text)
    await callback.answer()



def write_daily_log(message):
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = f"daily_log_{today}.txt"
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%H:%M:%S')}] {message}\n")

def get_stats_text(company):
    stats = company_stats.get(company, {
        "stories_viewed": 0,
        "likes_set": 0,
        "unique_users": set(),
        "channels_processed": 0,
        "chats_processed": 0,
        "unique_users_with_stories": set()
    })
    return (
        f"Сторис просмотрено: {stats['stories_viewed']}\n"
        f"Лайков поставлено: {stats['likes_set']}\n"
        f"Уникальных пользователей открыто: {len(stats['unique_users'])}\n"
        f"Уникальных пользователей со сторисами: {len(stats['unique_users_with_stories'])}\n"
        f"Каналов пройдено: {stats['channels_processed']}\n"
        f"Чатов пройдено: {stats['chats_processed']}\n\n"
        "Выберите действие ниже:"
    )

def get_all_stats():
    stats = []
    total_accounts = 0

    for company in company_configs.keys():
        company_sessions = list(filter(lambda x: x.me is not None and x.company == company, sessions))
        company_deactivated = list(filter(lambda x: x.me is None and x.company == company, sessions))
        accounts_count = len(company_sessions) + len(company_deactivated)
        stats.append(f"Компания: {company}")
        stats.append(f"Аккаунтов: {accounts_count}")
        stats.append(f"Лайков сторис сегодня: {sum(s.story_likes_today for s in company_sessions)}")
        stats.append(f"Сторис просмотрено: {company_stats[company]['stories_viewed']}")
        stats.append(f"Лайков поставлено: {company_stats[company]['likes_set']}")
        stats.append(f"Уникальных пользователей открыто: {len(company_stats[company]['unique_users'])}")
        stats.append(f"Уникальных пользователей со сторисами: {len(company_stats[company]['unique_users_with_stories'])}")  # Новый счётчик
        stats.append(f"Каналов пройдено: {company_stats[company]['channels_processed']}")
        stats.append(f"Чатов пройдено: {company_stats[company]['chats_processed']}")
        stats.append("")
        total_accounts += accounts_count

    stats.append("Общая статистика по всем компаниям:")
    stats.append(f"Аккаунтов: {total_accounts}")
    stats.append(f"Лайков сторис сегодня: {sum(s.story_likes_today for s in sessions if s.me)}")
    stats.append(f"Сторис просмотрено: {sum(stats['stories_viewed'] for stats in company_stats.values())}")
    stats.append(f"Лайков поставлено: {sum(stats['likes_set'] for stats in company_stats.values())}")
    stats.append(f"Уникальных пользователей открыто: {len(set.union(*(stats['unique_users'] for stats in company_stats.values())))}")
    stats.append(f"Уникальных пользователей со сторисами: {len(set.union(*(stats['unique_users_with_stories'] for stats in company_stats.values())))}")  # Новый счётчик
    stats.append(f"Каналов пройдено: {sum(stats['channels_processed'] for stats in company_stats.values())}")
    stats.append(f"Чатов пройдено: {sum(stats['chats_processed'] for stats in company_stats.values())}")
    return "\n".join(stats)


def transliterate(text):
    result = ''
    for char in text.lower():
        result += TRANS_TABLE.get(char, char)
    return ''.join(c for c in result if c.isalpha() or c.isspace()).replace(" ", "")

def make_client(session) -> TelegramClient:
    proxy = random.choice(proxies) if proxies else None
    device = random.choice(DEVICES_INFO) if 'DEVICES_INFO' in globals() else {"model": "iPhone 13 Pro Max", "system_version": "18.2", "app_version": "11.2"}
    client = TelegramClient(
        session=session,
        api_id=api_id,
        api_hash=api_hash,
        proxy=proxy,
        device_model=device["model"],
        system_version=device["system_version"],
        app_version=device["app_version"],
        lang_code="ru",
        system_lang_code="ru-RU"
    )
    log_msg = f"Регистрация обработчиков для сессии: {session}"
    rootLogger.info(log_msg)
    write_daily_log(log_msg)
    # Регистрация обработчика для @spambot
    client.add_event_handler(on_spambot_message, events.NewMessage(chats="@spambot"))
    # Существующий обработчик для 178220800
    client.add_event_handler(on_spambot_message, events.NewMessage(chats=178220800))
    return client

async def switch_company(callback: atypes.CallbackQuery, company: str):
    global selected_company
    selected_company = company
    stats_file = f"./companies/{company}/company_stats.json"
    if os.path.exists(stats_file):
        with open(stats_file, "r", encoding='utf-8') as f:
            stats = json.load(f)
            company_stats[company] = {
                "stories_viewed": stats.get("stories_viewed", 0),
                "likes_set": stats.get("likes_set", 0),
                "unique_users": set(stats.get("unique_users", [])),
                "channels_processed": stats.get("channels_processed", 0),
                "chats_processed": stats.get("chats_processed", 0),
                "unique_users_with_stories": set(stats.get("unique_users_with_stories", []))
            }
    else:
        company_stats[company] = {
            "stories_viewed": 0,
            "likes_set": 0,
            "unique_users": set(),
            "channels_processed": 0,
            "chats_processed": 0,
            "unique_users_with_stories": set()
        }
    await callback.message.edit_text(f"Вы переключились на компанию: {company}")
    await callback.answer()

async def reconnect_session(session):
    try:
        if session.app.is_connected():
            await session.app.disconnect()
        await session.app.connect()
        rootLogger.info(f"Сессия {session.filename} успешно переподключена")
        return True
    except Exception as e:
        rootLogger.error(f"Ошибка переподключения сессии {session.filename}: {str(e)}")
        return False

async def determine_chat_type(session, chat_id):
    try:
        participants_count = (await session.app(functions.channels.GetFullChannelRequest(chat_id))).full_chat.participants_count
        return "open" if participants_count > 50 else "closed"
    except Exception as e:
        rootLogger.error(f"Не удалось определить тип чата {chat_id}: {str(e)}")
        return "closed"

async def parse_open_chat(session, chat_id):
    users = []
    try:
        async for participant in session.app.iter_participants(chat_id):
            if hasattr(participant, 'user') and participant.user and participant.user.id:
                users.append(participant.user.id)
        users = list(set(users))
        rootLogger.info(f"Собрано {len(users)} уникальных пользователей из открытого чата {chat_id}")
    except Exception as e:
        rootLogger.error(f"Ошибка парсинга открытого чата {chat_id}: {str(e)}")
    return users

async def parse_closed_chat(session, chat_id):
    users = set()
    try:
        async for message in session.app.iter_messages(chat_id):
            try:
                if message.from_id and isinstance(message.from_id, types.PeerUser):
                    users.add(message.from_id.user_id)
            except Exception as e:
                rootLogger.error(f"Ошибка обработки сообщения в чате {chat_id}: {str(e)}")
                continue
        rootLogger.info(f"Собрано {len(users)} уникальных пользователей из истории чата {chat_id}")
    except FloodWaitError as e:
        rootLogger.warning(f"FloodWait при парсинге чата {chat_id}: ждём {e.seconds} секунд")
        await asyncio.sleep(e.seconds)
        return await parse_closed_chat(session, chat_id)
    except Exception as e:
        rootLogger.error(f"Ошибка парсинга закрытого чата {chat_id}: {str(e)}")
    return list(users)

async def process_channel(session, channel_id):
    try:
        channel = await session.app.get_entity(channel_id)
        full_channel = await session.app(functions.channels.GetFullChannelRequest(channel))
        linked_chat_id = full_channel.full_chat.linked_chat_id
        if linked_chat_id:
            chat_type = await determine_chat_type(session, linked_chat_id)
            users = await parse_open_chat(session, linked_chat_id) if chat_type == "open" else await parse_closed_chat(session, linked_chat_id)
            return users
        else:
            rootLogger.info(f"Канал {channel_id} не имеет привязанного чата")
            return []
    except Exception as e:
        rootLogger.error(f"Ошибка при обработке канала {channel_id}: {str(e)}")
        return []

async def process_user_stories(session, user_id):
    company = session.company
    cursor = conn.cursor()

    cursor.execute("""
        SELECT last_processed
        FROM processed_users
        WHERE user_id = ?
    """, (user_id,))
    result = cursor.fetchone()

    if result and result[0]:
        last_processed = datetime.strptime(result[0], "%Y-%m-%d %H:%M:%S.%f")
        last_processed = last_processed.replace(tzinfo=pytz.UTC)
        if (datetime.now(pytz.UTC) - last_processed).total_seconds() < 24 * 3600:
            rootLogger.info(f"Пользователь {user_id} уже обработан менее 24 часов назад, пропускаем")
            return

    try:
        try:
            user = await session.app.get_entity(user_id)
            peer = await session.app.get_input_entity(user)
        except ValueError:
            return

        user_display = f"@{user.username}" if user.username else f"ID {user_id}"

        # Проверка на чёрный список
        if user.username and is_user_in_blacklist(user.username, company):
            rootLogger.info(f"Пользователь {user_display} находится в чёрном списке компании {company}, пропускаем")
            return

        async with stats_lock:
            company_stats[company]["unique_users"].add(user_id)
            rootLogger.info(f"Добавлен пользователь {user_display} в unique_users для компании {company}, всего: {len(company_stats[company]['unique_users'])}")
        
        peer_stories = await session.app(GetPeerStoriesRequest(peer=peer))
        if not peer_stories.stories or not peer_stories.stories.stories:
            rootLogger.info(f"У пользователя {user_display} нет сторис")
            cursor.execute("""
                INSERT OR REPLACE INTO processed_users (user_id, username, last_processed)
                VALUES (?, ?, ?)
            """, (user_id, user.username, datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S.%f")))
            conn.commit()
            return
        
        stories = peer_stories.stories.stories
        rootLogger.info(f"Пользователь {user_display} имеет {len(stories)} сторис")
        
        async with stats_lock:
            company_stats[company]["unique_users_with_stories"].add(user_id)
            rootLogger.info(f"Добавлен пользователь {user_display} в unique_users_with_stories для компании {company}, всего: {len(company_stats[company]['unique_users_with_stories'])}")
        
        for story in stories:
            try:
                await session.app(ReadStoriesRequest(peer=peer, max_id=story.id))
                rootLogger.info(f"Сторис {story.id} от {user_display} просмотрена")
                log_message = f"Аккаунт {session.filename} (компания {company}) просмотрел сторис {story.id} у {user_display}"
                rootLogger.info(log_message)
                write_daily_log(log_message)
                
                async with stats_lock:
                    company_stats[company]["stories_viewed"] += 1
                
                if random.random() < 0.8:
                    if company_stats[company]["likes_set"] % LIKES_PER_ACCOUNT == 0:
                        await asyncio.sleep(LIKES_WAIT_SECONDS)

                    reaction = random.choice(POSITIVE_REACTIONS)
                    await session.app(SendReactionRequest(
                        peer=peer,
                        story_id=story.id,
                        reaction=ReactionEmoji(emoticon=reaction)
                    ))
                    rootLogger.info(f"Лайкнута сторис {story.id} от {user_display} с {reaction}")
                    log_message = f"Аккаунт {session.filename} (компания {company}) лайкнул сторис {story.id} у {user_display} с {reaction}"
                    rootLogger.info(log_message)
                    write_daily_log(log_message)

                    async with stats_lock:
                        company_stats[company]["likes_set"] += 1
                        
                else:
                    rootLogger.info(f"Сторис {story.id} от {user_display} просто просмотрена")
                    log_message = f"Сторис {story.id} от {user_display} просто просмотрена"
                    rootLogger.info(log_message)
                    write_daily_log(log_message)
                
                await asyncio.sleep(random.uniform(1, 2))
            except FloodWaitError as e:
                log_msg = f"FloodWait при обработке сторис {story.id} от {user_display}: ждём {e.seconds} секунд"
                rootLogger.warning(log_msg)
                write_daily_log(log_msg)
                await bot.send_message(chat_id, log_msg)
                await asyncio.sleep(e.seconds)
                continue
            except Exception as e:
                rootLogger.error(f"Ошибка обработки сторис {story.id} от {user_display}: {str(e)}")
                continue

        cursor.execute("""
            INSERT OR REPLACE INTO processed_users (user_id, username, last_processed)
            VALUES (?, ?, ?)
        """, (user_id, user.username if user.username else None, datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S.%f")))
        conn.commit()

    except FloodWaitError as e:
        log_msg = f"FloodWait при получении сторис пользователя {user_display}: ждём {e.seconds} секунд"
        rootLogger.warning(log_msg)
        write_daily_log(log_msg)
        await bot.send_message(chat_id, log_msg)
        await asyncio.sleep(e.seconds)
        return await process_user_stories(session, user_id)
    except Exception as e:
        rootLogger.error(f"Ошибка при обработке сторис пользователя {user_display}: {str(e)}")


@dp.callback_query((F.from_user.id == owner_id) & (F.data == "add_chats"))
async def start_add_chats(callback: atypes.CallbackQuery, state: FSMContext):
    if not selected_company:
        await callback.message.edit_text("Сначала выберите компанию.")
        return
    await state.set_state(AddChatsState.chats)
    await callback.message.edit_text("Отправьте файл с ссылками на чаты/каналы (одна ссылка на строку) или список ссылок текстом:")
    await callback.answer()

@dp.message(AddChatsState.chats, (F.from_user.id == owner_id) & (F.document | F.text))
async def add_chats_file(message: atypes.Message, state: FSMContext):
    company = selected_company
    if message.document:
        file = await bot.get_file(message.document.file_id)
        file_data = await bot.download_file(file.file_path)
        new_chats = file_data.getvalue().decode('utf-8').splitlines()
    elif message.text:
        new_chats = message.text.splitlines()
    else:
        await message.reply("Отправьте файл или текст с ссылками.")
        return
    
    added_count = 0
    cursor = conn.cursor()
    for chat in new_chats:
        chat = chat.strip()
        if chat:
            if chat.startswith("https://t.me/+"):
                chat = chat.replace("https://t.me/", "t.me/")
            elif chat.startswith("https://t.me/"):
                chat = chat.replace("https://t.me/", "@")
            elif chat.startswith("t.me/") and not chat.startswith("t.me/+"):
                chat = "@" + chat.split("t.me/")[1]
            if chat:
                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO channels (link, status, last_processed)
                        VALUES (?, 'pending', NULL)
                    """, (chat,))
                    if cursor.rowcount > 0:
                        added_count += 1
                except Exception as e:
                    rootLogger.error(f"Ошибка добавления канала {chat} в базу: {str(e)}")
    
    conn.commit()
    await state.clear()
    await message.reply(f"Добавлено {added_count} новых чатов/каналов в компанию {company}")
    await start(message)

async def join_by_invite_link(session, invite_link):
    try:
        if invite_link.startswith("t.me/+"):
            invite_hash = invite_link.split("+")[1]
        elif invite_link.startswith("https://t.me/+"):
            invite_hash = invite_link.split("+")[1]
        else:
            raise ValueError("Неверный формат пригласительной ссылки")

        result = await session.app(functions.messages.ImportChatInviteRequest(invite_hash))
        chat = result.chats[0]
        rootLogger.info(f"Аккаунт {session.filename} вступил в чат {chat.id} по пригласительной ссылке {invite_link}")
        return chat.id
    except (UserNotParticipantError, ForbiddenError, ValueError) as e:
        error_msg = str(e)
        if "No user has" in error_msg or "You're banned from sending messages" in error_msg or "An invalid Peer was used" in error_msg:
            log_msg = f"Ошибка при вступлении в чат {invite_link} для {session.filename}: {error_msg}. Проверяем статус через @spambot..."
            rootLogger.warning(log_msg)
            write_daily_log(log_msg)
            status, reason = await check_session_status(session)
            log_msg = f"Статус сессии {session.filename} после проверки: {status} (причина: {reason or 'нет'})"
            rootLogger.info(log_msg)
            write_daily_log(log_msg)
            if status == "красный":
                session_path = f"./companies/{session.company}/sessions/{session.filename}"
                await move_to_spamblocked(session_path)
                sessions.remove(session)
                log_msg = f"Сессия {session.filename} перемещена в ./spamblocked_sessions (статус: {status}, причина: {reason})"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)
                await bot.send_message(chat_id, log_msg)
            return None
        else:
            rootLogger.error(f"Не удалось вступить в чат по ссылке {invite_link}: {error_msg}")
            return None
    except Exception as e:
        rootLogger.error(f"Не удалось вступить в чат по ссылке {invite_link}: {str(e)}")
        return None
    
async def periodic_save_stats():
    while True:
        try:
            for company in company_stats.keys():
                async with stats_lock:
                    save_stats(company)
            await asyncio.sleep(300)
        except Exception as e:
            rootLogger.error(f"Ошибка в periodic_save_stats: {str(e)}")
            await asyncio.sleep(300)


async def worker_liking_stories(session: Session):
    company = session.company
    rootLogger.info(f"Запуск лайкинга сторис для {session.filename}, компания {company}")

    try:
        while True:
            rootLogger.info(f"[DEBUG] Состояние компании сессии {session.filename}: {company_active.get(company, False)}")
            if not company_active.get(company, False):
                rootLogger.info(f"Лайкинг для {session.filename} остановлен, ждём активации...")
                await asyncio.sleep(60)
                continue

            if not session.app.is_connected():
                log_msg = f"Клиент для сессии {session.filename} не подключён, пытаемся переподключиться..."
                rootLogger.warning(log_msg)
                write_daily_log(log_msg)
                try:
                    await session.app.connect()
                    log_msg = f"Клиент для сессии {session.filename} успешно переподключён"
                    rootLogger.info(log_msg)
                    write_daily_log(log_msg)
                except Exception as e:
                    log_msg = f"Не удалось переподключить клиент для сессии {session.filename}: {str(e)}"
                    rootLogger.error(log_msg)
                    write_daily_log(log_msg)
                    await asyncio.sleep(60)
                    continue

            cursor = conn.cursor()
            cursor.execute("""
                UPDATE channels
                SET status = 'processing'
                WHERE id = (
                    SELECT id
                    FROM channels
                    WHERE status = 'pending'
                    LIMIT 1
                )
                RETURNING link
            """)
            result = cursor.fetchone()
            conn.commit()

            if not result:
                cursor.execute("""
                    UPDATE channels
                    SET status = 'pending'
                    WHERE status = 'processed'
                    AND last_processed < ?
                """, ((datetime.now(pytz.UTC) - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S.%f"),))
                conn.commit()

                cursor.execute("""
                    UPDATE channels
                    SET status = 'processing'
                    WHERE id = (
                        SELECT id
                        FROM channels
                        WHERE status = 'pending'
                        LIMIT 1
                    )
                    RETURNING link
                """)
                result = cursor.fetchone()
                conn.commit()

            if not result:
                rootLogger.info(f"Нет доступных чатов для обработки для {session.filename}, ждём...")
                await asyncio.sleep(300)
                continue

            chat = result[0]
            try:
                chat_id = None
                chat_link = None
                is_invite_link = chat.startswith("t.me/+") or chat.startswith("https://t.me/+")

                if is_invite_link:
                    chat_id = await join_by_invite_link(session, chat)
                    chat_link = chat
                    if not chat_id:
                        continue
                else:
                    try:
                        entity = await session.app.get_entity(chat)
                        if not entity:
                            raise ValueError("Entity is None")
                        chat_id = entity.id
                        chat_link = f"@{entity.username}" if entity.username else f"t.me/c/{chat_id}"
                        rootLogger.info(f"Получены данные канала {chat_link} без подписки")
                    except (ChannelPrivateError, UserNotParticipantError) as e:
                        log_msg = f"Канал {chat} закрытый или требует подписки, пропускаем: {str(e)}"
                        rootLogger.info(log_msg)
                        write_daily_log(log_msg)
                        cursor.execute("DELETE FROM channels WHERE link = ?", (chat,))
                        conn.commit()
                        await asyncio.sleep(20)
                        continue
                    except ValueError as e:
                        error_msg = str(e)
                        if "No user has" in error_msg or "An invalid Peer was used" in error_msg:
                            log_msg = f"Ошибка при доступе к {chat} для {session.filename}: {error_msg}. Проверяем статус через @spambot..."
                            rootLogger.warning(log_msg)
                            write_daily_log(log_msg)
                            status, reason = await check_session_status(session)
                            log_msg = f"Статус сессии {session.filename} после проверки: {status} (причина: {reason or 'нет'})"
                            rootLogger.info(log_msg)
                            write_daily_log(log_msg)
                            if status == "красный":
                                session_path = f"./companies/{session.company}/sessions/{session.filename}"
                                await move_to_spamblocked(session_path)
                                sessions.remove(session)
                                log_msg = f"Сессия {session.filename} перемещена в ./spamblocked_sessions (статус: {status}, причина: {reason})"
                                rootLogger.info(log_msg)
                                write_daily_log(log_msg)
                                await bot.send_message(chat_id, log_msg)
                                return  # Завершаем задачу
                            await asyncio.sleep(20)
                            continue
                        else:
                            log_msg = f"Не удалось получить доступ к {chat}: {str(e)}"
                            rootLogger.info(log_msg)
                            write_daily_log(log_msg)
                            if "chat not found" in str(e).lower():
                                cursor.execute("DELETE FROM channels WHERE link = ?", (chat,))
                                conn.commit()
                            if chat.startswith('@'):
                                async with stats_lock:
                                    company_stats[company]["channels_processed"] += 1
                            else:
                                async with stats_lock:
                                    company_stats[company]["chats_processed"] += 1
                            await asyncio.sleep(20)
                            continue

                rootLogger.info(f"Перешёл в чат {chat_id} по ссылке {chat_link}")

                users_to_process = []
                entity = await session.app.get_entity(chat_id)
                entity_type = await determine_entity_type(session, entity)
                if entity_type == "channel":
                    users_to_process = await process_channel(session, chat_id)
                    async with stats_lock:
                        company_stats[company]["channels_processed"] += 1
                elif entity_type == "chat":
                    chat_type = await determine_chat_type(session, chat_id)
                    users_to_process = await parse_open_chat(session, chat_id) if chat_type == "open" else await parse_closed_chat(session, chat_id)
                    async with stats_lock:
                        company_stats[company]["chats_processed"] += 1
                else:
                    rootLogger.error(f"Неизвестный тип сущности для {chat}, пропускаем")
                    await asyncio.sleep(20)
                    continue

                processed_users = 0
                for user_id in users_to_process:
                    if not company_active.get(company, False):
                        break
                    try:
                        await process_user_stories(session, user_id)
                        processed_users += 1
                        await asyncio.sleep(random.uniform(1, 2))
                    except ValueError as e:
                        error_msg = str(e)
                        if "No user has" in error_msg or "An invalid Peer was used" in error_msg:
                            log_msg = f"Ошибка при обработке сторис пользователя {user_id} для {session.filename}: {error_msg}. Проверяем статус через @spambot..."
                            rootLogger.warning(log_msg)
                            write_daily_log(log_msg)
                            status, reason = await check_session_status(session)
                            log_msg = f"Статус сессии {session.filename} после проверки: {status} (причина: {reason or 'нет'})"
                            rootLogger.info(log_msg)
                            write_daily_log(log_msg)
                            if status == "красный":
                                session_path = f"./companies/{session.company}/sessions/{session.filename}"
                                await move_to_spamblocked(session_path)
                                sessions.remove(session)
                                log_msg = f"Сессия {session.filename} перемещена в ./spamblocked_sessions (статус: {status}, причина: {reason})"
                                rootLogger.info(log_msg)
                                write_daily_log(log_msg)
                                await bot.send_message(chat_id, log_msg)
                                return  # Завершаем задачу
                            await asyncio.sleep(10)
                            break
                        else:
                            log_msg = f"Неизвестная ошибка при обработке сторис пользователя {user_id} для {session.filename}: {error_msg}"
                            rootLogger.error(log_msg)
                            write_daily_log(log_msg)
                            await asyncio.sleep(5)

                log_msg = f"Аккаунт {session.filename} завершил обработку чата {chat_link}: обработано {processed_users} пользователей."
                rootLogger.info(log_msg)
                write_daily_log(log_msg)
                await bot.send_message(chat_id, log_msg)

                cursor.execute("""
                    UPDATE channels
                    SET status = 'processed', last_processed = ?
                    WHERE link = ?
                """, (datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S.%f"), chat))
                conn.commit()

                await asyncio.sleep(8)

            except Exception as e:
                rootLogger.error(f"Ошибка при обработке {chat} для {session.filename}: {str(e)}")
                if "chat not found" in str(e).lower():
                    cursor.execute("DELETE FROM channels WHERE link = ?", (chat,))
                    conn.commit()
                await asyncio.sleep(60)
    except asyncio.CancelledError:
        rootLogger.info(f"Лайкинг для {session.filename} отменён")
        if session.app and session.app.is_connected():
            await session.app.disconnect()
            if hasattr(session.app, '_sender') and session.app._sender:
                await session.app._sender.disconnect()
        raise
    except Exception as e:
        rootLogger.error(f"Критическая ошибка в worker_liking_stories для {session.filename}: {str(e)}")
        if session.app and session.app.is_connected():
            await session.app.disconnect()
            if hasattr(session.app, '_sender') and session.app._sender:
                await session.app._sender.disconnect()

async def remove_inactive_sessions():
    global sessions
    now = datetime.now(pytz.UTC)
    inactive_sessions = []
    
    for session in sessions:
        try:
            if not session.app.is_connected():
                await session.app.connect()
            me = await session.app.get_me()
            if not me:
                inactive_sessions.append(session)
                continue
            if me.status and hasattr(me.status, 'was_online'):
                last_online = me.status.was_online
                if last_online and (now - last_online).days > 7:
                    inactive_sessions.append(session)
        except Exception as e:
            rootLogger.error(f"Ошибка проверки сессии {session.filename}: {str(e)}")
            inactive_sessions.append(session)
    
    for session in inactive_sessions:
        try:
            session_path = f"./companies/{session.company}/sessions/{session.filename}"
            await move_to_banned(session_path)
            sessions.remove(session)
            rootLogger.info(f"Сессия {session.filename} удалена как неактивная")
            await bot.send_message(chat_id, f"Сессия {session.filename} удалена как неактивная")
        except Exception as e:
            rootLogger.error(f"Ошибка удаления неактивной сессии {session.filename}: {str(e)}")

async def add_to_contacts(session, username):
    try:
        user = await session.app.get_entity(username)
        await session.app(functions.contacts.AddContactRequest(
            id=user.id,
            first_name=user.first_name or "",
            last_name=user.last_name or "",
            phone=user.phone or ""
        ))
        log_msg = f"Пользователь {username} добавлен в контакты сессии {session.filename}"
        rootLogger.info(log_msg)
        write_daily_log(log_msg)
    except Exception as e:
        log_msg = f"Ошибка добавления {username} в контакты для {session.filename}: {str(e)}"
        rootLogger.error(log_msg)
        write_daily_log(log_msg)

@dp.message(SendPMState.username, (F.from_user.id == owner_id))
async def process_pm_username(message: atypes.Message, state: FSMContext):
    username = message.text.strip()
    if not username:
        await message.reply("Юзернейм или ID не может быть пустым.")
        return
    await state.clear()
    
    ses = [s for s in sessions if s.me and s.company == selected_company]
    if not ses:
        await message.reply(f"В компании {selected_company} нет активных сессий.")
        return
    
    await message.reply(f"Отправка сообщения '{username}' от {len(ses)} аккаунтов началась...")
    
    success_count = 0
    for session in ses:
        try:
            await session.app.send_message(username, "1")
            log_msg = f"Аккаунт {session.filename} отправил сообщение пользователю {username}"
            rootLogger.info(log_msg)
            write_daily_log(log_msg)
            success_count += 1
            await add_to_contacts(session, username)
        except FloodWaitError as e:
            log_msg = f"FloodWait при отправке от {session.filename} к {username}: ждём {e.seconds} секунд"
            rootLogger.warning(log_msg)
            write_daily_log(log_msg)
            await bot.send_message(chat_id, log_msg)
            await asyncio.sleep(e.seconds)
            continue
        except Exception as e:
            log_msg = f"Ошибка отправки от {session.filename}: {str(e)}"
            rootLogger.error(log_msg)
            write_daily_log(log_msg)
            await bot.send_message(chat_id, log_msg)
        
        await asyncio.sleep(1)
    
    log_msg = f"Отправка завершена для {success_count} из {len(ses)} аккаунтов."
    rootLogger.info(log_msg)
    write_daily_log(log_msg)
    await bot.send_message(chat_id, log_msg)
    await message.reply(f"Отправка завершена для {len(ses)} аккаунтов.")
    await start(message)

async def start(message: atypes.Message):
    global selected_company
    log_msg = f"Запуск команды /start. Текущая компания: {selected_company}"
    rootLogger.info(log_msg)
    write_daily_log(log_msg)
    ses = list(filter(lambda x: x.me is not None and x.company == selected_company, sessions)) if selected_company else []
    log_msg = f"Найдено активных сессий для компании {selected_company}: {len(ses)}"
    rootLogger.info(log_msg)
    write_daily_log(log_msg)
    
    if selected_company is None:
        stats = get_all_stats()
        kb_companies = atypes.InlineKeyboardMarkup(inline_keyboard=list(
            map(lambda x: [atypes.InlineKeyboardButton(text=x, callback_data="sel_company_" + x)],
                listdir("./companies"))
            ) + [[atypes.InlineKeyboardButton(text="Создать компанию", callback_data="create_company")],
                 [atypes.InlineKeyboardButton(text="Статистика аккаунтов", callback_data="account_stats")]])
        await message.reply(f"Общая статистика по всем компаниям:\n{stats}\n\nВыберите компанию:", reply_markup=kb_companies)
    else:
        await message.reply("Выберите действие ниже:", reply_markup=kb_menu)

async def update_stats_message(chat_id, message_id, company):
    while True:
        try:
            async with stats_lock:
                stats_text = get_stats_text(company)
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=stats_text,
                reply_markup=kb_menu
            )
            await asyncio.sleep(120)
        except FloodWaitError as e:
            rootLogger.warning(f"FloodWait при обновлении статистики: ждём {e.seconds} секунд")
            await bot.send_message(chat_id, f"FloodWait при обновлении статистики: ждём {e.seconds} секунд")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            if "message is not modified" not in str(e).lower():
                rootLogger.error(f"Ошибка обновления статистики: {str(e)}")
            await asyncio.sleep(60)

@dp.message(CollectViewsStatsState.channel_name, (F.from_user.id == owner_id))
async def process_channel_name(message: atypes.Message, state: FSMContext):
    await state.clear()
    await message.reply("Сбор статистики по привязанным каналам начался...")
    await collect_created_views_stats(message)
    await start(message)

async def determine_entity_type(session, entity):
    try:
        if isinstance(entity, types.Chat):
            return "chat"
        elif isinstance(entity, types.Channel):
            if entity.broadcast:
                return "channel"
            elif entity.megagroup:
                return "chat"
            else:
                participant = await session.app(functions.channels.GetParticipantRequest(
                    channel=entity,
                    participant=await session.app.get_me()
                ))
                if participant.participant._constructor_id == types.ChannelParticipant._constructor_id:
                    return "chat"
                return "channel"
        return "unknown"
    except Exception as e:
        rootLogger.error(f"Ошибка определения типа сущности {entity.id}: {str(e)}")
        return "unknown"

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "send_pm"))
async def start_send_pm(callback: atypes.CallbackQuery, state: FSMContext):
    if not selected_company:
        await callback.message.edit_text("Сначала выберите компанию.")
        return
    await state.set_state(SendPMState.username)
    await callback.message.edit_text("Отправьте юзернейм или ID пользователя:")
    await callback.answer()

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "check_account_status"))
async def check_account_status(callback: atypes.CallbackQuery):
    if not selected_company:
        await callback.message.edit_text("Сначала выберите компанию.")
        return
    log_msg = f"Проверка состояния аккаунтов для компании {selected_company}"
    rootLogger.info(log_msg)
    write_daily_log(log_msg)
    company_sessions = [s for s in sessions if s.company == selected_company and s.app]
    if not company_sessions:
        await callback.message.edit_text(f"В компании {selected_company} нет активных сессий.")
        return
    await callback.message.edit_text(f"Проверка состояния аккаунтов для компании {selected_company}...")
    checked_count = 0
    green_count = 0
    yellow_count = 0
    red_count = 0
    for session in company_sessions:
        status, reason = await check_session_status(session)
        checked_count += 1
        log_msg = f"Статус сессии {session.filename} после проверки: {status} (причина: {reason or 'нет'})"
        rootLogger.info(log_msg)
        write_daily_log(log_msg)
        if status == "зелёный":
            green_count += 1
        elif status == "жёлтый":
            yellow_count += 1
        elif status == "красный":
            red_count += 1
            session_path = f"./companies/{session.company}/sessions/{session.filename}"
            await move_to_spamblocked(session_path)
            sessions.remove(session)
            log_msg = f"Сессия {session.filename} перемещена в ./spamblocked_sessions (статус: {status}, причина: {reason})"
            rootLogger.info(log_msg)
            write_daily_log(log_msg)
            await bot.send_message(chat_id, log_msg)
    # Формируем и отправляем статистику
    stats_msg = (f"Проверка завершена для компании {selected_company}:\n"
                 f"- Проверено аккаунтов: {checked_count}\n"
                 f"- Зелёных: {green_count}\n"
                 f"- Жёлтых: {yellow_count}\n"
                 f"- Красных: {red_count}")
    rootLogger.info(stats_msg)
    write_daily_log(stats_msg)
    await bot.send_message(chat_id, stats_msg)
    await callback.message.edit_text(stats_msg)

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "toggle_story_liking"))
async def toggle_story_liking(callback: atypes.CallbackQuery, state: FSMContext):
    global selected_company, company_active, liking_tasks, sessions
    if not selected_company:
        await callback.message.edit_text("Сначала выберите компанию с помощью /select_company.")
        return

    # Сохраняем начальное состояние выбранных компаний
    await state.set_state(CompanyToggleState.selected_companies)
    await state.update_data(selected_companies=list(company_active.keys()))  # Изначально все компании

    # Формируем клавиатуру
    keyboard = []
    for company in company_configs.keys():
        ses = [s for s in sessions if s.company == company and s.me]
        is_liking = company_active.get(company, False)
        status = "✅" if is_liking else "❌"
        button_text = f"{status} {company}"
        button = InlineKeyboardButton(
            text=button_text,
            callback_data=CompanyToggleCallback(action="toggle", company_name=company).pack()
        )
        keyboard.append([button])

    done_button = InlineKeyboardButton(text="Готово", callback_data="done_toggling")
    keyboard.append([done_button])

    reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    await callback.message.edit_text("Выберите компании для переключения лайкинга сторис:", reply_markup=reply_markup)

# Обработчик переключения статуса компании
@dp.callback_query((F.from_user.id == owner_id) & (F.data.startswith("company_toggle:")))
async def process_company_toggle(callback: atypes.CallbackQuery, state: FSMContext):
    global company_active, liking_tasks, sessions
    try:
        callback_data = CompanyToggleCallback.unpack(callback.data)
        company_name = callback_data.company_name
        action = callback_data.action
        if action == "toggle":
            is_liking = company_active.get(company_name, False)
            company_active[company_name] = not is_liking
            ses = [s for s in sessions if s.company == company_name and s.me]
            status = "✅" if company_active[company_name] else "❌"
            button_text = f"{status} {company_name}"
            await callback.message.edit_reply_markup(
                reply_markup=await build_toggle_keyboard(state)
            )
            await callback.answer(f"Лайкинг для {company_name} {'включён' if company_active[company_name] else 'выключен'}")
    except ValueError as e:
        await callback.answer("Ошибка обработки данных. Попробуйте снова.")
        return

async def build_toggle_keyboard(state: FSMContext):
    data = await state.get_data()
    selected_companies = data.get("selected_companies", [])
    keyboard = []
    for company in company_configs.keys():
        ses = [s for s in sessions if s.company == company and s.me]
        is_liking = company_active.get(company, False)
        status = "✅" if is_liking else "❌"
        button_text = f"{status} {company}"
        button = InlineKeyboardButton(
            text=button_text,
            callback_data=CompanyToggleCallback(action="toggle", company_name=company).pack()
        )
        keyboard.append([button])
    done_button = InlineKeyboardButton(text="Готово", callback_data="done_toggling")
    keyboard.append([done_button])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "toggle_story_liking"))
async def toggle_story_liking(callback: atypes.CallbackQuery):
    global selected_company, company_active, liking_tasks, sessions
    if not selected_company:
        await callback.message.edit_text("Сначала выберите компанию с помощью /select_company.")
        return
    reply_markup = await build_toggle_keyboard()
    await callback.message.edit_text("Выберите компании для переключения лайкинга сторис:", reply_markup=reply_markup)

@dp.callback_query(ChangeState.select, (F.from_user.id == owner_id) & (F.data.in_({"select_all", "select_selective"})))
async def sel_accs(callback: atypes.CallbackQuery, state: FSMContext):
    global sessions
    report_sent = False
    try:
        ses = list(filter(lambda x: x.me is not None and x.company == selected_company, sessions))
        if not ses:
            await bot.edit_message_text(
                chat_id=callback.message.chat.id,
                message_id=callback.message.message_id,
                text="Нет активных сессий для этой компании."
            )
            await state.clear()
            await start(callback.message)
            return
        data = await state.get_data()
        change = data.get("change", "")
        if callback.data == "select_all":
            await state.update_data(select=[int(session.me.id) for session in ses])
            log_msg = f"Выбраны все аккаунты для компании {selected_company}: {len(ses)}"
            rootLogger.info(log_msg)
            write_daily_log(log_msg)
            max_attempts = 1
            attempt = 0
            while attempt < max_attempts:
                try:
                    await apply_data(callback, state, change=change)
                    break
                except Exception as e:
                    attempt += 1
                    if attempt < max_attempts:
                        log_msg = f"Попытка {attempt}/{max_attempts}: Ошибка, повтор через 3 секунды: {str(e)}"
                        rootLogger.warning(log_msg)
                        write_daily_log(log_msg)
                        await asyncio.sleep(3)
                    else:
                        log_msg = f"Не удалось применить изменения после {max_attempts} попыток: {str(e)}"
                        rootLogger.error(log_msg)
                        write_daily_log(log_msg)
                        await bot.edit_message_text(
                            chat_id=callback.message.chat.id,
                            message_id=callback.message.message_id,
                            text="Ошибка при применении изменений."
                        )
            report_text = f"Действия применены к {len(ses)} {'сессии' if len(ses) == 1 else 'сессиям'}"
            attempt = 0
            while attempt < max_attempts:
                try:
                    await bot.edit_message_text(
                        chat_id=callback.message.chat.id,
                        message_id=callback.message.message_id,
                        text=report_text
                    )
                    report_sent = True
                    break
                except Exception as e:
                    attempt += 1
                    if attempt < max_attempts:
                        await asyncio.sleep(3)
                    else:
                        await bot.send_message(chat_id, f"Не удалось отправить отчёт: {str(e)}")
        elif callback.data == "select_selective":
            await state.update_data(select=[])
            builder = InlineKeyboardBuilder()
            for session in ses:
                me = session.me
                builder.row(atypes.InlineKeyboardButton(
                    text=f"❌ {me.first_name}{' ' + me.last_name if me.last_name else ''} ({'@' + me.username if me.username else '+' + me.phone_number if me.phone_number else str(me.id)} {me.id})",
                    callback_data=f"select_{me.id}"))
            builder.row(atypes.InlineKeyboardButton(text="✅ Готово", callback_data="select_done"))
            await bot.edit_message_text(
                chat_id=callback.message.chat.id,
                message_id=callback.message.message_id,
                text="Выберите аккаунты:",
                reply_markup=builder.as_markup()
            )
        await callback.answer()
    except Exception as e:
        log_msg = f"Критическая ошибка в sel_accs: {str(e)}"
        rootLogger.error(log_msg)
        write_daily_log(log_msg)
        if not report_sent:
            await bot.edit_message_text(
                chat_id=callback.message.chat.id,
                message_id=callback.message.message_id,
                text="Произошла критическая ошибка."
            )

async def move_to_spamblocked(session_path: str):
    filename = os.path.basename(session_path)
    spamblocked_path = os.path.join("./spamblocked_sessions", filename)
    max_attempts = 5
    attempt = 0
    session = next((s for s in sessions if f"./companies/{s.company}/sessions/{s.filename}" == session_path), None)
    if session and session.app and session.app.is_connected():
        await session.app.disconnect()
        if hasattr(session.app, '_sender') and session.app._sender:
            session.app._sender.disconnect()
        await asyncio.sleep(1)  # Даём время на закрытие файла

    while attempt < max_attempts:
        try:
            if not os.path.exists(session_path):
                log_msg = f"Ошибка: Файл сессии {filename} не найден"
                rootLogger.error(log_msg)
                write_daily_log(log_msg)
                await bot.send_message(chat_id, log_msg)
                return
            if not os.path.exists("./spamblocked_sessions"):
                os.makedirs("./spamblocked_sessions")
            shutil.move(session_path, spamblocked_path)
            log_msg = f"Сессия {filename} перемещена в ./spamblocked_sessions"
            rootLogger.info(log_msg)
            write_daily_log(log_msg)

            json_filename = filename.replace('.session', '.json')
            source_json_path = session_path.replace(filename, json_filename)
            target_json_path = os.path.join("./spamblocked_sessions", json_filename)
            if os.path.exists(source_json_path):
                shutil.move(source_json_path, target_json_path)
                log_msg = f"JSON-файл {json_filename} перемещён в ./spamblocked_sessions"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)
            return
        except Exception as e:
            attempt += 1
            if attempt < max_attempts:
                await asyncio.sleep(2)
            else:
                log_msg = f"Не удалось переместить сессию {filename}: {str(e)}"
                rootLogger.error(log_msg)
                write_daily_log(log_msg)
                await bot.send_message(chat_id, log_msg)

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "done_toggling"))
async def process_done_toggling(callback: atypes.CallbackQuery, state: FSMContext):
    global company_active, liking_tasks, sessions
    await state.clear()  # Очищаем состояние

    # Активируем масслукинг для всех компаний, где company_active == True
    for company in company_configs.keys():
        if company_active.get(company, False):
            ses = [s for s in sessions if s.company == company and s.me]
            for session in ses:
                if not any(task[1] == session for task in liking_tasks):  # Проверяем, не запущена ли задача
                    task = asyncio.create_task(worker_liking_stories(session))
                    liking_tasks.append((task, session))
                    rootLogger.info(f"Запущена задача масслукинга для сессии {session.filename} (компания {company})")

    await callback.message.edit_text("Масслукинг настроен и запущен для всех выбранных компаний.")
    await callback.answer()
    await start(callback.message)  # Возвращаемся к меню

async def check_session_status(session: Session) -> tuple[str, str | None]:
    if not session.app or not session.me:
        log_msg = f"Сессия {session.filename} не активирована"
        rootLogger.error(log_msg)
        write_daily_log(log_msg)
        return "красный", "banned"
    try:
        status, reason = await handle_spam_block(session.app)
        if status:
            log_msg = f"Сессия {session.filename} имеет статус: {status} (причина: {reason or 'нет'})"
            rootLogger.info(log_msg)
            write_daily_log(log_msg)
        return status, reason
    except Exception as e:
        log_msg = f"Ошибка при проверке состояния сессии {session.filename}: {str(e)}"
        rootLogger.error(log_msg)
        write_daily_log(log_msg)
        return "красный", "ошибка"

async def activate_session(session):
    try:
        await session.app.start(phone=f"+{session.filename[:-8]}")
        me = await session.app.get_me()
        if me:
            session.id = me.id
            session.me = me
            if not session.app.is_connected():
                await session.app.connect()
            log_msg = f"Сессия {session.filename} активирована с ID {me.id}"
            rootLogger.info(log_msg)
            write_daily_log(log_msg)
            return True
        else:
            raise Exception("Не удалось получить данные пользователя")
    except Exception as e:
        log_msg = f"Ошибка активации сессии {session.filename}: {str(e)}"
        rootLogger.error(log_msg)
        write_daily_log(log_msg)
        return False

async def set_bot_commands(bot: Bot):
    commands = [
        BotCommand(command="start", description="Начать работу с ботом"),
        BotCommand(command="stats_all", description="Показать статистику по всем компаниям"),
        BotCommand(command="cancel", description="Отменить текущую операцию"),
        BotCommand(command="blacklist", description="Добавить пользователей в чёрный список"),
        BotCommand(command="view_blacklist", description="Посмотреть чёрный список")
    ]
    await bot.set_my_commands(commands)
    rootLogger.info("Команды бота успешно установлены через setMyCommands")

async def main():
    for company_path in listdir("./companies"):
        users_paused[company_path] = True
        company_active[company_path] = False
        rootLogger.info(f"Компания {company_path} изначально неактивна для лайкинга сторис")

    company_files = listdir("./companies")
    for company_path in company_files:
        session_files = [f for f in listdir(f"./companies/{company_path}/sessions") if f.endswith('.session')]
        for session_file_path in session_files:
            full_path = f"./companies/{company_path}/sessions/{session_file_path}"
            client = make_client(full_path)
            session = Session(client, session_file_path, company_path)
            sessions.append(session)
            log_msg = f"Сессия {session_file_path} добавлена в sessions"
            rootLogger.info(log_msg)
            write_daily_log(log_msg)

    # Устанавливаем команды бота
    await set_bot_commands(bot)

    tasks = [activate_session(session) for session in sessions]
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        inactive_sessions = []
        for session, result in zip(sessions[:], results):
            if result is False or isinstance(result, Exception):
                log_msg = f"Сессия {session.filename} не активирована: {str(result)}"
                rootLogger.error(log_msg)
                write_daily_log(log_msg)
                if session.app:
                    if session.app.is_connected():
                        rootLogger.info(f"Закрываем клиент для сессии {session.filename} перед перемещением")
                        await session.app.disconnect()
                        if hasattr(session.app, '_sender') and session.app._sender:
                            await session.app._sender.disconnect()
                        await asyncio.sleep(2)
                    else:
                        rootLogger.info(f"Клиент для сессии {session.filename} уже отключён")
                session_path = f"./companies/{session.company}/sessions/{session.filename}"
                if "The used phone number has been banned" in str(result):
                    await move_to_banned(session_path)
                    sessions.remove(session)
                    log_msg = f"Сессия {session.filename} заблокирована Telegram и удалена из sessions"
                    rootLogger.info(log_msg)
                    write_daily_log(log_msg)
                else:
                    inactive_sessions.append(session)

        # Проверка всех сессий через @spambot после активации
        problematic_sessions = []
        for session in sessions[:]:
            try:
                if not session.app.is_connected():
                    await session.app.connect()
                log_msg = f"Проверка статуса сессии {session.filename} через @{FROZE_CHECK_BOT_USERNAME}..."
                rootLogger.info(log_msg)
                write_daily_log(log_msg)

                status, reason = await handle_spam_block(session.app)
                log_msg = f"Статус сессии {session.filename}: {status}, причина: {reason or 'нет'}"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)

                if status in ["жёлтый", "красный"]:
                    problematic_sessions.append(session)
                    session_path = f"./companies/{session.company}/sessions/{session.filename}"
                    try:
                        await move_to_spamblocked(session_path)
                        log_msg = f"Сессия {session.filename} перемещена в ./spamblocked_sessions (статус: {status})"
                        rootLogger.info(log_msg)
                        write_daily_log(log_msg)
                        await bot.send_message(chat_id, log_msg)
                    except Exception as e:
                        log_msg = f"Ошибка при перемещении сессии {session.filename}: {str(e)}"
                        rootLogger.error(log_msg)
                        write_daily_log(log_msg)
                        await bot.send_message(chat_id, log_msg)
                else:
                    me = await session.app.get_me()  # Убедимся, что me обновлён
                    session.id = me.id
                    session.me = me
                    log_msg = f"Сессия {session.filename} активна (ID: {me.id})"
                    rootLogger.info(log_msg)
                    write_daily_log(log_msg)

            except Exception as e:
                log_msg = f"Ошибка проверки сессии {session.filename}: {str(e)}"
                rootLogger.error(log_msg)
                write_daily_log(log_msg)
                problematic_sessions.append(session)

        # Удаляем проблемные сессии из списка активных
        for session in problematic_sessions:
            if session in sessions:
                sessions.remove(session)
                log_msg = f"Сессия {session.filename} удалена из активных сессий"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)

        if not os.path.exists("./banned_sessions"):
            os.makedirs("./banned_sessions")
        for session in inactive_sessions:
            if session.app:
                if session.app.is_connected():
                    rootLogger.info(f"Закрываем клиент для сессии {session.filename} перед удалением")
                    await session.app.disconnect()
                    if hasattr(session.app, '_sender') and session.app._sender:
                        await session.app._sender.disconnect()
                    await asyncio.sleep(2)
                else:
                    rootLogger.info(f"Клиент для сессии {session.filename} уже отключён")
            session_path = f"./companies/{session.company}/sessions/{session.filename}"
            await move_to_banned(session_path)
            sessions.remove(session)
            log_msg = f"Сессия {session.filename} неактивна и удалена"
            rootLogger.info(log_msg)
            write_daily_log(log_msg)

        active_sessions = [s for s in sessions if s.me is not None]
        log_msg = f"Активных сессий после проверки: {len(active_sessions)}"
        rootLogger.info(log_msg)
        write_daily_log(log_msg)

        global liking_tasks
        liking_tasks.clear()
        for session in active_sessions:
            if company_active.get(session.company, False):
                task = asyncio.create_task(worker_liking_stories(session))
                liking_tasks.append((task, session))
                rootLogger.info(f"Изначально запущена задача лайкинга для {session.filename}")

        asyncio.create_task(periodic_save_stats())
        scheduler.start()
        scheduler.add_job(remove_inactive_sessions, "interval", days=1, timezone=pytz.UTC)
        asyncio.create_task(dp.start_polling(bot, handle_as_tasks=False, handle_signals=False))
        await asyncio.Future()

    except Exception as e:
        log_msg = f"Критическая ошибка в main: {str(e)}"
        rootLogger.error(log_msg)
        write_daily_log(log_msg)
        await bot.send_message(chat_id, log_msg)

async def move_to_banned(session_path: str):
    filename = os.path.basename(session_path)
    banned_path = os.path.join("./banned_sessions", filename)
    max_attempts = 5
    attempt = 0
    session = next((s for s in sessions if f"./companies/{s.company}/sessions/{s.filename}" == session_path), None)
    if session and session.app and session.app.is_connected():
        await session.app.disconnect()
    while attempt < max_attempts:
        try:
            if not os.path.exists(session_path):
                log_msg = f"Ошибка: Файл сессии {filename} не найден"
                rootLogger.error(log_msg)
                write_daily_log(log_msg)
                await bot.send_message(chat_id, log_msg)
                return
            if not os.path.exists("./banned_sessions"):
                os.makedirs("./banned_sessions")
            shutil.move(session_path, banned_path)
            log_msg = f"Сессия {filename} перемещена в ./banned_sessions"
            rootLogger.info(log_msg)
            write_daily_log(log_msg)

            json_filename = filename.replace('.session', '.json')
            source_json_path = session_path.replace(filename, json_filename)
            target_json_path = os.path.join("./banned_sessions", json_filename)
            if os.path.exists(source_json_path):
                shutil.move(source_json_path, target_json_path)
                log_msg = f"JSON-файл {json_filename} перемещён в ./banned_sessions"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)

            return
        except Exception as e:
            attempt += 1
            if attempt < max_attempts:
                await asyncio.sleep(2)
            else:
                log_msg = f"Не удалось переместить сессию {filename}: {str(e)}"
                rootLogger.error(log_msg)
                write_daily_log(log_msg)
                await bot.send_message(chat_id, log_msg)

async def create_channel_for_accounts(data, message, state: FSMContext):
    name = data.get("name")
    avatar = data.get("avatar")
    posts = data.get("posts", [])
    select = data.get("select", [])
    ses = [s for s in sessions if s.me and int(s.me.id) in select]
    if not ses:
        await message.edit_text("Нет выбранных аккаунтов для создания канала.")
        await state.clear()
        await start(message)
        return

    created_channels.setdefault(selected_company, [])
    created_count = 0

    def convert_entities_to_html(text, entities):
        if not text or not entities:
            return text or ""
        result = text
        for entity in sorted(entities, key=lambda e: e.offset, reverse=True):
            start = entity.offset
            end = entity.offset + entity.length
            if end > len(text):
                end = len(text)
            if start >= end or start >= len(text) or start < 0:
                continue
            if entity.type == "bold":
                result = result[:start] + "<b>" + result[start:end] + "</b>" + result[end:]
            elif entity.type == "italic":
                result = result[:start] + "<i>" + result[start:end] + "</i>" + result[end:]
            elif entity.type == "spoiler":
                result = result[:start] + "<spoiler>" + result[start:end] + "</spoiler>" + result[end:]
        return result

    for session in ses:
        try:
            if not session.app.is_connected():
                log_msg = f"Клиент для сессии {session.filename} не подключён, пытаемся подключить"
                rootLogger.warning(log_msg)
                write_daily_log(log_msg)
                await session.app.connect()

            full_user = await session.app(functions.users.GetFullUserRequest(id=session.me.id))
            channel_id = full_user.full_user.personal_channel_id

            if channel_id:
                log_msg = f"Сессия {session.filename}: редактируем существующий канал {channel_id}"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)
                channel_entity = await session.app.get_entity(types.PeerChannel(channel_id))

                if channel_entity.title != name:
                    try:
                        await session.app(functions.channels.EditTitleRequest(
                            channel=channel_entity,
                            title=name
                        ))
                        log_msg = f"Название канала {channel_id} изменено на '{name}' для {session.filename}"
                        rootLogger.info(log_msg)
                        write_daily_log(log_msg)
                    except ForbiddenError as e:
                        if "The chat or channel wasn't modified" in str(e):
                            log_msg = f"Название канала {channel_id} уже '{name}', пропускаем изменение для {session.filename}"
                            rootLogger.info(log_msg)
                            write_daily_log(log_msg)
                        else:
                            raise
                else:
                    log_msg = f"Название канала {channel_id} уже '{name}', пропускаем изменение для {session.filename}"
                    rootLogger.info(log_msg)
                    write_daily_log(log_msg)

                if avatar:
                    file_info = await bot.get_file(avatar)
                    photo_bytes = await bot.download_file(file_info.file_path)
                    file = await session.app.upload_file(photo_bytes, file_name="avatar.jpg")
                    await session.app(functions.channels.EditPhotoRequest(
                        channel=channel_entity,
                        photo=file
                    ))
                    log_msg = f"Аватарка канала {channel_id} обновлена для {session.filename}"
                    rootLogger.info(log_msg)
                    write_daily_log(log_msg)

                async for msg in session.app.iter_messages(channel_id):
                    try:
                        await session.app.delete_messages(channel_id, [msg.id])
                    except Exception as e:
                        log_msg = f"Ошибка при удалении сообщения {msg.id} в канале {channel_id}: {str(e)}"
                        rootLogger.warning(log_msg)
                        write_daily_log(log_msg)
                log_msg = f"Все старые посты удалены из канала {channel_id} для {session.filename}"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)

                for post in posts:
                    if post['type'] == 'text':
                        formatted_text = convert_entities_to_html(post['content'], post['entities'])
                        await session.app.send_message(channel_id, formatted_text, parse_mode='html')
                    elif post['type'] in ['photo', 'video']:
                        file_info = await bot.get_file(post['file_id'])
                        media_bytes = await bot.download_file(file_info.file_path)
                        file_name = "media.jpg" if post['type'] == 'photo' else "media.mp4"
                        file = await session.app.upload_file(media_bytes, file_name=file_name)
                        formatted_caption = convert_entities_to_html(post['caption'], post['entities'])
                        await session.app.send_message(channel_id, formatted_caption, file=file, parse_mode='html')
                    elif post['type'] == 'album':
                        files = []
                        first_photo_caption = post['photos'][0]['caption']
                        first_photo_entities = post['photos'][0]['entities']
                        for idx, photo in enumerate(post['photos']):
                            file_info = await bot.get_file(photo['file_id'])
                            media_bytes = await bot.download_file(file_info.file_path)
                            file = await session.app.upload_file(media_bytes, file_name=f"media_{idx}.jpg")
                            files.append(file)
                        formatted_caption = convert_entities_to_html(first_photo_caption, first_photo_entities)
                        log_msg = f"Попытка отправки альбома из {len(files)} фото в канал {channel_id} для {session.filename}"
                        rootLogger.info(log_msg)
                        write_daily_log(log_msg)
                        max_attempts = 3
                        for attempt in range(max_attempts):
                            try:
                                await session.app.send_file(channel_id, files, caption=formatted_caption, parse_mode='html')
                                log_msg = f"Альбом из {len(files)} фото отправлен в канал {channel_id} для {session.filename}"
                                rootLogger.info(log_msg)
                                write_daily_log(log_msg)
                                break
                            except FloodWaitError as e:
                                log_msg = f"Ожидание {e.seconds} секунд из-за FloodWait при отправке альбома в канал {channel_id} для {session.filename}"
                                rootLogger.warning(log_msg)
                                write_daily_log(log_msg)
                                await bot.send_message(chat_id, log_msg)
                                await asyncio.sleep(e.seconds)
                            except Exception as e:
                                if attempt < max_attempts - 1:
                                    log_msg = f"Ошибка при отправке альбома (попытка {attempt + 1}/{max_attempts}) в канал {channel_id} для {session.filename}: {str(e)}, повтор через 2 секунды"
                                    rootLogger.warning(log_msg)
                                    write_daily_log(log_msg)
                                    await asyncio.sleep(2)
                                    continue
                                else:
                                    log_msg = f"Не удалось отправить альбом после {max_attempts} попыток в канал {channel_id} для {session.filename}: {str(e)}"
                                    rootLogger.error(log_msg)
                                    write_daily_log(log_msg)
                                    await bot.send_message(chat_id, log_msg)

                log_msg = f"Канал {channel_id} обновлён для аккаунта {session.filename}: новое название '{name}', добавлено {len(posts)} постов"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)
                created_count += 1
            else:
                log_msg = f"Сессия {session.filename}: создаём новый канал"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)
                channel = await session.app(functions.channels.CreateChannelRequest(
                    title=name,
                    about="Создан автоматически",
                    megagroup=False
                ))
                channel_entity = channel.chats[0]
                channel_id = channel_entity.id

                if avatar:
                    file_info = await bot.get_file(avatar)
                    photo_bytes = await bot.download_file(file_info.file_path)
                    file = await session.app.upload_file(photo_bytes, file_name="avatar.jpg")
                    await session.app(functions.channels.EditPhotoRequest(
                        channel=channel_id,
                        photo=file
                    ))

                for post in posts:
                    if post['type'] == 'text':
                        formatted_text = convert_entities_to_html(post['content'], post['entities'])
                        await session.app.send_message(channel_id, formatted_text, parse_mode='html')
                    elif post['type'] in ['photo', 'video']:
                        file_info = await bot.get_file(post['file_id'])
                        media_bytes = await bot.download_file(file_info.file_path)
                        file_name = "media.jpg" if post['type'] == 'photo' else "media.mp4"
                        file = await session.app.upload_file(media_bytes, file_name=file_name)
                        formatted_caption = convert_entities_to_html(post['caption'], post['entities'])
                        await session.app.send_message(channel_id, formatted_caption, file=file, parse_mode='html')
                    elif post['type'] == 'album':
                        files = []
                        first_photo_caption = post['photos'][0]['caption']
                        first_photo_entities = post['photos'][0]['entities']
                        for idx, photo in enumerate(post['photos']):
                            file_info = await bot.get_file(photo['file_id'])
                            media_bytes = await bot.download_file(file_info.file_path)
                            file = await session.app.upload_file(media_bytes, file_name=f"media_{idx}.jpg")
                            files.append(file)
                        formatted_caption = convert_entities_to_html(first_photo_caption, first_photo_entities)
                        log_msg = f"Попытка отправки альбома из {len(files)} фото в канал {channel_id} для {session.filename}"
                        rootLogger.info(log_msg)
                        write_daily_log(log_msg)
                        max_attempts = 3
                        for attempt in range(max_attempts):
                            try:
                                await session.app.send_file(channel_id, files, caption=formatted_caption, parse_mode='html')
                                log_msg = f"Альбом из {len(files)} фото отправлен в канал {channel_id} для {session.filename}"
                                rootLogger.info(log_msg)
                                write_daily_log(log_msg)
                                break
                            except FloodWaitError as e:
                                log_msg = f"Ожидание {e.seconds} секунд из-за FloodWait при отправке альбома в канал {channel_id} для {session.filename}"
                                rootLogger.warning(log_msg)
                                write_daily_log(log_msg)
                                await bot.send_message(chat_id, log_msg)
                                await asyncio.sleep(e.seconds)
                            except Exception as e:
                                if attempt < max_attempts - 1:
                                    log_msg = f"Ошибка при отправке альбома (попытка {attempt + 1}/{max_attempts}) в канал {channel_id} для {session.filename}: {str(e)}, повтор через 2 секунды"
                                    rootLogger.warning(log_msg)
                                    write_daily_log(log_msg)
                                    await asyncio.sleep(2)
                                    continue
                                else:
                                    log_msg = f"Не удалось отправить альбом после {max_attempts} попыток в канал {channel_id} для {session.filename}: {str(e)}"
                                    rootLogger.error(log_msg)
                                    write_daily_log(log_msg)
                                    await bot.send_message(chat_id, log_msg)

                base_username = "".join(TRANS_TABLE.get(c, c) for c in name.lower() if c.isalnum() or c in TRANS_TABLE)
                username = f"{base_username[:20]}{session.me.id % 10000}"
                i = 0
                username_set = False
                while i < 5:
                    try:
                        await session.app(functions.channels.UpdateUsernameRequest(
                            channel=channel_entity,
                            username=username
                        ))
                        username_set = True
                        log_msg = f"Username {username} установлен для канала {channel_id} в сессии {session.filename}"
                        rootLogger.info(log_msg)
                        write_daily_log(log_msg)
                        break
                    except FloodWaitError as e:
                        log_msg = f"Ожидание {e.seconds} секунд из-за FloodWait для {session.filename}"
                        rootLogger.warning(log_msg)
                        write_daily_log(log_msg)
                        await bot.send_message(chat_id, log_msg)
                        await asyncio.sleep(e.seconds)
                    except UsernameOccupiedError:
                        i += 1
                        username = f"{base_username[:20]}{session.me.id % 10000}{i}"
                        log_msg = f"Username {username[:-1]} занят, пробуем {username}"
                        rootLogger.warning(log_msg)
                        write_daily_log(log_msg)
                    except Exception as e:
                        log_msg = f"Ошибка установки username для {session.filename}: {str(e)}"
                        rootLogger.error(log_msg)
                        write_daily_log(log_msg)
                        await bot.send_message(chat_id, log_msg)
                        break

                if username_set:
                    try:
                        await session.app(functions.account.UpdatePersonalChannelRequest(channel=channel_entity))
                        log_msg = f"Канал {username} успешно привязан как личный для {session.filename}"
                        rootLogger.info(log_msg)
                        write_daily_log(log_msg)
                    except FloodWaitError as e:
                        log_msg = f"FloodWait при привязке канала {username} для {session.filename}: ждём {e.seconds} секунд"
                        rootLogger.warning(log_msg)
                        write_daily_log(log_msg)
                        await bot.send_message(chat_id, log_msg)
                        await asyncio.sleep(e.seconds)
                        continue
                    except Exception as e:
                        log_msg = f"Ошибка привязки канала {username} для {session.filename}: {str(e)}"
                        rootLogger.error(log_msg)
                        write_daily_log(log_msg)
                        await bot.send_message(chat_id, log_msg)

                channel_link = f"https://t.me/{username}" if username_set else f"Канал {channel_id} (приватный)"
                log_msg = f"Канал '{name}' создан для аккаунта {session.filename} с ID {channel_id} по ссылке {channel_link}, добавлено {len(posts)} постов"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)
                created_channels[selected_company].append({"id": channel_id, "name": name})
                created_count += 1

        except Exception as e:
            log_msg = f"Ошибка создания/редактирования канала для {session.filename}: {str(e)}"
            rootLogger.error(log_msg)
            write_daily_log(log_msg)
            await bot.send_message(chat_id, log_msg)

    with open(f"./companies/{selected_company}/created_channels.json", "w", encoding="utf-8") as file:
        json.dump(created_channels[selected_company], file, ensure_ascii=False)

    log_msg = f"Создание/обновление каналов завершено: обработано {created_count} из {len(ses)} аккаунтов."
    rootLogger.info(log_msg)
    write_daily_log(log_msg)
    await bot.send_message(chat_id, log_msg)
    await message.edit_text(f"Обработано каналов: {created_count} из {len(ses)}")
    await state.clear()
    await start(message)

async def apply_data(callback: atypes.CallbackQuery, state: FSMContext, change: str = ""):
    data = await state.get_data()
    select = data.get("select", [])
    change = data.get("change", change)
    value = data.get("value", None) if change != "delete_avatar" else None
    ses = [s for s in sessions if s.me and int(s.me.id) in select]
    if not ses:
        await callback.message.edit_text("Нет выбранных аккаунтов для применения изменений.")
        await state.clear()
        await start(callback.message)
        return
    rootLogger.info(f"Применение действия '{change}' для {len(ses)} сессий")
    async def apply_to_session(session):
        try:
            if change == "fname":
                me = await session.app.get_me()
                current_last_name = me.last_name if me.last_name else ""
                await session.app(functions.account.UpdateProfileRequest(
                    first_name=value,
                    last_name=current_last_name
                ))
                log_msg = f"Имя изменено для сессии {session.filename}: {value}"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)
            elif change == "lname":
                me = await session.app.get_me()
                current_first_name = me.first_name if me.first_name else ""
                await session.app(functions.account.UpdateProfileRequest(
                    first_name=current_first_name,
                    last_name=value
                ))
                log_msg = f"Фамилия изменена для сессии {session.filename}: {value}"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)
            elif change == "bio":
                await session.app(functions.account.UpdateProfileRequest(
                    first_name=session.me.first_name if session.me.first_name else "",
                    last_name=session.me.last_name if session.me.last_name else "",
                    about=value
                ))
                log_msg = f"Био изменено для сессии {session.filename}: {value}"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)
            elif change == "avatar":
                if isinstance(value, bytes):
                    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as temp_file:
                        temp_file.write(value)
                        temp_file_path = temp_file.name
                    try:
                        photo = await session.app.upload_file(temp_file_path)
                        await session.app(functions.photos.UploadProfilePhotoRequest(file=photo))
                        log_msg = f"Аватар обновлён для сессии {session.filename}"
                        rootLogger.info(log_msg)
                        write_daily_log(log_msg)
                    finally:
                        os.remove(temp_file_path)
                else:
                    raise ValueError("Неверный формат аватара")
            elif change == "delete_avatar":
                photos = await session.app(functions.photos.GetUserPhotosRequest(
                    user_id=await session.app.get_input_entity(session.me.id),
                    offset=0,
                    max_id=0,
                    limit=100
                ))
                if isinstance(photos, types.photos.Photos) and photos.photos:
                    photo_ids = [types.InputPhoto(
                        id=photo.id,
                        access_hash=photo.access_hash,
                        file_reference=photo.file_reference
                    ) for photo in photos.photos]
                    await session.app(functions.photos.DeletePhotosRequest(id=photo_ids))
                    log_msg = f"Все фото удалены для сессии {session.filename}"
                    rootLogger.info(log_msg)
                    write_daily_log(log_msg)
                else:
                    log_msg = f"Нет фото для удаления в профиле сессии {session.filename}"
                    rootLogger.info(log_msg)
                    write_daily_log(log_msg)
            elif change == "story_reactions":
                log_msg = f"Реакции для {session.filename} обновлены на фиксированный список"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)
        except Exception as e:
            raise e
    tasks = [apply_to_session(session) for session in ses]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    successful_updates = 0
    for session, result in zip(ses, results):
        if isinstance(result, Exception):
            log_msg = f"ОШИБКА: Не удалось применить изменения для {session.filename}: {str(result)}"
            rootLogger.error(log_msg)
            write_daily_log(log_msg)
            await bot.send_message(chat_id, log_msg)
        else:
            successful_updates += 1
    report_text = f"Действия применены к {successful_updates} {'сессии' if successful_updates == 1 else 'сессиям'} из {len(ses)}"
    max_attempts = 1
    attempt = 0
    while attempt < max_attempts:
        try:
            current_message = await bot.get_messages(callback.message.chat.id, callback.message.message_id)
            if current_message.text != report_text:
                await callback.message.edit_text(report_text)
            break
        except Exception as e:
            if "message is not modified" in str(e).lower():
                break  
            attempt += 1
            if attempt < max_attempts:
                await asyncio.sleep(3)
            else:
                log_msg = f"Не удалось отправить отчёт: {str(e)}"
                rootLogger.error(log_msg)
                write_daily_log(log_msg)
                await bot.send_message(chat_id, log_msg)
    await callback.answer()
    await state.clear()
    await start(callback.message)


async def check_account_frozen(client: TelegramClient) -> bool:
    """
    Checking whether account if frozen

    :param client: Telegram client
    :return: True if frozen, false otherwise
    """

    try:
        await client.send_message(FROZE_CHECK_BOT_USERNAME, "/start")
    except Exception as e:
        print(e)
        return True

    await client.delete_dialog(FROZE_CHECK_BOT_USERNAME)
    return False


async def handle_spam_block(client: TelegramClient):
    me = await client.get_me()
    filtered_sessions = list(filter(lambda x: x.me is not None and x.me.id == me.id, sessions))
    if not filtered_sessions:
        log_msg = f"Сессия для клиента с ID {me.id} не найдена"
        rootLogger.error(log_msg)
        write_daily_log(log_msg)
        return None, None
    session: Session = filtered_sessions[0]
    if not hasattr(session, 'last_spam_check'):
        session.last_spam_check = None
    if not hasattr(session, 'spam_check_cooldown'):
        session.spam_check_cooldown = 300
    current_time = datetime.now(pytz.UTC)
    if session.last_spam_check and (current_time - session.last_spam_check).total_seconds() < session.spam_check_cooldown:
        log_msg = f"Проверка статуса сессии {session.filename} на паузе (cooldown)"
        rootLogger.debug(log_msg)
        write_daily_log(log_msg)
        return None, None

    try:
        is_frozen = await check_account_frozen(client)

        if is_frozen:
            session.blocked = True
            session.unblocked_at = None
            return "красный", "заморожен"

        return "зелёный", None
    except Exception as e:
        if "Request was unsuccessful" in str(e) and attempt < max_attempts - 1:
            await asyncio.sleep(5)
        else:
            log_msg = f"Сессия {session.filename} не смогла проверить спам-блок: {str(e)}"
            rootLogger.error(log_msg)
            write_daily_log(log_msg)
            return "жёлтый", "ошибка"

async def on_spambot_message(event):
    client = event.client
    message = event.message
    await handle_spam_block(client)
    me = await client.get_me()
    session = next((s for s in sessions if s.me is not None and s.me.id == me.id), None)
    if session and session.blocked and not session.sent_appelation:
        if isinstance(message.reply_markup, types.ReplyKeyboardMarkup):
            if len(message.reply_markup.rows) == 4:
                await client.send_message(178220800, message.reply_markup.rows[3][0].text)
            if len(message.reply_markup.rows) == 2:
                await client.send_message(178220800, message.reply_markup.rows[0][0].text)
            session.sent_appelation = True

async def collect_created_views_stats(message: atypes.Message):
    ses = [s for s in sessions if s.me and s.company == selected_company]
    if not ses:
        await message.reply(f"В компании {selected_company} нет активных сессий.")
        return
    
    stats_report = []
    total_channels = 0
    total_views_all = 0

    for session in ses:
        try:
            full_user = await session.app(functions.users.GetFullUserRequest(id=session.me.id))
            channel_id = full_user.full_user.personal_channel_id
            if not channel_id:
                log_msg = f"У аккаунта {session.filename} нет привязанного канала."
                rootLogger.info(log_msg)
                write_daily_log(log_msg)
                continue

            channel_entity = await session.app.get_entity(types.PeerChannel(channel_id))
            channel_username = f"@{channel_entity.username}" if channel_entity.username else f"t.me/c/{channel_id}"

            messages = []
            async for msg in session.app.iter_messages(channel_id, limit=10):
                if msg.views is not None:
                    messages.append({
                        "views": msg.views
                    })
            
            if not messages:
                log_msg = f"В канале (ID {channel_id}) нет постов с просмотрами для аккаунта {session.filename}."
                rootLogger.info(log_msg)
                write_daily_log(log_msg)
                continue

            total_views = sum(msg["views"] for msg in messages)
            total_views_all += total_views
            total_channels += 1
            stats_report.append(f"Сессия {session.filename}: {channel_username}, просмотров: {total_views}")

            log_msg = f"Статистика собрана для канала {channel_username} (ID {channel_id}): {len(messages)} постов, {total_views} просмотров (аккаунт {session.filename})."
            rootLogger.info(log_msg)
            write_daily_log(log_msg)

        except Exception as e:
            log_msg = f"Ошибка при сборе статистики для аккаунта {session.filename}: {str(e)}"
            rootLogger.error(log_msg)
            write_daily_log(log_msg)
            await bot.send_message(chat_id, log_msg)
            continue

    if not stats_report:
        await message.reply(f"Не найдено привязанных каналов с постами для компании {selected_company}.")
        return

    stats_report.append(f"Итог: общее количество просмотров: {total_views_all}")
    report_text = "\n".join(stats_report)
    await message.reply(report_text)

    log_msg = f"Сбор статистики по просмотрам завершён для компании {selected_company}: {total_channels} каналов, {total_views_all} просмотров."
    rootLogger.info(log_msg)
    write_daily_log(log_msg)
    await bot.send_message(chat_id, log_msg)

# Ботовское
@dp.callback_query((F.from_user.id == owner_id) & (F.data == "back_to_start"))
async def back_to_start(callback: atypes.CallbackQuery):
    global selected_company
    if selected_company is None:
        kb_companies = atypes.InlineKeyboardMarkup(inline_keyboard=[
            *[atypes.InlineKeyboardButton(text=x, callback_data="sel_company_" + x) for x in listdir("./companies")],
            [atypes.InlineKeyboardButton(text="Создать компанию", callback_data="create_company")],
            [atypes.InlineKeyboardButton(text="Статистика аккаунтов", callback_data="account_stats")]
        ])
        await callback.message.edit_text("Выберите компанию:", reply_markup=kb_companies)
    else:
        async with stats_lock:
            stats_text = get_stats_text(selected_company)
        msg = await callback.message.edit_text(stats_text, reply_markup=kb_menu)
        asyncio.create_task(update_stats_message(msg.chat.id, msg.message_id, selected_company))
    await callback.answer()

@dp.message((F.from_user.id == owner_id) & (F.text == "/stats_all"))
async def stats_all(message: atypes.Message):
    stats = get_all_stats()
    await message.reply(stats)

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "collect_created_views_stats"))
async def start_collect_views_stats(callback: atypes.CallbackQuery):
    if not selected_company:
        await callback.message.edit_text("Сначала выберите компанию.")
        await callback.answer()
        return
    await callback.answer()
    msg = await callback.message.edit_text("Сбор статистики по привязанным каналам начался...")
    await collect_created_views_stats(msg)
    await start(callback.message)

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "back_to_menu"))
async def back_to_menu(callback: atypes.CallbackQuery):
    async with stats_lock:
        stats_text = (
            f"Сторис просмотрено: {company_stats[selected_company]['stories_viewed']}\n"
            f"Лайков поставлено: {company_stats[selected_company]['likes_set']}\n"
            f"Уникальных пользователей открыто: {len(company_stats[selected_company]['unique_users'])}\n"
            f"Уникальных пользователей со сторисами: {len(company_stats[selected_company]['unique_users_with_stories'])}\n"  # Новый счётчик
            f"Каналов пройдено: {company_stats[selected_company]['channels_processed']}\n"
            f"Чатов пройдено: {company_stats[selected_company]['chats_processed']}\n\n"
            "Выберите действие ниже:"
        )
    msg = await callback.message.edit_text(stats_text, reply_markup=kb_menu)
    asyncio.create_task(update_stats_message(msg.chat.id, msg.message_id, selected_company))
    await callback.answer()

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "back_to_change"))
async def back_to_change(callback: atypes.CallbackQuery):
    async with stats_lock:
        stats_text = get_stats_text(selected_company)
    msg = await callback.message.edit_text(stats_text, reply_markup=kb_change_settings)
    asyncio.create_task(update_stats_message(msg.chat.id, msg.message_id, selected_company))
    await callback.answer()

@dp.message((F.from_user.id == owner_id) & (F.text == "/start"))
async def start(message: atypes.Message):
    global selected_company
    log_msg = f"Запуск команды /start. Текущая компания: {selected_company}"
    rootLogger.info(log_msg)
    write_daily_log(log_msg)
    ses = list(filter(lambda x: x.me is not None and x.company == selected_company, sessions)) if selected_company else []
    log_msg = f"Найдено активных сессий для компании {selected_company}: {len(ses)}"
    rootLogger.info(log_msg)
    write_daily_log(log_msg)
    if selected_company is None:
        kb_companies = atypes.InlineKeyboardMarkup(inline_keyboard=list(
            map(lambda x: [atypes.InlineKeyboardButton(text=x, callback_data="sel_company_" + x)],
                list(listdir("./companies"))
                )) + [[atypes.InlineKeyboardButton(text="Создать компанию", callback_data="create_company")],
                      [atypes.InlineKeyboardButton(text="Статистика аккаунтов", callback_data="account_stats")]])
        await message.reply("Чтобы начать работу, выберите компанию:", reply_markup=kb_companies)
    else:
        await message.reply("Выберите действие ниже:", reply_markup=kb_menu)

def make_stat_str(session):
    blocked = "☢️ Заблокирован" if session.blocked else f"⚠️ Разблокируется в {session.unblocked_at}" if session.unblocked_at else "🗒 Ждёт апелляции" if session.sent_appelation else "✅ Свободен"
    username = session.me.username if session.me.username else session.me.phone_number if session.me.phone_number else session.me.id
    premium = "✅ Есть премиум" if session.me.is_premium else "❌ Нет премиума"
    return f"{session.filename} - {blocked} - {username} - {premium} - Лайков сторис сегодня: {session.story_likes_today}"

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "account_stats"))
async def account_stat(callback: atypes.CallbackQuery):
    text = list(map(make_stat_str, list(
        filter(lambda x: x.me is not None and x.company == selected_company if selected_company else True,
               sessions)))) + ["⛔️ Отключенные аккаунты: " + ", ".join(
        list(map(lambda x: x.filename, list(filter(lambda x: x.me is None, sessions)))))]
    await callback.message.edit_text("\n".join(text))

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "create_company"))
async def create_company(callback: atypes.CallbackQuery, state: FSMContext):
    await state.set_state(CreateCompanyState.name)
    await callback.message.edit_text("Напишите имя компании")

@dp.message(CreateCompanyState.name, (F.from_user.id == owner_id))
async def set_name(message: atypes.Message, state: FSMContext):
    global selected_company
    await state.clear()
    selected_company = message.text.lower()
    os.makedirs(f"./companies/{selected_company}/sessions")
    company_configs[selected_company] = {}
    company_stats[selected_company] = {
        "stories_viewed": 0,
        "likes_set": 0,
        "unique_users": set(),
        "channels_processed": 0,
        "chats_processed": 0,
        "unique_users_with_stories": set()
    }
    stats_file = f"./companies/{selected_company}/company_stats.json"
    with open(stats_file, "w", encoding='utf-8') as f:
        json.dump({
            "stories_viewed": 0,
            "likes_set": 0,
            "unique_users": [],
            "channels_processed": 0,
            "chats_processed": 0,
            "unique_users_with_stories": []
        }, f, ensure_ascii=False)
    await message.reply("Компания создана, добавьте сессии и каналы")
    await start(message)


def create_companies_keyboard(selected_companies: set):
    builder = InlineKeyboardBuilder()
    for company in company_configs.keys():
        is_selected = company in selected_companies
        builder.row(atypes.InlineKeyboardButton(
            text=f"{'✅' if is_selected else '❌'} {company}",
            callback_data=f"select_company_{company}"
        ))
    builder.row(atypes.InlineKeyboardButton(text="✅ Готово", callback_data="select_companies_done"))
    builder.row(atypes.InlineKeyboardButton(text="Запустить все", callback_data="select_all_companies"))
    return builder.as_markup()

@dp.callback_query((F.from_user.id == owner_id) & (F.data.startswith("select_company_")))
async def handle_company_selection(callback: atypes.CallbackQuery):
    company = callback.data.replace("select_company_", "")
    selected_companies = set()
    await callback.message.edit_reply_markup(
        reply_markup=create_companies_keyboard(selected_companies)
    )
    await switch_company(callback, company)

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "select_companies_done"))
async def finish_company_selection(callback: atypes.CallbackQuery):
    await callback.message.edit_text("Выбор компаний завершён.")
    await callback.answer()

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "select_all_companies"))
async def select_all_companies(callback: atypes.CallbackQuery):
    selected_companies = set(company_configs.keys())
    await callback.message.edit_reply_markup(
        reply_markup=create_companies_keyboard(selected_companies)
    )
    await callback.answer()

@dp.callback_query((F.from_user.id == owner_id) & (F.data.startswith("sel_company_")))
async def select_company(callback: atypes.CallbackQuery):
    global selected_company
    company = callback.data[12:]
    if not path.exists(f"./companies/{company}"):
        return await callback.message.edit_text("Выбранная компания не существует")
    selected_company = company
    async with stats_lock:
        stats_text = get_stats_text(selected_company)
    msg = await callback.message.edit_text(stats_text, reply_markup=kb_menu)
    asyncio.create_task(update_stats_message(msg.chat.id, msg.message_id, selected_company))
    await callback.answer()

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "add_channel"))
async def start_add_channel(callback: atypes.CallbackQuery, state: FSMContext):
    await state.set_state(AddPrivateChannelState.name)
    await callback.message.edit_text("Как назвать канал?")
    await callback.answer()

@dp.message(AddPrivateChannelState.name, (F.from_user.id == owner_id))
async def set_channel_name(message: atypes.Message, state: FSMContext):
    await state.update_data(name=message.text)
    await state.set_state(AddPrivateChannelState.avatar)
    await message.reply("Какой будет аватарка? Пришли фото или напиши 'нет'.")

@dp.message(AddPrivateChannelState.avatar, (F.from_user.id == owner_id))
async def set_channel_avatar(message: atypes.Message, state: FSMContext):
    if message.content_type == 'photo':
        avatar = message.photo[-1].file_id
    elif message.content_type == 'document' and message.document.mime_type.startswith('image/'):
        avatar = message.document.file_id
    else:
        avatar = None if message.text and message.text.lower() == 'нет' else None
    await state.update_data(avatar=avatar)
    await state.set_state(AddPrivateChannelState.posts)
    await state.update_data(posts=[])
    await message.reply("Отправляйте посты для канала (текст, фото, видео). Завершите командой /done.")

@dp.message(AddPrivateChannelState.posts, (F.from_user.id == owner_id))
async def add_channel_post(message: atypes.Message, state: FSMContext):
    if message.text and message.text.lower() == '/done':
        data = await state.get_data()
        posts = data.get('posts', [])
        if not posts:
            await message.reply("Вы не добавили ни одного поста.")
            return
        await state.set_state(AddPrivateChannelState.select)
        ses = list(filter(lambda x: x.me is not None and x.company == selected_company, sessions))
        await message.reply("Выберите аккаунты для создания канала:", reply_markup=kb_all_or_select)
        return
    
    data = await state.get_data()
    posts = data.get('posts', [])
    media_groups = data.get('media_groups', {})

    if message.media_group_id:
        if message.media_group_id not in media_groups:
            media_groups[message.media_group_id] = []
        media_groups[message.media_group_id].append({
            'file_id': message.photo[-1].file_id,
            'caption': message.caption or '' if not media_groups[message.media_group_id] else '',
            'entities': message.caption_entities or [] if not media_groups[message.media_group_id] else []
        })
        if len(media_groups[message.media_group_id]) == 1:
            posts.append({'type': 'album', 'photos': media_groups[message.media_group_id]})
    elif message.content_type == 'text':
        posts.append({'type': 'text', 'content': message.text, 'entities': message.entities or []})
    elif message.content_type == 'photo':
        posts.append({'type': 'photo', 'file_id': message.photo[-1].file_id, 'caption': message.caption or '', 'entities': message.caption_entities or []})
    elif message.content_type == 'video':
        posts.append({'type': 'video', 'file_id': message.video.file_id, 'caption': message.caption or '', 'entities': message.caption_entities or []})
    else:
        await message.reply("Поддерживаются только текст, фото и видео.")
        return
    
    await state.update_data(posts=posts, media_groups=media_groups)
    await message.reply("Пост добавлен. Добавьте ещё или завершите с /done.")

@dp.callback_query(AddPrivateChannelState.select, (F.from_user.id == owner_id) & (F.data.in_({"select_all", "select_selective"})))
async def select_channel_accounts(callback: atypes.CallbackQuery, state: FSMContext):
    global sessions, selected_company
    data = await state.get_data()
    ses = list(filter(lambda x: x.me is not None and x.company == selected_company, sessions))
    if not ses:
        await callback.message.edit_text("Нет активных сессий для этой компании.")
        await state.clear()
        await start(callback.message)
        return
    if callback.data == "select_all":
        await state.update_data(select=[int(session.me.id) for session in ses])
        log_msg = f"Выбраны все аккаунты для компании {selected_company}: {len(ses)}"
        rootLogger.info(log_msg)
        write_daily_log(log_msg)
        await state.set_state(AddPrivateChannelState.confirm)
        await callback.message.edit_text("Подтвердите создание канала:", reply_markup=atypes.InlineKeyboardMarkup(inline_keyboard=[
            [atypes.InlineKeyboardButton(text="Подтвердить", callback_data="confirm_create")],
            [atypes.InlineKeyboardButton(text="Назад", callback_data="back_to_select")]
        ]))
    elif callback.data == "select_selective":
        await state.update_data(select=[])
        builder = InlineKeyboardBuilder()
        for session in ses:
            me = session.me
            builder.row(atypes.InlineKeyboardButton(
                text=f"❌ {me.first_name}{' ' + me.last_name if me.last_name else ''} ({'@' + me.username if me.username else '+' + me.phone_number} {me.id})",
                callback_data=f"select_channel_{me.id}"))
        builder.row(atypes.InlineKeyboardButton(text="✅ Готово", callback_data="select_channel_done"))
        await callback.message.edit_text("Выберите аккаунты:", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(AddPrivateChannelState.select, (F.from_user.id == owner_id) & (F.data.startswith("select_channel_")))
async def select_channel_specific(callback: atypes.CallbackQuery, state: FSMContext):
    global sessions, selected_company
    data = await state.get_data()
    if callback.data == "select_channel_done":
        select = data.get("select", [])
        if not select:
            await callback.message.edit_text("Выберите хотя бы один аккаунт.")
            return
        await state.set_state(AddPrivateChannelState.confirm)
        await callback.message.edit_text("Подтвердите создание канала:", reply_markup=atypes.InlineKeyboardMarkup(inline_keyboard=[
            [atypes.InlineKeyboardButton(text="Подтвердить", callback_data="confirm_create")],
            [atypes.InlineKeyboardButton(text="Назад", callback_data="back_to_select")]
        ]))
        return
    session_id = int(callback.data.split("_")[-1])
    select = data.get("select", [])
    if session_id in select:
        select.remove(session_id)
    else:
        select.append(session_id)
    await state.update_data(select=select)
    ses = list(filter(lambda x: x.me is not None and x.company == selected_company, sessions))
    builder = InlineKeyboardBuilder()
    for session in ses:
        me = session.me
        is_selected = int(me.id) in data.get("select", [])
        builder.row(atypes.InlineKeyboardButton(
            text=f"{'✅' if is_selected else '❌'} {me.first_name}{' ' + me.last_name if me.last_name else ''} ({'@' + me.username if me.username else '+' + me.phone_number} {me.id})",
            callback_data=f"select_channel_{me.id}"))
    builder.row(atypes.InlineKeyboardButton(text="✅ Готово", callback_data="select_channel_done"))
    await callback.message.edit_reply_markup(reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(AddPrivateChannelState.confirm, (F.from_user.id == owner_id) & (F.data == "confirm_create"))
async def confirm_create_channel(callback: atypes.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    await create_channel_for_accounts(data, callback.message, state)

@dp.callback_query(AddPrivateChannelState.confirm, (F.from_user.id == owner_id) & (F.data == "back_to_select"))
async def back_to_select(callback: atypes.CallbackQuery, state: FSMContext):
    await state.set_state(AddPrivateChannelState.select)
    ses = list(filter(lambda x: x.me is not None and x.company == selected_company, sessions))
    await callback.message.edit_text("Выберите аккаунты:", reply_markup=kb_all_or_select)
    await callback.answer()

@dp.callback_query((F.from_user.id == owner_id) & (F.data.in_({"change_fname", "change_bio", "change_lname", "change_avatar", "delete_avatar", "change_story_reactions"})))
async def change(callback: atypes.CallbackQuery, state: FSMContext):
    callback_data = callback.data
    change_type = callback_data[7:] if callback_data.startswith("change_") else callback_data
    rootLogger.info(f"Выбран тип изменения: {change_type}")
    await state.update_data(change=change_type)
    if change_type == "fname":
        await state.set_state(ChangeState.value)
        await callback.message.edit_text("Отправьте новое имя:")
    elif change_type == "lname":
        await state.set_state(ChangeState.value)
        await callback.message.edit_text("Отправьте новую фамилию:")
    elif change_type == "avatar":
        await state.set_state(ChangeState.value)
        await callback.message.edit_text("Отправьте новое фото:")
    elif change_type == "delete_avatar":
        await state.set_state(ChangeState.select)
        await callback.message.edit_text("К каким аккаунтам применить удаление?", reply_markup=kb_all_or_select)
    elif change_type == "bio":
        await state.set_state(ChangeState.value)
        await callback.message.edit_text("Отправьте новое био:")
    elif change_type == "story_reactions":
        await state.set_state(ChangeState.select)
        await callback.message.edit_text("К каким аккаунтам применить фиксированные реакции?", reply_markup=kb_all_or_select)
    await callback.answer()

@dp.message((F.from_user.id == owner_id) & (F.text == "/cancel"))
async def cancel(message: atypes.Message, state: FSMContext):
    if await state.get_state() is not None:
        await state.clear()
        await message.reply("Отменено.")
    await start(message)

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "change_company"))
async def change_company(callback: atypes.CallbackQuery):
    global selected_company
    selected_company = None
    kb_companies = atypes.InlineKeyboardMarkup(inline_keyboard=list(
        map(lambda x: [atypes.InlineKeyboardButton(text=x, callback_data="sel_company_" + x)],
            listdir("./companies"))
        ) + [[atypes.InlineKeyboardButton(text="Создать компанию", callback_data="create_company")]])
    await callback.message.edit_text("Выберите компанию:", reply_markup=kb_companies)
    await callback.answer()

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "add_sessions"))
async def add_sessions(callback: atypes.CallbackQuery, state: FSMContext):
    await state.set_state(AddSessionState.add)
    await callback.message.edit_text("Отправьте zip-файл с сессиями:", reply_markup=None)
    await callback.answer()

async def add_sessions(company, zip_path):
    temp_dir = tempfile.mkdtemp()
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        sessions_dir = f"./companies/{company}/sessions"
        os.makedirs(sessions_dir, exist_ok=True)

        for filename in os.listdir(temp_dir):
            if filename.endswith('.session'):
                session_path = os.path.join(temp_dir, filename)
                target_path = os.path.join(sessions_dir, filename)
                shutil.move(session_path, target_path)
                rootLogger.info(f"Добавлена сессия {filename} для компании {company}")

                session_number = filename.replace('.session', '')
                json_filename = f"{session_number}.json"
                json_path = os.path.join(temp_dir, json_filename)
                if os.path.exists(json_path):
                    target_json_path = os.path.join(sessions_dir, json_filename)
                    shutil.move(json_path, target_json_path)
                    rootLogger.info(f"Добавлен JSON-файл {json_filename} для сессии {session_number}")

    finally:
        try:
            shutil.rmtree(temp_dir)
        except Exception as e:
            rootLogger.error(f"Ошибка при удалении временной папки {temp_dir}: {str(e)}")

async def check_all_sessions_on_start():
    global sessions
    if not sessions:
        log_msg = "Нет загруженных сессий для проверки."
        rootLogger.warning(log_msg)
        write_daily_log(log_msg)
        return
    
    problematic_sessions = []
    
    for session in sessions[:]:  # Копируем список, чтобы безопасно удалять элементы
        try:
            client = session.app
            log_msg = f"Проверка статуса сессии {session.filename}..."
            rootLogger.info(log_msg)
            write_daily_log(log_msg)

            # Подключаем клиента, если он не подключён
            if not client.is_connected():
                await client.connect()

            status, reason = await handle_spam_block(client)
            log_msg = f"Статус сессии {session.filename}: {status}, причина: {reason or 'нет'}"
            rootLogger.info(log_msg)
            write_daily_log(log_msg)

            if status in ["жёлтый", "красный"]:
                problematic_sessions.append(session)
                session_path = f"./companies/{session.company}/sessions/{session.filename}"
                try:
                    await move_to_spamblocked(session_path)
                    log_msg = f"Сессия {session.filename} перемещена в ./spamblocked_sessions (статус: {status})"
                    rootLogger.info(log_msg)
                    write_daily_log(log_msg)
                except Exception as e:
                    log_msg = f"Ошибка при перемещении сессии {session.filename}: {str(e)}"
                    rootLogger.error(log_msg)
                    write_daily_log(log_msg)
            else:  # Зелёный статус
                me = await client.get_me()
                session.id = me.id
                session.me = me
                log_msg = f"Сессия {session.filename} активна (ID: {me.id})"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)

        except Exception as e:
            log_msg = f"Ошибка при проверке сессии {session.filename}: {str(e)}"
            rootLogger.error(log_msg)
            write_daily_log(log_msg)
            problematic_sessions.append(session)

    # Удаляем проблемные сессии из списка активных
    for session in problematic_sessions:
        if session in sessions:
            sessions.remove(session)
            log_msg = f"Сессия {session.filename} удалена из активных сессий"
            rootLogger.info(log_msg)
            write_daily_log(log_msg)

    if not sessions:
        log_msg = "Все сессии проблемные. Завершение работы."
        rootLogger.critical(log_msg)
        write_daily_log(log_msg)
        raise SystemExit("Нет активных сессий.")

@dp.message(AddSessionState.add, (F.from_user.id == owner_id))
async def add_zip_sessions(message: atypes.Message, state: FSMContext):
    global sessions, liking_tasks
    if message.document is None or message.document.mime_type != "application/zip":
        return await message.reply("Отправьте zip-файл")

    await state.clear()
    file = await bot.get_file(message.document.file_id)
    msg = await message.reply("Сессии добавляются...")

    counter = 0
    new_sessions = []
    try:
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as temp_zip:
            zip_data = await bot.download_file(file.file_path)
            temp_zip.write(zip_data.getvalue())
            temp_zip_path = temp_zip.name

        await add_sessions(selected_company, temp_zip_path)

        session_files = [f for f in os.listdir(f"./companies/{selected_company}/sessions") if f.endswith('.session')]
        for session_file in session_files:
            full_path = f"./companies/{selected_company}/sessions/{session_file}"
            client = make_client(full_path)
            session = Session(client, session_file, selected_company)
            new_sessions.append(session)

        async def activate_session(session):
            try:
                await session.app.start(phone=f"+{session.filename[:-8]}")
                me = await session.app.get_me()
                if me:
                    session.id = me.id
                    session.me = me
                    # Проверка статуса после активации
                    status, reason = await check_session_status(session)
                    if status == "красный":
                        session_path = f"./companies/{selected_company}/sessions/{session.filename}"
                        await move_to_spamblocked(session_path)
                        log_msg = f"Сессия {session.filename} перемещена в ./spamblocked_sessions (статус: {status}, причина: {reason})"
                        rootLogger.info(log_msg)
                        write_daily_log(log_msg)
                        return False
                    # Переименование сессии
                    new_filename = f"{me.phone}.session"
                    old_path = f"./companies/{selected_company}/sessions/{session.filename}"
                    new_path = f"./companies/{selected_company}/sessions/{new_filename}"
                    if session.app.is_connected():
                        await session.app.disconnect()
                    if session.filename != new_filename:
                        shutil.move(old_path, new_path)
                        old_json_path = f"./companies/{selected_company}/sessions/{session.filename.replace('.session', '.json')}"
                        new_json_path = f"./companies/{selected_company}/sessions/{new_filename.replace('.session', '.json')}"
                        if os.path.exists(old_json_path):
                            shutil.move(old_json_path, new_json_path)
                            rootLogger.info(f"JSON-файл переименован: {session.filename.replace('.session', '.json')} -> {new_filename.replace('.session', '.json')}")
                        session.filename = new_filename
                    if company_active.get(selected_company, False):
                        task = asyncio.create_task(worker_liking_stories(session))
                        liking_tasks.append((task, session))
                        rootLogger.info(f"Запущена задача лайкинга для {session.filename} после добавления")
                    log_msg = f"Сессия {session.filename} активирована с ID {me.id} (статус: {status})"
                    rootLogger.info(log_msg)
                    write_daily_log(log_msg)
                    return True
                else:
                    raise Exception("Не удалось получить данные пользователя")
            except Exception as e:
                if session.app.is_connected():
                    await session.app.disconnect()
                log_msg = f"Ошибка активации сессии {session.filename}: {str(e)}"
                rootLogger.error(log_msg)
                write_daily_log(log_msg)
                return False

        tasks = [activate_session(session) for session in new_sessions]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for session, result in zip(new_sessions[:], results):
            if result is True:
                counter += 1
            else:
                if session.app.is_connected():
                    await session.app.disconnect()
                session_path = f"./companies/{selected_company}/sessions/{session.filename}"
                json_path = f"./companies/{selected_company}/sessions/{session.filename.replace('.session', '.json')}"
                if os.path.exists(session_path):
                    os.remove(session_path)
                if os.path.exists(json_path):
                    os.remove(json_path)
                    rootLogger.info(f"Удалён JSON-файл {session.filename.replace('.session', '.json')} для неактивированной сессии")
                new_sessions.remove(session)

        sessions.extend(new_sessions)
        if counter == 0:
            await msg.edit_text("Сессии в архиве невалидны.")
        else:
            await msg.edit_text(f"Добавлено сессий: {counter}")
        await start(message)

    except Exception as e:
        await msg.edit_text(f"Ошибка при обработке архива: {str(e)}")
        log_msg = f"Критическая ошибка при добавлении сессий: {str(e)}"
        rootLogger.error(log_msg)
        write_daily_log(log_msg)

    finally:
        if 'temp_zip_path' in locals():
            try:
                os.remove(temp_zip_path)
            except Exception as e:
                rootLogger.error(f"Ошибка при удалении временного файла {temp_zip_path}: {str(e)}")

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "add_users"))
async def change_users(callback: atypes.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Выберите способ добавления пользователей:", reply_markup=kb_add_users)
    await callback.answer()

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "change_sessions"))
async def change_sessions(callback: atypes.CallbackQuery):
    await callback.message.edit_text("Выберите действие ниже:", reply_markup=kb_change_settings)
    await callback.answer()

@dp.message(ChangeState.value, (F.from_user.id == owner_id) & (F.photo | F.text))
async def set_val(message: atypes.Message, state: FSMContext):
    data = await state.get_data()
    change = data["change"]
    if change == "avatar":
        if message.photo:
            best_photo = sorted(message.photo, key=lambda x: x.file_size, reverse=True)[0]
            file = await bot.get_file(best_photo.file_id)
            photo_io = await bot.download_file(file.file_path)
            photo_bytes = photo_io.getvalue()
            await state.update_data(value=photo_bytes)
            await state.set_state(ChangeState.select)
            await message.reply("К каким аккаунтам применить?", reply_markup=kb_all_or_select)
        else:
            await message.reply("Отправьте фото или 'отмена'")
    elif change in ["fname", "lname", "bio"]:
        await state.update_data(value=message.text)
        await state.set_state(ChangeState.select)
        await message.reply("К каким аккаунтам применить?", reply_markup=kb_all_or_select)

@dp.message((F.from_user.id == owner_id) & (F.text == "/blacklist"))
async def blacklist_command(message: atypes.Message, state: FSMContext):
    # Создаём клавиатуру для выбора компании
    kb_companies = atypes.InlineKeyboardMarkup(inline_keyboard=list(
        map(lambda x: [atypes.InlineKeyboardButton(text=x, callback_data="blacklist_company_" + x)],
            listdir("./companies"))
    ) + [[atypes.InlineKeyboardButton(text="Отмена", callback_data="blacklist_cancel")]])
    
    await message.reply("Выберите компанию для добавления в чёрный список:", reply_markup=kb_companies)
    await state.set_state(BlacklistState.company)

@dp.callback_query(BlacklistState.company, (F.from_user.id == owner_id) & (F.data.startswith("blacklist_company_")))
async def process_company_selection(callback: atypes.CallbackQuery, state: FSMContext):
    company = callback.data.replace("blacklist_company_", "")
    await state.update_data(company=company)
    await callback.message.edit_text("Отправьте список юзернеймов в столбик (например:\nuser1\nuser2\nuser3):", reply_markup=None)
    await state.set_state(BlacklistState.usernames)
    await callback.answer()

@dp.callback_query(BlacklistState.company, (F.from_user.id == owner_id) & (F.data == "blacklist_cancel"))
async def cancel_blacklist(callback: atypes.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Добавление в чёрный список отменено.", reply_markup=None)
    await callback.answer()
    await start(callback.message)

@dp.message(BlacklistState.usernames, (F.from_user.id == owner_id))
async def process_usernames(message: atypes.Message, state: FSMContext):
    data = await state.get_data()
    company = data['company']
    usernames = message.text.strip().split('\n')
    
    cursor = conn.cursor()
    added_count = 0
    
    for username in usernames:
        username = username.strip().lstrip('@')  # Убираем @ и лишние пробелы
        if not username:
            continue
        
        # Добавляем в чёрный список, если ещё не существует
        cursor.execute("""
            INSERT OR IGNORE INTO blacklist (username, company)
            VALUES (?, ?)
        """, (username, company))
        if cursor.rowcount > 0:
            added_count += 1
            rootLogger.info(f"Добавлен пользователь @{username} в чёрный список для компании {company}")
    
    conn.commit()
    await message.reply(f"Добавлено {added_count} пользователей в чёрный список компании {company}.")
    await state.clear()
    await start(message)

@dp.message((F.from_user.id == owner_id) & (F.text == "/view_blacklist"))
async def view_blacklist(message: atypes.Message):
    if not selected_company:
        await message.reply("Сначала выберите компанию.")
        return
    
    cursor = conn.cursor()
    cursor.execute("SELECT username FROM blacklist WHERE company = ?", (selected_company,))
    blacklisted_users = cursor.fetchall()
    
    if not blacklisted_users:
        await message.reply(f"Чёрный список компании {selected_company} пуст.")
    else:
        users_list = "\n".join([f"@{user[0]}" for user in blacklisted_users])
        await message.reply(f"Чёрный список компании {selected_company}:\n{users_list}")
    
    await start(message)

@dp.callback_query(ChangeState.select, (F.from_user.id == owner_id) & (F.data.startswith("select_")))
async def sel_acc(callback: atypes.CallbackQuery, state: FSMContext):
    global sessions
    try:
        ses = list(filter(lambda x: x.me is not None and x.company == selected_company, sessions))
        if not ses:
            await callback.message.edit_text("Нет активных сессий для этой компании.")
            await state.clear()
            await start(callback.message)
            return
        data = await state.get_data()
        select = data.get("select", [])
        if callback.data == "select_done":
            if not select:
                await callback.message.edit_text("Выберите хотя бы один аккаунт.")
                return
            await apply_data(callback, state)
            return
        elif callback.data == "select_all":
            await state.update_data(select=[int(session.me.id) for session in ses])
            await apply_data(callback, state)
            return
        elif callback.data == "select_selective":
            await sel_accs(callback, state)
            return
        session_id = int(callback.data[7:])
        if session_id in select:
            select.remove(session_id)
        else:
            select.append(session_id)
        await state.update_data(select=select)
        builder = InlineKeyboardBuilder()
        for session in ses:
            me = session.me
            is_selected = int(me.id) in select
            builder.row(atypes.InlineKeyboardButton(
                text=f"{'✅' if is_selected else '❌'} {me.first_name}{' ' + me.last_name if me.last_name else ''} ({'@' + me.username if me.username else '+' + me.phone_number if me.phone_number else str(me.id)} {me.id})",
                callback_data=f"select_{me.id}"))
        builder.row(atypes.InlineKeyboardButton(text="✅ Готово", callback_data="select_done"))
        builder.row(atypes.InlineKeyboardButton(text="Выбрать все", callback_data="select_all"))
        builder.row(atypes.InlineKeyboardButton(text="Выбранные аккаунты", callback_data="select_selective"))
        await callback.message.edit_reply_markup(reply_markup=builder.as_markup())
    except Exception as e:
        log_msg = f"Критическая ошибка в sel_acc: {str(e)}"
        rootLogger.error(log_msg)
        write_daily_log(log_msg)
        await callback.message.edit_text("Произошла критическая ошибка.")
    await callback.answer()

try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass