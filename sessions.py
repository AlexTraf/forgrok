import asyncio
import os
import random
import shutil
from datetime import datetime
import pytz
from telethon import TelegramClient, events, types  # Добавлено types
from telethon.errors import FloodWaitError
from config import (
    rootLogger, write_daily_log, api_id, api_hash, FROZE_CHECK_BOT_USERNAME, sessions,
    bot, chat_id, company_active, liking_tasks, DEVICES_INFO, proxies  # Добавлено proxies
)
from workers import worker_liking_stories

# Класс сессии
class Session:
    def __init__(self, client, filename, company):
        self.app = client
        self.filename = filename
        self.company = company
        self.id = None
        self.me = None
        self.blocked = False
        self.sent_appelation = False
        self.unblocked_at = None
        self.story_likes_today = 0

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
    attempt = 0  # Добавлено для устранения ВЖЛ, потенциальная ошибка в оригинале
    max_attempts = 1  # Добавлено для устранения ВЖЛ, потенциальная ошибка в оригинале
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

async def check_session_status(session):
    try:
        if not session.app.is_connected():
            log_msg = f"Клиент для сессии {session.filename} не подключён, пытаемся подключить"
            rootLogger.warning(log_msg)
            write_daily_log(log_msg)
            await session.app.connect()
        return await handle_spam_block(session.app)
    except Exception as e:
        log_msg = f"Ошибка проверки статуса сессии {session.filename}: {str(e)}"
        rootLogger.error(log_msg)
        write_daily_log(log_msg)
        return "жёлтый", str(e)

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

async def move_to_spamblocked(session_path: str):
    filename = os.path.basename(session_path)
    spamblocked_path = os.path.join("./spamblocked_sessions", filename)
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