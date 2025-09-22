import json
import os
import logging
import asyncio
from datetime import datetime
import pytz
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from telethon import functions
from telethon.tl.types import ChannelParticipant, Chat, Channel, PeerUser
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.errors import UserNotParticipantError, ForbiddenError, ValueError, FloodWaitError
from config import (
    rootLogger, sessions, company_stats, stats_lock, selected_company, company_configs,
    company_active, bot, chat_id, TRANS_TABLE, default_company_config, created_channels, users_paused
)

def write_daily_log(message):
    global daily_log_file, dailyLogHandler
    today = datetime.now().strftime("%Y-%m-%d")
    new_log_file = f"daily_log_{today}.txt"
    if new_log_file != daily_log_file:
        daily_log_file = new_log_file
        dailyLogHandler.close()
        dailyLogHandler = logging.FileHandler(daily_log_file, "a", encoding="utf-8")
        dailyLogHandler.setLevel(logging.INFO)
        dailyLogHandler.setFormatter(logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s]: %(message)s (%(filename)s:%(lineno)d)'))
        rootLogger.handlers = [h for h in rootLogger.handlers if not isinstance(h, logging.FileHandler) or h.baseFilename != daily_log_file]
        rootLogger.addHandler(dailyLogHandler)

def transliterate(text):
    result = ''
    for char in text.lower():
        result += TRANS_TABLE.get(char, char)
    return ''.join(c for c in result if c.isalpha() or c.isspace()).replace(" ", "")

async def get_stats_text(company):
    ses = list(filter(lambda x: x.me is not None and x.company == company, sessions))
    async with stats_lock:
        stats = []
        stats.append(f"Компания: {company}")
        stats.append(f"Аккаунтов: {len(ses)}")
        stats.append(f"Лайков сторис сегодня: {sum(s.story_likes_today for s in ses)}")
        stats.append(f"Сторис просмотрено: {company_stats[company]['stories_viewed']}")
        stats.append(f"Лайков поставлено: {company_stats[company]['likes_set']}")
        stats.append(f"Уникальных пользователей открыто: {len(company_stats[company]['unique_users'])}")
        stats.append(f"Уникальных пользователей со сторисами: {len(company_stats[company]['unique_users_with_stories'])}")
        stats.append(f"Каналов пройдено: {company_stats[company]['channels_processed']}")
        stats.append(f"Чатов пройдено: {company_stats[company]['chats_processed']}")
        stats.append("")
        stats.append("Выберите действие ниже:")
    return "\n".join(stats)

def get_all_stats():
    total_accounts = 0
    stats = []
    for company in company_configs.keys():
        company_sessions = list(filter(lambda x: x.me is not None and x.company == company, sessions))
        if company not in company_stats:
            company_stats[company] = {
                "stories_viewed": 0,
                "likes_set": 0,
                "unique_users": set(),
                "channels_processed": 0,
                "chats_processed": 0,
                "unique_users_with_stories": set()
            }
        accounts_count = len(company_sessions)
        if not accounts_count:
            continue
        stats.append(f"Компания: {company}")
        stats.append(f"Аккаунтов: {accounts_count}")
        stats.append(f"Лайков сторис сегодня: {sum(s.story_likes_today for s in company_sessions)}")
        stats.append(f"Сторис просмотрено: {company_stats[company]['stories_viewed']}")
        stats.append(f"Лайков поставлено: {company_stats[company]['likes_set']}")
        stats.append(f"Уникальных пользователей открыто: {len(company_stats[company]['unique_users'])}")
        stats.append(f"Уникальных пользователей со сторисами: {len(company_stats[company]['unique_users_with_stories'])}")
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
    stats.append(f"Уникальных пользователей со сторисами: {len(set.union(*(stats['unique_users_with_stories'] for stats in company_stats.values())))}")
    stats.append(f"Каналов пройдено: {sum(stats['channels_processed'] for stats in company_stats.values())}")
    stats.append(f"Чатов пройдено: {sum(stats['chats_processed'] for stats in company_stats.values())}")
    return "\n".join(stats)

def make_stat_str(session):
    blocked = "☢️ Заблокирован" if session.blocked else f"⚠️ Разблокируется в {session.unblocked_at}" if session.unblocked_at else "🗒 Ждёт апелляции" if session.sent_appelation else "✅ Свободен"
    username = session.me.username if session.me.username else session.me.phone_number if session.me.phone_number else session.me.id
    premium = "✅ Есть премиум" if session.me.is_premium else "❌ Нет премиума"
    return f"{session.filename} - {blocked} - {username} - {premium} - Лайков сторис сегодня: {session.story_likes_today}"

def create_companies_keyboard(selected_companies: set):
    builder = InlineKeyboardBuilder()
    for company in company_configs.keys():
        is_selected = company in selected_companies
        builder.row(InlineKeyboardButton(
            text=f"{'✅' if is_selected else '❌'} {company}",
            callback_data=f"select_company_{company}"
        ))
    builder.row(InlineKeyboardButton(text="✅ Готово", callback_data="select_companies_done"))
    builder.row(InlineKeyboardButton(text="Запустить все", callback_data="select_all_companies"))
    return builder.as_markup()

async def determine_entity_type(session, entity):
    try:
        if isinstance(entity, Chat):
            return "chat"
        elif isinstance(entity, Channel):
            if entity.broadcast:
                return "channel"
            elif entity.megagroup:
                return "chat"
            else:
                participant = await session.app(functions.channels.GetParticipantRequest(
                    channel=entity,
                    participant=await session.app.get_me()
                ))
                if participant.participant._constructor_id == ChannelParticipant._constructor_id:
                    return "chat"
                return "channel"
        return "unknown"
    except Exception as e:
        rootLogger.error(f"Ошибка определения типа сущности {entity.id}: {str(e)}")
        return "unknown"

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
                if message.from_id and isinstance(message.from_id, PeerUser):
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
        rootLogger.error(f"Ошибка вступления в чат по ссылке {invite_link} для {session.filename}: {str(e)}")
        return None