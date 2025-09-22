import asyncio
import json
import random
from datetime import datetime
import pytz
from telethon.tl.functions.stories import GetPeerStoriesRequest, ReadStoriesRequest, SendReactionRequest
from telethon.tl.types import ReactionEmoji
from telethon.errors import FloodWaitError
from config import (
    rootLogger, write_daily_log, bot, chat_id, sessions, company_stats, stats_lock,
    company_active, liking_tasks, POSITIVE_REACTIONS, LIKES_PER_ACCOUNT, LIKES_WAIT_SECONDS,
    selected_company, kb_menu
)
from database import conn, cursor
from sessions import Session, move_to_banned
from utils import get_stats_text, parse_open_chat, parse_closed_chat, determine_chat_type, process_channel, join_by_invite_link, determine_entity_type

async def is_user_in_blacklist(username, company):
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM blacklist WHERE username = ? AND company = ?", (username, company))
    result = cursor.fetchone()
    return bool(result)

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

async def worker_liking_stories(session):
    try:
        if not session.app.is_connected():
            await session.app.connect()
        cursor = conn.cursor()
        while company_active.get(session.company, False):
            cursor.execute("SELECT link FROM channels WHERE status = 'pending'")
            chats = cursor.fetchall()
            if not chats:
                rootLogger.info(f"Нет ожидающих чатов для обработки в компании {session.company}")
                await asyncio.sleep(60)
                continue
            for chat in chats:
                chat = chat[0]
                try:
                    try:
                        entity = await session.app.get_entity(chat)
                    except ValueError as e:
                        rootLogger.error(f"Чат {chat} не найден: {str(e)}")
                        cursor.execute("DELETE FROM channels WHERE link = ?", (chat,))
                        conn.commit()
                        continue
                    entity_type = await determine_entity_type(session, entity)
                    if entity_type == "chat":
                        async with stats_lock:
                            company_stats[session.company]["chats_processed"] += 1
                        users = await parse_open_chat(session, entity.id) if await determine_chat_type(session, entity.id) == "open" else await parse_closed_chat(session, entity.id)
                    elif entity_type == "channel":
                        async with stats_lock:
                            company_stats[session.company]["channels_processed"] += 1
                        users = await process_channel(session, entity.id)
                    else:
                        rootLogger.error(f"Неизвестный тип сущности для {chat}: {entity_type}")
                        cursor.execute("DELETE FROM channels WHERE link = ?", (chat,))
                        conn.commit()
                        continue
                    if not users:
                        rootLogger.info(f"Чат {chat} не содержит пользователей для обработки")
                        cursor.execute("""
                            UPDATE channels
                            SET status = 'processed', last_processed = ?
                            WHERE link = ?
                        """, (datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S.%f"), chat))
                        conn.commit()
                        await asyncio.sleep(5)
                        continue
                    processed_users = 0
                    for user_id in users:
                        try:
                            await process_user_stories(session, user_id)
                            processed_users += 1
                            await asyncio.sleep(random.uniform(3, 7))
                        except FloodWaitError as e:
                            log_msg = f"FloodWait при обработке пользователя {user_id} для {session.filename}: ждём {e.seconds} секунд"
                            rootLogger.warning(log_msg)
                            write_daily_log(log_msg)
                            await bot.send_message(chat_id, log_msg)
                            await asyncio.sleep(e.seconds)
                            continue
                        except Exception as e:
                            error_msg = str(e).lower()
                            if "user not found" in error_msg or "user is deleted" in error_msg or "chat not found" in error_msg or "channel not found" in error_msg:
                                rootLogger.warning(f"Пользователь {user_id} или чат не найден: {error_msg}")
                                continue
                            elif "banned" in error_msg or "kicked" in error_msg or "you can't do that" in error_msg:
                                log_msg = f"Аккаунт {session.filename} заблокирован в чате {chat}: {error_msg}"
                                rootLogger.error(log_msg)
                                write_daily_log(log_msg)
                                cursor.execute("DELETE FROM channels WHERE link = ?", (chat,))
                                conn.commit()
                                await bot.send_message(chat_id, log_msg)
                                return
                            else:
                                log_msg = f"Неизвестная ошибка при обработке сторис пользователя {user_id} для {session.filename}: {error_msg}"
                                rootLogger.error(log_msg)
                                write_daily_log(log_msg)
                                await asyncio.sleep(5)
                    log_msg = f"Аккаунт {session.filename} завершил обработку чата {chat}: обработано {processed_users} пользователей."
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

async def periodic_save_stats():
    while True:
        try:
            async with stats_lock:
                for company in company_stats:
                    stats_file = f"./companies/{company}/company_stats.json"
                    with open(stats_file, "w", encoding='utf-8') as f:
                        json.dump({
                            "stories_viewed": company_stats[company]["stories_viewed"],
                            "likes_set": company_stats[company]["likes_set"],
                            "unique_users": list(company_stats[company]["unique_users"]),
                            "channels_processed": company_stats[company]["channels_processed"],
                            "chats_processed": company_stats[company]["chats_processed"],
                            "unique_users_with_stories": list(company_stats[company]["unique_users_with_stories"])
                        }, f, ensure_ascii=False)
                    rootLogger.info(f"Статистика сохранена для компании {company}")
            await asyncio.sleep(600)
        except Exception as e:
            rootLogger.error(f"Ошибка при сохранении статистики: {str(e)}")
            await asyncio.sleep(60)

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