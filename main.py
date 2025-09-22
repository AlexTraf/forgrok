import asyncio
import os
import pytz
from aiogram.types import BotCommand
from config import rootLogger, write_daily_log, bot, dp, sessions, company_active, liking_tasks, scheduler, chat_id
from database import init_db
from sessions import Session, make_client, activate_session, check_session_status, move_to_banned, move_to_spamblocked
from workers import worker_liking_stories, periodic_save_stats, remove_inactive_sessions

async def set_bot_commands(bot):
    commands = [
        BotCommand(command="/start", description="Начать работу с ботом"),
        BotCommand(command="/stats_all", description="Показать статистику по всем компаниям"),
        BotCommand(command="/cancel", description="Отменить текущее действие"),
        BotCommand(command="/blacklist", description="Добавить пользователей в чёрный список"),
        BotCommand(command="/view_blacklist", description="Просмотреть чёрный список")
    ]
    await bot.set_my_commands(commands)
    rootLogger.info("Команды бота установлены")
    write_daily_log("Команды бота установлены")

async def check_all_sessions_on_start():
    global sessions
    if not sessions:
        log_msg = "Нет загруженных сессий для проверки."
        rootLogger.warning(log_msg)
        write_daily_log(log_msg)
        return
    problematic_sessions = []
    for session in sessions[:]:
        try:
            client = session.app
            log_msg = f"Проверка статуса сессии {session.filename}..."
            rootLogger.info(log_msg)
            write_daily_log(log_msg)
            if not client.is_connected():
                await client.connect()
            status, reason = await check_session_status(client)
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
            else:
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

async def main():
    global sessions
    init_db()
    proxies = []
    for company in os.listdir("./companies"):
        company_path = f"./companies/{company}/sessions"
        if not os.path.exists(company_path):
            continue
        for filename in os.listdir(company_path):
            if filename.endswith('.session'):
                full_path = os.path.join(company_path, filename)
                client = make_client(full_path)
                sessions.append(Session(client, filename, company))
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
        problematic_sessions = []
        for session in sessions[:]:
            try:
                if not session.app.is_connected():
                    await session.app.connect()
                log_msg = f"Проверка статуса сессии {session.filename} через @vpilotnotifybot..."
                rootLogger.info(log_msg)
                write_daily_log(log_msg)
                status, reason = await check_session_status(session)
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
                    me = await session.app.get_me()
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

try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass