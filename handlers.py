import asyncio
import json
import os
from os import listdir
import random
import re
import shutil
import tempfile
import zipfile
from datetime import datetime
import pytz
from aiogram import types as atypes
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from telethon import functions
from telethon.tl.types import InputPhoto, PeerChannel
from telethon.tl.types.photos import Photos
from telethon.tl.functions.channels import EditTitleRequest, EditPhotoRequest
from telethon.tl.functions.photos import UploadProfilePhotoRequest, DeletePhotosRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.errors import FloodWaitError, ForbiddenError, UsernameOccupiedError
from config import (
    rootLogger, write_daily_log, bot, dp, chat_id, owner_id, selected_company, sessions,
    company_configs, company_active, liking_tasks, kb_menu, kb_change_settings,
    kb_company_config, kb_all_or_select, kb_add_users, CompanyToggleState, AddChatsState,
    BlacklistState, SendPMState, CollectViewsStatsState, CreateCompanyState,
    CompanyConfigChangeState, AddPrivateChannelState, AddSessionState, ChangeState,
    CompanyToggleCallback, company_stats, stats_lock, created_channels, F
)
from database import conn, cursor
from sessions import Session, make_client, activate_session, check_session_status, move_to_banned, move_to_spamblocked
from utils import get_stats_text, get_all_stats, make_stat_str, create_companies_keyboard
from workers import worker_liking_stories, update_stats_message

async def create_channel_for_accounts(data, message, state):
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
                channel_entity = await session.app.get_entity(PeerChannel(channel_id))
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
                    about="",
                    broadcast=True
                ))
                channel_id = channel.chats[0].id
                created_channels[selected_company].append(channel_id)
                log_msg = f"Создан канал {channel_id} для {session.filename}"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)
                if avatar:
                    file_info = await bot.get_file(avatar)
                    photo_bytes = await bot.download_file(file_info.file_path)
                    file = await session.app.upload_file(photo_bytes, file_name="avatar.jpg")
                    await session.app(functions.channels.EditPhotoRequest(
                        channel=channel_id,
                        photo=file
                    ))
                    log_msg = f"Аватарка установлена для канала {channel_id} ({session.filename})"
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
                log_msg = f"Канал {channel_id} создан для аккаунта {session.filename}: название '{name}', добавлено {len(posts)} постов"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)
                created_count += 1
        except UsernameOccupiedError as e:
            log_msg = f"Ошибка: Название канала '{name}' занято для {session.filename}: {str(e)}"
            rootLogger.error(log_msg)
            write_daily_log(log_msg)
            await bot.send_message(chat_id, log_msg)
            continue
        except Exception as e:
            log_msg = f"Ошибка при создании/редактировании канала для {session.filename}: {str(e)}"
            rootLogger.error(log_msg)
            write_daily_log(log_msg)
            await bot.send_message(chat_id, log_msg)
            continue
    report_text = f"Создано/обновлено {created_count} каналов из {len(ses)}"
    max_attempts = 1
    attempt = 0
    while attempt < max_attempts:
        try:
            current_message = await bot.get_messages(message.chat.id, message.message_id)
            if current_message.text != report_text:
                await message.edit_text(report_text)
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
    await state.clear()
    await start(message)

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
            channel_entity = await session.app.get_entity(PeerChannel(channel_id))
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

async def apply_data(callback: atypes.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    change = data.get("change")
    value = data.get("value")
    select = data.get("select", [])
    ses = [s for s in sessions if s.me and int(s.me.id) in select]
    if not ses:
        await callback.message.edit_text("Нет выбранных аккаунтов.")
        await state.clear()
        await start(callback.message)
        return
    async def apply_to_session(session):
        try:
            if not session.app.is_connected():
                log_msg = f"Клиент для сессии {session.filename} не подключён, пытаемся подключить"
                rootLogger.warning(log_msg)
                write_daily_log(log_msg)
                await session.app.connect()
            if change == "fname":
                await session.app(functions.account.UpdateProfileRequest(
                    first_name=value
                ))
                log_msg = f"Имя изменено на '{value}' для сессии {session.filename}"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)
            elif change == "lname":
                await session.app(functions.account.UpdateProfileRequest(
                    last_name=value
                ))
                log_msg = f"Фамилия изменена на '{value}' для сессии {session.filename}"
                rootLogger.info(log_msg)
                write_daily_log(log_msg)
            elif change == "bio":
                await session.app(functions.account.UpdateProfileRequest(
                    about=value
                ))
                log_msg = f"Био изменено на '{value}' для сессии {session.filename}"
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
                if isinstance(photos, Photos) and photos.photos:
                    photo_ids = [InputPhoto(
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

@dp.message((F.from_user.id == owner_id) & (F.text == "/stats_all"))
async def stats_all(message: atypes.Message):
    stats = get_all_stats()
    await message.reply(stats)

@dp.message((F.from_user.id == owner_id) & (F.text == "/cancel"))
async def cancel(message: atypes.Message, state: FSMContext):
    if await state.get_state() is not None:
        await state.clear()
        await message.reply("Отменено.")
    await start(message)

@dp.message((F.from_user.id == owner_id) & (F.text == "/blacklist"))
async def blacklist_command(message: atypes.Message, state: FSMContext):
    kb_companies = atypes.InlineKeyboardMarkup(inline_keyboard=list(
        map(lambda x: [atypes.InlineKeyboardButton(text=x, callback_data="blacklist_company_" + x)],
            listdir("./companies"))
    ) + [[atypes.InlineKeyboardButton(text="Отмена", callback_data="blacklist_cancel")]])
    await message.reply("Выберите компанию для добавления в чёрный список:", reply_markup=kb_companies)
    await state.set_state(BlacklistState.company)

@dp.message(BlacklistState.usernames, (F.from_user.id == owner_id))
async def process_usernames(message: atypes.Message, state: FSMContext):
    data = await state.get_data()
    company = data['company']
    usernames = message.text.strip().split('\n')
    cursor = conn.cursor()
    added_count = 0
    for username in usernames:
        username = username.strip().lstrip('@')
        if not username:
            continue
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

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "add_chats"))
async def start_add_chats(callback: atypes.CallbackQuery, state: FSMContext):
    if not selected_company:
        await callback.message.edit_text("Сначала выберите компанию.")
        return
    await state.set_state(AddChatsState.chats)
    await callback.message.edit_text("Отправьте файл с ссылками на чаты/каналы (одна ссылка на строку) или список ссылок текстом:")
    await callback.answer()

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
    await state.set_state(CompanyToggleState.selected_companies)
    await state.update_data(selected_companies=list(company_active.keys()))
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

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "done_toggling"))
async def done_toggling(callback: atypes.CallbackQuery, state: FSMContext):
    global liking_tasks, sessions
    for task, session in liking_tasks[:]:
        if not company_active.get(session.company, False):
            task.cancel()
            liking_tasks.remove((task, session))
            rootLogger.info(f"Задача лайкинга для {session.filename} отменена")
    for session in sessions:
        if company_active.get(session.company, False) and session.me:
            if not any(t[1] == session for t in liking_tasks):
                task = asyncio.create_task(worker_liking_stories(session))
                liking_tasks.append((task, session))
                rootLogger.info(f"Запущена задача лайкинга для {session.filename}")
    await state.clear()
    await callback.message.edit_text("Настройки лайкинга обновлены.")
    await start(callback.message)

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

@dp.callback_query((F.from_user.id == owner_id) & (F.data.startswith("sel_company_")))
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

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "change_company"))
async def change_company(callback: atypes.CallbackQuery):
    global selected_company
    selected_company = None
    kb_companies = atypes.InlineKeyboardMarkup(inline_keyboard=list(
        map(lambda x: [atypes.InlineKeyboardButton(text=x, callback_data="sel_company_" + x)],
            list(listdir("./companies"))
        )) + [[atypes.InlineKeyboardButton(text="Создать компанию", callback_data="create_company")]])
    await callback.message.edit_text("Выберите компанию:", reply_markup=kb_companies)
    await callback.answer()

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "add_sessions"))
async def add_sessions_callback(callback: atypes.CallbackQuery, state: FSMContext):
    await state.set_state(AddSessionState.add)
    await callback.message.edit_text("Отправьте zip-файл с сессиями:", reply_markup=None)
    await callback.answer()

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "change_sessions"))
async def change_sessions(callback: atypes.CallbackQuery):
    await callback.message.edit_text("Выберите действие ниже:", reply_markup=kb_change_settings)
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
            await apply_data(callback, state)
            return
        elif callback.data == "select_selective":
            builder = InlineKeyboardBuilder()
            for session in ses:
                me = session.me
                builder.row(atypes.InlineKeyboardButton(
                    text=f"❌ {me.first_name}{' ' + me.last_name if me.last_name else ''} ({'@' + me.username if me.username else '+' + me.phone_number if me.phone_number else str(me.id)} {me.id})",
                    callback_data=f"select_{me.id}"))
            builder.row(atypes.InlineKeyboardButton(text="✅ Готово", callback_data="select_done"))
            await callback.message.edit_text("Выберите аккаунты:", reply_markup=builder.as_markup())
    except Exception as e:
        log_msg = f"Критическая ошибка в sel_accs: {str(e)}"
        rootLogger.error(log_msg)
        write_daily_log(log_msg)
        await bot.send_message(chat_id, log_msg)
        if not report_sent:
            await callback.message.edit_text("Произошла ошибка при выборе аккаунтов.")
            report_sent = True
    await callback.answer()

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
            stats_text = (
                f"Сторис просмотрено: {company_stats[selected_company]['stories_viewed']}\n"
                f"Лайков поставлено: {company_stats[selected_company]['likes_set']}\n"
                f"Уникальных пользователей открыто: {len(company_stats[selected_company]['unique_users'])}\n"
                f"Уникальных пользователей со сторисами: {len(company_stats[selected_company]['unique_users_with_stories'])}\n"
                f"Каналов пройдено: {company_stats[selected_company]['channels_processed']}\n"
                f"Чатов пройдено: {company_stats[selected_company]['chats_processed']}\n\n"
                "Выберите действие ниже:"
            )
        msg = await callback.message.edit_text(stats_text, reply_markup=kb_menu)
        asyncio.create_task(update_stats_message(msg.chat.id, msg.message_id, selected_company))
    await callback.answer()

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "back_to_menu"))
async def back_to_menu(callback: atypes.CallbackQuery):
    async with stats_lock:
        stats_text = (
            f"Сторис просмотрено: {company_stats[selected_company]['stories_viewed']}\n"
            f"Лайков поставлено: {company_stats[selected_company]['likes_set']}\n"
            f"Уникальных пользователей открыто: {len(company_stats[selected_company]['unique_users'])}\n"
            f"Уникальных пользователей со сторисами: {len(company_stats[selected_company]['unique_users_with_stories'])}\n"
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

@dp.message(CollectViewsStatsState.channel_name, (F.from_user.id == owner_id))
async def process_channel_name(message: atypes.Message, state: FSMContext):
    await state.clear()
    await message.reply("Сбор статистики по привязанным каналам начался...")
    await collect_created_views_stats(message)
    await start(message)

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "add_users"))
async def change_users(callback: atypes.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Выберите способ добавления пользователей:", reply_markup=kb_add_users)
    await callback.answer()

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

@dp.callback_query((F.from_user.id == owner_id) & (F.data.startswith("select_company_")))
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

@dp.callback_query((F.from_user.id == owner_id) & (F.data == "add_channel"))
async def start_add_channel(callback: atypes.CallbackQuery, state: FSMContext):
    if not selected_company:
        await callback.message.edit_text("Сначала выберите компанию.")
        return
    await state.set_state(AddPrivateChannelState.name)
    await callback.message.edit_text("Введите название канала:")
    await callback.answer()

@dp.message(AddPrivateChannelState.name, (F.from_user.id == owner_id))
async def process_channel_name(message: atypes.Message, state: FSMContext):
    await state.update_data(name=message.text)
    await state.set_state(AddPrivateChannelState.avatar)
    await message.reply("Отправьте аватарку канала или напишите 'нет', чтобы пропустить:")
    await message.delete()

@dp.message(AddPrivateChannelState.avatar, (F.from_user.id == owner_id) & (F.photo | F.text))
async def process_channel_avatar(message: atypes.Message, state: FSMContext):
    if message.photo:
        best_photo = sorted(message.photo, key=lambda x: x.file_size, reverse=True)[0]
        file = await bot.get_file(best_photo.file_id)
        photo_io = await bot.download_file(file.file_path)
        photo_bytes = photo_io.getvalue()
        await state.update_data(avatar=best_photo.file_id)
    elif message.text.lower() == 'нет':
        await state.update_data(avatar=None)
    else:
        await message.reply("Отправьте фото или напишите 'нет'.")
        return
    await state.set_state(AddPrivateChannelState.posts)
    await message.reply("Отправьте посты для канала (текст, фото, видео или альбом) или напишите 'нет', чтобы пропустить:")

@dp.message(AddPrivateChannelState.posts, (F.from_user.id == owner_id) & (F.photo | F.text | F.video))
async def process_channel_posts(message: atypes.Message, state: FSMContext):
    data = await state.get_data()
    posts = data.get("posts", [])
    if message.text and message.text.lower() == 'нет':
        await state.update_data(posts=posts)
        await state.set_state(AddPrivateChannelState.select)
        ses = list(filter(lambda x: x.me is not None and x.company == selected_company, sessions))
        await message.reply("Выберите аккаунты:", reply_markup=kb_all_or_select)
        return
    if message.photo and message.grouped_id:
        album = []
        album_messages = await message.get_media_group()
        first_photo_caption = message.caption or ""
        first_photo_entities = message.caption_entities or []
        for msg in album_messages:
            if msg.photo:
                best_photo = sorted(msg.photo, key=lambda x: x.file_size, reverse=True)[0]
                album.append({
                    "file_id": best_photo.file_id,
                    "caption": msg.caption or "",
                    "entities": msg.caption_entities or []
                })
        posts.append({
            "type": "album",
            "photos": album
        })
    elif message.photo:
        best_photo = sorted(message.photo, key=lambda x: x.file_size, reverse=True)[0]
        posts.append({
            "type": "photo",
            "file_id": best_photo.file_id,
            "caption": message.caption or "",
            "entities": message.caption_entities or []
        })
    elif message.video:
        posts.append({
            "type": "video",
            "file_id": message.video.file_id,
            "caption": message.caption or "",
            "entities": message.caption_entities or []
        })
    elif message.text:
        posts.append({
            "type": "text",
            "content": message.text,
            "entities": message.caption_entities or []
        })
    await state.update_data(posts=posts)
    await message.reply("Отправьте ещё посты или напишите 'нет', чтобы продолжить:")

@dp.message(AddPrivateChannelState.select, (F.from_user.id == owner_id) & (F.data.in_({"select_all", "select_selective"})))
async def select_accounts_for_channel(callback: atypes.CallbackQuery, state: FSMContext):
    ses = list(filter(lambda x: x.me is not None and x.company == selected_company, sessions))
    if not ses:
        await callback.message.edit_text("Нет активных сессий для этой компании.")
        await state.clear()
        await start(callback.message)
        return
    data = await state.get_data()
    if callback.data == "select_all":
        await state.update_data(select=[int(session.me.id) for session in ses])
        await state.set_state(AddPrivateChannelState.confirm)
        await callback.message.edit_text("Подтвердите создание канала:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Подтвердить", callback_data="confirm_create")],
            [InlineKeyboardButton(text="Назад", callback_data="back_to_select")]
        ]))
    elif callback.data == "select_selective":
        builder = InlineKeyboardBuilder()
        for session in ses:
            me = session.me
            builder.row(atypes.InlineKeyboardButton(
                text=f"❌ {me.first_name}{' ' + me.last_name if me.last_name else ''} ({'@' + me.username if me.username else '+' + me.phone_number if me.phone_number else str(me.id)} {me.id})",
                callback_data=f"select_{me.id}"))
        builder.row(atypes.InlineKeyboardButton(text="✅ Готово", callback_data="select_done"))
        await callback.message.edit_text("Выберите аккаунты:", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(AddPrivateChannelState.select, (F.from_user.id == owner_id) & (F.data.startswith("select_")))
async def select_account(callback: atypes.CallbackQuery, state: FSMContext):
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
        await state.set_state(AddPrivateChannelState.confirm)
        await callback.message.edit_text("Подтвердите создание канала:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Подтвердить", callback_data="confirm_create")],
            [InlineKeyboardButton(text="Назад", callback_data="back_to_select")]
        ]))
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
    await callback.answer()