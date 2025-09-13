#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UI модуль с inline клавиатурами для Telegram бота.
Реализует полное меню согласно ТЗ с правами доступа и callback-обработчиками.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from aiogram import Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile
)
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from pathlib import Path

from handlers.commands import is_admin, ADMIN_IDS
from utils.adapter import (
    create_invite_for, get_invites, find_user, add_user_manual, 
    delete_user, export_excel, get_stats, get_inviter_list,
    get_invite_manager, get_user_manager, get_report_adapter, get_settings_manager
)
from reports.unified_report_manager import UnifiedReportManager
from db.db import get_db
from utils.logging_conf import get_logger
from utils.time_utils import format_datetime_for_report, get_almaty_now

logger = get_logger(__name__)

# Создаем роутер для UI
ui_router = Router(name="ui")

# FSM состояния для диалогов
class UIStates(StatesGroup):
    # Создание ссылки
    waiting_inviter_name = State()
    
    # Поиск пользователя
    waiting_user_search = State()
    
    # Добавление пользователя вручную
    waiting_user_id = State()
    waiting_username = State()
    waiting_user_name = State()
    waiting_inviter = State()
    
    # Удаление пользователя
    waiting_delete_confirm = State()
    waiting_delete_user = State()
    
    # Настройки
    waiting_report_time = State()
    waiting_admin_id = State()


def main_admin_kb() -> InlineKeyboardMarkup:
    """Главное меню для администраторов."""
    buttons = [
        [InlineKeyboardButton(text="➕ Создать пригласительную ссылку", callback_data="menu:create_invite")],
        [InlineKeyboardButton(text="👥 Список пригласительных", callback_data="menu:list_invites")],
        [
            InlineKeyboardButton(text="📊 Отчёты", callback_data="menu:reports"),
            InlineKeyboardButton(text="🏆 Рейтинг", callback_data="menu:rating")
        ],
        [InlineKeyboardButton(text="📤 Экспорт Excel", callback_data="menu:export_excel")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def user_menu_kb() -> InlineKeyboardMarkup:
    """Меню для обычных пользователей (пригласителей)."""
    buttons = [
        [InlineKeyboardButton(text="📥 Мои приглашённые", callback_data="user:my_invited")],
        [InlineKeyboardButton(text="🏆 Моя статистика", callback_data="user:my_stats")],
        [InlineKeyboardButton(text="📤 Скачать Excel (мои данные)", callback_data="user:my_excel")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def reports_kb() -> InlineKeyboardMarkup:
    """Подменю отчетов."""
    buttons = [
        [
            InlineKeyboardButton(text="📅 Сегодня", callback_data="reports:today"),
            InlineKeyboardButton(text="📊 Неделя", callback_data="reports:week")
        ],
        [
            InlineKeyboardButton(text="📈 Месяц", callback_data="reports:month"),
            InlineKeyboardButton(text="📒 Полный Excel", callback_data="reports:excel")
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:back")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def back_kb() -> InlineKeyboardMarkup:
    """Кнопка назад."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:back")]
    ])


def confirm_delete_kb(username: str) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения удаления."""
    buttons = [
        [
            InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"confirm:delete:{username}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="menu:back")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@ui_router.message(CommandStart())
async def handle_start_ui(message: Message, **kwargs):
    """Обработчик команды /start с UI меню."""
    user = message.from_user
    user_id = user.id
    
    logger.info(f"UI Start command from user {user_id} (@{user.username})")
    
    # Приветственное сообщение
    welcome_text = f"👋 Привет, {user.first_name}!\n\n"
    
    if is_admin(user_id):
        welcome_text += (
            "🔐 **Панель администратора**\n\n"
            "Управляйте пригласительными ссылками, пользователями и отчетами:"
        )
        keyboard = main_admin_kb()
    else:
        welcome_text += (
            "📊 **Личный кабинет пригласителя**\n\n"
            "Просматривайте свою статистику и управляйте приглашениями:"
        )
        keyboard = user_menu_kb()
    
    await message.answer(welcome_text, reply_markup=keyboard, parse_mode="Markdown")


@ui_router.message(Command("menu"))
async def handle_menu_command(message: Message, **kwargs):
    """Показать главное меню."""
    await handle_start_ui(message, **kwargs)


# === ADMIN MENU HANDLERS ===

@ui_router.callback_query(F.data == "menu:create_invite")
async def handle_create_invite_menu(callback: CallbackQuery, state: FSMContext, **kwargs):
    """Меню создания пригласительной ссылки."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    try:
        # Получаем список существующих пригласителей
        inviter_list = get_inviter_list()
        
        text = (
            "➕ **Создание пригласительной ссылки**\n\n"
            "Выберите существующего пригласителя или создайте нового:"
        )
        
        # Создаем клавиатуру с существующими пригласителями
        buttons = []
        for inviter in inviter_list[:10]:  # Ограничиваем количество
            buttons.append([InlineKeyboardButton(
                text=f"👤 {inviter}", 
                callback_data=f"invite:existing:{inviter}"
            )])
        
        buttons.append([InlineKeyboardButton(
            text="➕ Новый пригласитель", 
            callback_data="invite:new"
        )])
        buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="menu:back")])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
        await callback.answer()
        
    except Exception as e:
        logger.exception(f"Error in create invite menu: {e}")
        await callback.answer("❌ Ошибка при загрузке меню", show_alert=True)


@ui_router.callback_query(F.data.startswith("invite:existing:"))
async def handle_existing_inviter(callback: CallbackQuery, **kwargs):
    """Создание ссылки для существующего пригласителя."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    try:
        inviter_name = callback.data.split(":", 2)[2]
        
        # Создаем ссылку в канал
        bot = callback.bot
        invite_link = await create_invite_for(inviter_name, bot)
        
        text = (
            f"✅ **Ссылка создана для {inviter_name}!**\n\n"
            f"`{invite_link}`\n\n"
            "Скопируйте ссылку и отправьте пригласителю."
        )
        
        # Добавляем кнопки для копирования и возврата
        buttons = [
            [InlineKeyboardButton(text="🔗 Скопировать ссылку", url=invite_link)],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:create_invite")]
        ]
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
        await callback.answer("✅ Ссылка создана!")
        
        logger.info(f"Created invite link for {inviter_name} by admin {callback.from_user.id}")
        
    except Exception as e:
        logger.exception(f"Error creating invite for existing inviter: {e}")
        await callback.answer("❌ Ошибка при создании ссылки", show_alert=True)


@ui_router.callback_query(F.data == "invite:new")
async def handle_new_inviter(callback: CallbackQuery, state: FSMContext, **kwargs):
    """Запрос имени нового пригласителя."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    text = (
        "✏️ **Новый пригласитель**\n\n"
        "Введите имя нового пригласителя:"
    )
    
    await callback.message.edit_text(text, reply_markup=back_kb(), parse_mode="Markdown")
    await state.set_state(UIStates.waiting_inviter_name)
    await callback.answer()


@ui_router.message(UIStates.waiting_inviter_name)
async def handle_inviter_name_input(message: Message, state: FSMContext, **kwargs):
    """Обработка ввода имени пригласителя."""
    if not is_admin(message.from_user.id):
        await message.reply("❌ Недостаточно прав")
        return
    
    try:
        inviter_name = message.text.strip()
        
        if not inviter_name or len(inviter_name) > 50:
            await message.reply(
                "❌ Имя должно быть от 1 до 50 символов. Попробуйте еще раз:",
                reply_markup=back_kb()
            )
            return
        
        # Создаем ссылку в канал
        bot = message.bot
        invite_link = await create_invite_for(inviter_name, bot)
        
        text = (
            f"✅ **Ссылка создана для {inviter_name}!**\n\n"
            f"`{invite_link}`\n\n"
            "Скопируйте ссылку и отправьте пригласителю."
        )
        
        buttons = [
            [InlineKeyboardButton(text="🔗 Скопировать ссылку", url=invite_link)],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu:back")]
        ]
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await message.answer(text, reply_markup=keyboard, parse_mode="Markdown")
        await state.clear()
        
        logger.info(f"Created new inviter {inviter_name} by admin {message.from_user.id}")
        
    except Exception as e:
        logger.exception(f"Error creating new inviter: {e}")
        await message.reply("❌ Ошибка при создании ссылки. Попробуйте еще раз.")


@ui_router.callback_query(F.data == "menu:list_invites")
async def handle_list_invites(callback: CallbackQuery, **kwargs):
    """Показать список пригласительных ссылок."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    try:
        invites = get_invites()
        
        if not invites:
            text = "📝 **Список пригласительных ссылок**\n\nСписок пуст. Создайте первую ссылку!"
            await callback.message.edit_text(text, reply_markup=back_kb(), parse_mode="Markdown")
            await callback.answer()
            return
        
        from datetime import datetime
        current_time = get_almaty_now().strftime("%H:%M")
        text = f"👥 **Список пригласительных ссылок** (обновлено {current_time})\n\n"
        
        # Показываем каждую ссылку отдельно с кнопкой удаления
        buttons = []
        for i, invite in enumerate(invites[:15], 1):  # Ограничиваем до 15 ссылок
            name = invite['name']
            total = invite.get('total_invited', 0)
            active = invite.get('active_now', 0)
            retention = invite.get('retention_rate', 0)
            invite_id = invite.get('id', i)
            
            # Отображаем информацию о ссылке
            text += f"**{i}. {name}**\n"
            text += f"👥 Приглашено: {total} | Активных: {active} | Удержание: {retention}%\n"
            text += f"🔗 `{invite.get('invite_link', 'Нет ссылки')}`\n\n"
            
            # Добавляем кнопку удаления для каждой ссылки
            buttons.append([
                InlineKeyboardButton(
                    text=f"🗑 Удалить {name}",
                    callback_data=f"delete:invite:{invite_id}:{name}"
                )
            ])
        
        # Добавляем общие кнопки управления
        buttons.extend([
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu:list_invites")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:back")]
        ])
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
        await callback.answer()
        
    except Exception as e:
        logger.exception(f"Error listing invites: {e}")
        await callback.answer("❌ Ошибка при загрузке списка", show_alert=True)


@ui_router.callback_query(F.data == "menu:reports")
async def handle_reports_menu(callback: CallbackQuery, **kwargs):
    """Подменю отчетов."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    text = (
        "📊 **Отчёты**\n\n"
        "Выберите период для отчета:"
    )
    
    await callback.message.edit_text(text, reply_markup=reports_kb(), parse_mode="Markdown")
    await callback.answer()


@ui_router.callback_query(F.data.startswith("reports:"))
async def handle_reports(callback: CallbackQuery, **kwargs):
    """Обработка отчетов."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    try:
        report_type = callback.data.split(":")[1]
        
        if report_type == "excel":
            # Полный Excel отчет
            await callback.answer("📊 Генерирую Excel отчет...")
            
            file_path = export_excel("full")
            if not file_path or not Path(file_path).exists():
                await callback.message.answer("❌ Ошибка при создании отчета")
                return
            
            document = FSInputFile(Path(file_path), filename=Path(file_path).name)
            await callback.message.answer_document(
                document=document,
                caption=f"📊 Полный отчет Excel\n🕐 Создан: {format_datetime_for_report(get_almaty_now())}"
            )
            
        else:
            # Текстовый отчет
            await callback.answer("📊 Загружаю отчет...")
            
            stats = get_stats(report_type)
            
            period_names = {
                "today": "сегодня",
                "week": "за неделю", 
                "month": "за месяц"
            }
            
            period_name = period_names.get(report_type, report_type)
            
            text = f"📊 **Отчет {period_name}**\n\n"
            text += f"👥 Новые подписки: {stats.get('total_subscriptions', 0)}\n"
            text += f"❌ Отписки: {stats.get('total_unsubscriptions', 0)}\n" 
            text += f"📈 Чистый прирост: {stats.get('net_growth', 0)}\n"
            text += f"👤 Уникальных пользователей: {stats.get('unique_subscribers', 0)}\n"
            
            await callback.message.answer(text, reply_markup=reports_kb(), parse_mode="Markdown")
        
    except Exception as e:
        logger.exception(f"Error handling report {report_type}: {e}")
        await callback.answer("❌ Ошибка при создании отчета", show_alert=True)


@ui_router.callback_query(F.data == "menu:rating")
async def handle_rating(callback: CallbackQuery, **kwargs):
    """Показать рейтинг пригласителей."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    try:
        rating_data = get_report_adapter().get_rating()
        
        from datetime import datetime
        current_time = get_almaty_now().strftime("%H:%M")
        text = f"🏆 **Рейтинг пригласителей** (обновлено {current_time})\n\n"
        
        if not rating_data:
            text += "Данных пока нет."
        else:
            for i, data in enumerate(rating_data[:10], 1):  # Топ-10
                name = data.get('inviter_name', 'Неизвестный')
                total = data.get('total_invited', 0)
                active = data.get('currently_subscribed', 0)
                retention = data.get('retention_percentage', 0)
                
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                text += f"{medal} **{name}**: {total} приглашено, {active} активных ({retention}%)\n"
        
        buttons = [
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu:rating")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:back")]
        ]
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
        await callback.answer()
        
    except Exception as e:
        logger.exception(f"Error showing rating: {e}")
        await callback.answer("❌ Ошибка при загрузке рейтинга", show_alert=True)


@ui_router.callback_query(F.data == "menu:find_user")
async def handle_find_user_menu(callback: CallbackQuery, state: FSMContext, **kwargs):
    """Меню поиска пользователя."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    text = (
        "🔎 **Поиск пользователя**\n\n"
        "Введите @username или user_id для поиска:"
    )
    
    await callback.message.edit_text(text, reply_markup=back_kb(), parse_mode="Markdown")
    await state.set_state(UIStates.waiting_user_search)
    await callback.answer()


@ui_router.message(UIStates.waiting_user_search)
async def handle_user_search(message: Message, state: FSMContext, **kwargs):
    """Обработка поиска пользователя."""
    if not is_admin(message.from_user.id):
        await message.reply("❌ Недостаточно прав")
        return
    
    try:
        search_query = message.text.strip()
        user_data = find_user(search_query)
        
        if not user_data:
            await message.reply(
                f"❌ Пользователь '{search_query}' не найден.\n\nПопробуйте еще раз:",
                reply_markup=back_kb()
            )
            return
        
        # Формируем информацию о пользователе
        text = f"👤 **Информация о пользователе**\n\n"
        text += f"**ID:** `{user_data['tg_user_id']}`\n"
        text += f"**Username:** @{user_data.get('username', 'не указан')}\n"
        text += f"**Имя:** {user_data.get('name', 'не указано')}\n"
        text += f"**Статус:** {user_data.get('current_status', 'неизвестен')}\n"
        text += f"**Подписок:** {user_data.get('subscribe_count', 0)}\n"
        text += f"**Отписок:** {user_data.get('unsubscribe_count', 0)}\n"
        text += f"**Последняя активность:** {user_data.get('last_activity', 'неизвестно')}\n\n"
        
        # История (последние 5 событий)
        history = user_data.get('history', [])[:5]
        if history:
            text += "**📋 Последние события:**\n"
            for event in history:
                event_type = "➕" if event['event_type'] == 'subscribe' else "➖"
                inviter = f" (от {event['inviter_name']})" if event['inviter_name'] else ""
                text += f"{event_type} {event['event_type']}{inviter} - {event['event_time'][:10]}\n"
        
        buttons = [
            [InlineKeyboardButton(text="🗂 Экспорт истории", callback_data=f"export:user:{user_data['tg_user_id']}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu:back")]
        ]
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await message.answer(text, reply_markup=keyboard, parse_mode="Markdown")
        await state.clear()
        
    except Exception as e:
        logger.exception(f"Error searching user: {e}")
        await message.reply("❌ Ошибка при поиске пользователя")


@ui_router.callback_query(F.data.startswith("delete:invite:"))
async def handle_delete_invite_callback(callback: CallbackQuery, **kwargs):
    """Подтверждение удаления пригласительной ссылки."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    try:
        # Парсим данные: delete:invite:ID:name
        parts = callback.data.split(":", 3)
        if len(parts) < 4:
            await callback.answer("❌ Ошибка формата данных", show_alert=True)
            return
        
        invite_id = parts[2]
        name = parts[3]
        
        # Подтверждение удаления
        text = f"⚠️ **Подтвердите удаление**\n\n"
        text += f"Вы действительно хотите удалить пригласительную ссылку для **{name}**?\n\n"
        text += f"ID: `{invite_id}`\n\n"
        text += "⚠️ Это действие необратимо!"
        
        buttons = [
            [
                InlineKeyboardButton(
                    text="✅ Да, удалить",
                    callback_data=f"confirm:delete:invite:{invite_id}:{name}"
                ),
                InlineKeyboardButton(
                    text="❌ Отмена",
                    callback_data="menu:list_invites"
                )
            ]
        ]
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
        await callback.answer()
        
    except Exception as e:
        logger.exception(f"Error in delete invite callback: {e}")
        await callback.answer("❌ Ошибка при обработке запроса", show_alert=True)


@ui_router.callback_query(F.data.startswith("confirm:delete:invite:"))
async def handle_confirm_delete_invite(callback: CallbackQuery, **kwargs):
    """Окончательное удаление пригласительной ссылки."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    try:
        # Парсим данные: confirm:delete:invite:ID:name
        parts = callback.data.split(":", 4)
        if len(parts) < 5:
            await callback.answer("❌ Ошибка формата данных", show_alert=True)
            return
        
        invite_id = parts[3]
        name = parts[4]
        
        # Удаляем ссылку
        invite_manager = get_invite_manager()
        success = invite_manager.delete_invite(invite_id)
        
        if success:
            text = f"✅ **Ссылка удалена**\n\n"
            text += f"Пригласительная ссылка для **{name}** (ID: {invite_id}) успешно удалена."
            
            await callback.answer("✅ Ссылка удалена!", show_alert=True)
            logger.info(f"Admin {callback.from_user.id} deleted invite {invite_id} for {name}")
            
        else:
            text = f"❌ **Ошибка удаления**\n\n"
            text += f"Не удалось удалить ссылку для **{name}** (ID: {invite_id}).\n\n"
            text += "Возможно, ссылка уже была удалена или произошла ошибка базы данных."
            
            await callback.answer("❌ Ошибка удаления", show_alert=True)
        
        buttons = [
            [InlineKeyboardButton(text="📋 К списку ссылок", callback_data="menu:list_invites")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu:back")]
        ]
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
        
    except Exception as e:
        logger.exception(f"Error confirming delete invite: {e}")
        await callback.answer("❌ Ошибка при удалении ссылки", show_alert=True)


@ui_router.callback_query(F.data == "menu:export_excel")
async def handle_export_excel(callback: CallbackQuery, **kwargs):
    """Экспорт Excel файла."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    try:
        await callback.answer("📊 Генерирую Excel файл...")
        
        file_path = export_excel("full")
        if not file_path or not Path(file_path).exists():
            await callback.message.answer("❌ Ошибка при создании файла")
            return
        
        document = FSInputFile(Path(file_path), filename=Path(file_path).name)
        await callback.message.answer_document(
            document=document,
            caption=f"📊 Полная база данных Excel\n🕐 Создан: {format_datetime_for_report(get_almaty_now())}"
        )
        
    except Exception as e:
        logger.exception(f"Error exporting excel: {e}")
        await callback.answer("❌ Ошибка при экспорте", show_alert=True)


# === USER MENU HANDLERS (для обычных пользователей) ===

@ui_router.callback_query(F.data == "user:my_invited")
async def handle_my_invited(callback: CallbackQuery, **kwargs):
    """Показать приглашенных пользователем."""
    user_id = callback.from_user.id
    
    try:
        # Получаем данные о приглашенных этим пользователем
        user_manager = get_user_manager()
        db = get_db()
        
        with db.get_connection() as conn:
            # Найдем пригласителя по user_id
            cursor = conn.execute(
                "SELECT id, name FROM inviters WHERE name = ? OR invite_link LIKE ?",
                (callback.from_user.username or str(user_id), f"%{user_id}%")
            )
            inviter_row = cursor.fetchone()
            
            if not inviter_row:
                text = "❌ **Вы не являетесь пригласителем**\n\nВы не создавали пригласительных ссылок."
                await callback.message.edit_text(text, reply_markup=user_menu_kb(), parse_mode="Markdown")
                await callback.answer()
                return
            
            inviter_id = inviter_row[0]
            inviter_name = inviter_row[1]
            
            # Получаем приглашенных
            cursor = conn.execute(
                """SELECT j.tg_user_id, j.username, j.name, j.event_time, j.status
                   FROM journal j
                   WHERE j.inviter_id = ? AND j.event_type = 'subscribe'
                   ORDER BY j.event_time DESC
                   LIMIT 20""",
                (inviter_id,)
            )
            invited_users = [dict(row) for row in cursor.fetchall()]
        
        from datetime import datetime
        current_time = get_almaty_now().strftime("%H:%M")
        text = f"📥 **Мои приглашенные ({len(invited_users)})** (обновлено {current_time})\n\n"
        
        if not invited_users:
            text += "Пока никого не пригласили."
        else:
            for i, user in enumerate(invited_users[:10], 1):  # Показываем первых 10
                username_display = f"@{user['username']}" if user['username'] else f"ID:{user['tg_user_id']}"
                name_display = user['name'] or 'без имени'
                status_emoji = "✅" if user['status'] == 'subscribed' else "❌"
                date_str = user['event_time'][:10]  # YYYY-MM-DD
                
                text += f"{i}. {status_emoji} {name_display} ({username_display}) - {date_str}\n"
            
            if len(invited_users) > 10:
                text += f"\n... и еще {len(invited_users) - 10} человек"
        
        buttons = [
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="user:my_invited")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:back")]
        ]
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
        await callback.answer()
        
    except Exception as e:
        logger.exception(f"Error showing my invited: {e}")
        await callback.answer("❌ Ошибка при загрузке данных", show_alert=True)


@ui_router.callback_query(F.data == "user:my_stats")
async def handle_my_stats(callback: CallbackQuery, **kwargs):
    """Показать статистику пользователя."""
    user_id = callback.from_user.id
    
    try:
        db = get_db()
        
        with db.get_connection() as conn:
            # Найдем пригласителя
            cursor = conn.execute(
                "SELECT id, name FROM inviters WHERE name = ? OR invite_link LIKE ?",
                (callback.from_user.username or str(user_id), f"%{user_id}%")
            )
            inviter_row = cursor.fetchone()
            
            if not inviter_row:
                text = "❌ **Статистика недоступна**\n\nВы не являетесь пригласителем."
                await callback.message.edit_text(text, reply_markup=user_menu_kb(), parse_mode="Markdown")
                await callback.answer()
                return
            
            inviter_id = inviter_row[0]
            inviter_name = inviter_row[1]
            
            # Статистика
            # Всего приглашено
            cursor = conn.execute(
                "SELECT COUNT(*) FROM journal WHERE inviter_id = ? AND event_type = 'subscribe'",
                (inviter_id,)
            )
            total_invited = cursor.fetchone()[0]
            
            # Активных сейчас
            cursor = conn.execute(
                """SELECT COUNT(DISTINCT j1.tg_user_id) FROM journal j1 
                   WHERE j1.inviter_id = ? AND j1.event_type = 'subscribe'
                   AND j1.tg_user_id NOT IN (
                       SELECT j2.tg_user_id FROM journal j2 
                       WHERE j2.event_type = 'unsubscribe' AND j2.event_time > j1.event_time
                   )""",
                (inviter_id,)
            )
            active_now = cursor.fetchone()[0]
            
            # Ушли
            left_count = total_invited - active_now
            
            # Процент удержания
            retention_rate = (active_now / total_invited * 100) if total_invited > 0 else 0
            
            # За последние 7 дней
            from datetime import datetime, timedelta
            week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            cursor = conn.execute(
                "SELECT COUNT(*) FROM journal WHERE inviter_id = ? AND event_type = 'subscribe' AND event_time >= ?",
                (inviter_id, week_ago)
            )
            week_invited = cursor.fetchone()[0]
        
        from datetime import datetime
        current_time = get_almaty_now().strftime("%H:%M")
        text = f"🏆 **Моя статистика** (обновлено {current_time})\n\n"
        text += f"👤 **Пригласитель:** {inviter_name}\n\n"
        text += f"📊 **Всего приглашено:** {total_invited}\n"
        text += f"✅ **Активных сейчас:** {active_now}\n"
        text += f"❌ **Ушли:** {left_count}\n"
        text += f"📈 **% удержания:** {retention_rate:.1f}%\n\n"
        text += f"📅 **За неделю:** +{week_invited} новых\n"
        
        # Оценка
        if retention_rate >= 80:
            text += "\n🥇 **Отличная работа!** Высокий уровень удержания!"
        elif retention_rate >= 60:
            text += "\n🥈 **Хорошо!** Стабильные результаты."
        elif retention_rate >= 40:
            text += "\n🥉 **Неплохо!** Есть место для улучшения."
        else:
            text += "\n📈 **Есть потенциал!** Работайте над качеством аудитории."
        
        buttons = [
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="user:my_stats")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:back")]
        ]
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
        await callback.answer()
        
    except Exception as e:
        logger.exception(f"Error showing my stats: {e}")
        await callback.answer("❌ Ошибка при загрузке статистики", show_alert=True)


@ui_router.callback_query(F.data == "user:my_excel")
async def handle_my_excel(callback: CallbackQuery, **kwargs):
    """Скачать Excel с данными пользователя."""
    user_id = callback.from_user.id
    
    try:
        await callback.answer("📊 Генерирую ваш отчет...")
        
        db = get_db()
        
        with db.get_connection() as conn:
            # Найдем пригласителя
            cursor = conn.execute(
                "SELECT id, name FROM inviters WHERE name = ? OR invite_link LIKE ?",
                (callback.from_user.username or str(user_id), f"%{user_id}%")
            )
            inviter_row = cursor.fetchone()
            
            if not inviter_row:
                await callback.message.answer(
                    "❌ **Excel недоступен**\n\nВы не являетесь пригласителем.",
                    reply_markup=user_menu_kb(),
                    parse_mode="Markdown"
                )
                return
            
            inviter_id = inviter_row[0]
            inviter_name = inviter_row[1]
            
            # Получаем данные пользователя
            cursor = conn.execute(
                """SELECT j.event_time, j.event_type, j.tg_user_id, j.username, j.name, j.status, j.note
                   FROM journal j
                   WHERE j.inviter_id = ?
                   ORDER BY j.event_time DESC""",
                (inviter_id,)
            )
            user_data = [dict(row) for row in cursor.fetchall()]
        
        if not user_data:
            await callback.message.answer(
                "❌ **Нет данных для экспорта**\n\nВы еще никого не пригласили.",
                reply_markup=user_menu_kb(),
                parse_mode="Markdown"
            )
            return
        
        # Создаем Excel файл
        import pandas as pd
        from pathlib import Path
        
        # Создаем директорию для отчетов если не существует
        reports_dir = Path("reports/user_exports")
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        filename = f"my_data_{inviter_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        file_path = reports_dir / filename
        
        # Конвертируем в DataFrame
        df = pd.DataFrame(user_data)
        
        # Переименовываем колонки на русский
        df.columns = ['Дата/время', 'Действие', 'User ID', 'Username', 'Имя', 'Статус', 'Примечание']
        
        # Сохраняем в Excel
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Мои приглашенные', index=False)
        
        # Отправляем файл
        document = FSInputFile(file_path, filename=filename)
        await callback.message.answer_document(
            document=document,
            caption=f"📊 **Ваши данные**\n👤 Пригласитель: {inviter_name}\n📅 Экспорт: {format_datetime_for_report(get_almaty_now())}"
        )
        
        # Удаляем временный файл
        file_path.unlink(missing_ok=True)
        
    except Exception as e:
        logger.exception(f"Error generating user excel: {e}")
        await callback.answer("❌ Ошибка при создании файла", show_alert=True)


@ui_router.callback_query(F.data == "download:unified_excel")
async def handle_download_unified_excel(callback: CallbackQuery, **kwargs):
    """Скачать unified Excel файл (subscribers_report.xlsx)."""
    try:
        # Initialize unified report manager
        db = get_db()
        unified_manager = UnifiedReportManager(db)
        
        # Get Excel file path
        file_path = unified_manager.export_excel_file()
        
        if not Path(file_path).exists():
            await callback.answer("❌ Excel файл не найден", show_alert=True)
            return
        
        # Send file
        document = FSInputFile(file_path, filename="subscribers_report.xlsx")
        await callback.message.answer_document(
            document=document,
            caption="📊 **Отчёт subscribers_report.xlsx**\n\nСодержит:\n• История — все события\n• Статистика — сводка по пригласителям\n• Дневные отчёты",
            parse_mode="Markdown"
        )
        
        await callback.answer("✅ Файл отправлен")
        
    except Exception as e:
        logger.exception(f"Error downloading unified Excel: {e}")
        await callback.answer("❌ Ошибка при скачивании файла", show_alert=True)


@ui_router.callback_query(F.data == "menu:back")
async def handle_back_to_main(callback: CallbackQuery, state: FSMContext, **kwargs):
    """Возврат в главное меню."""
    await state.clear()  # Очищаем состояние
    
    if is_admin(callback.from_user.id):
        text = "🔐 **Панель администратора**\n\nВыберите действие:"
        keyboard = main_admin_kb()
    else:
        text = "📊 **Личный кабинет**\n\nВыберите действие:"
        keyboard = user_menu_kb()
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    await callback.answer()


# === ERROR HANDLER ===

@ui_router.callback_query()
async def handle_unknown_callback(callback: CallbackQuery, **kwargs):
    """Обработчик неизвестных callback'ов."""
    logger.warning(f"Unknown callback: {callback.data} from user {callback.from_user.id}")
    await callback.answer("❌ Неизвестная команда", show_alert=True)