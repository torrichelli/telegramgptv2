#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FSM flows для сложных диалогов в Telegram боте.
Реализует многошаговые диалоги для создания, управления и настройки.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from handlers.commands import is_admin
from handlers.ui import UIStates, back_kb, main_admin_kb, confirm_delete_kb
from utils.adapter import add_user_manual, delete_user, find_user, get_settings_manager
from utils.logging_conf import get_logger

logger = get_logger(__name__)

# Создаем роутер для flows
flows_router = Router(name="flows")


# === MANUAL ADD USER FLOW ===

@flows_router.callback_query(F.data == "menu:manual_add")
async def start_manual_add_flow(callback: CallbackQuery, state: FSMContext, **kwargs):
    """Начать процесс добавления пользователя вручную."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    text = (
        "📝 **Добавление пользователя вручную**\n\n"
        "Шаг 1 из 4: Введите Telegram ID пользователя:"
    )
    
    await callback.message.edit_text(text, reply_markup=back_kb(), parse_mode="Markdown")
    await state.set_state(UIStates.waiting_user_id)
    await callback.answer()


@flows_router.message(UIStates.waiting_user_id)
async def handle_user_id_input(message: Message, state: FSMContext, **kwargs):
    """Обработка ввода user ID."""
    if not is_admin(message.from_user.id):
        await message.reply("❌ Недостаточно прав")
        return
    
    try:
        user_id_str = message.text.strip()
        user_id = int(user_id_str)
        
        if user_id <= 0:
            raise ValueError("Invalid user ID")
        
        # Сохраняем user_id в состоянии
        await state.update_data(tg_user_id=user_id)
        
        text = (
            f"✅ User ID: `{user_id}`\n\n"
            "Шаг 2 из 4: Введите username (без @) или пропустите:"
        )
        
        # Добавляем кнопку пропуска
        skip_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ Пропустить", callback_data="skip:username")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:back")]
        ])
        
        await message.answer(text, reply_markup=skip_kb, parse_mode="Markdown")
        await state.set_state(UIStates.waiting_username)
        
    except ValueError:
        await message.reply(
            "❌ Неверный формат. Введите числовой Telegram ID:",
            reply_markup=back_kb()
        )


@flows_router.message(UIStates.waiting_username)
async def handle_username_input(message: Message, state: FSMContext, **kwargs):
    """Обработка ввода username."""
    if not is_admin(message.from_user.id):
        await message.reply("❌ Недостаточно прав")
        return
    
    username = message.text.strip()
    
    # Убираем @ если есть
    if username.startswith('@'):
        username = username[1:]
    
    await state.update_data(username=username if username else None)
    
    data = await state.get_data()
    user_id = data.get('tg_user_id')
    
    text = (
        f"✅ User ID: `{user_id}`\n"
        f"✅ Username: {f'@{username}' if username else 'не указан'}\n\n"
        "Шаг 3 из 4: Введите имя пользователя или пропустите:"
    )
    
    skip_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data="skip:name")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:back")]
    ])
    
    await message.answer(text, reply_markup=skip_kb, parse_mode="Markdown")
    await state.set_state(UIStates.waiting_user_name)


@flows_router.callback_query(F.data == "skip:username")
async def skip_username(callback: CallbackQuery, state: FSMContext, **kwargs):
    """Пропустить ввод username."""
    await state.update_data(username=None)
    
    data = await state.get_data()
    user_id = data.get('tg_user_id')
    
    text = (
        f"✅ User ID: `{user_id}`\n"
        f"✅ Username: не указан\n\n"
        "Шаг 3 из 4: Введите имя пользователя или пропустите:"
    )
    
    skip_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data="skip:name")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:back")]
    ])
    
    await callback.message.edit_text(text, reply_markup=skip_kb, parse_mode="Markdown")
    await state.set_state(UIStates.waiting_user_name)
    await callback.answer()


@flows_router.message(UIStates.waiting_user_name)
async def handle_user_name_input(message: Message, state: FSMContext, **kwargs):
    """Обработка ввода имени пользователя."""
    if not is_admin(message.from_user.id):
        await message.reply("❌ Недостаточно прав")
        return
    
    name = message.text.strip()
    await state.update_data(name=name if name else None)
    
    data = await state.get_data()
    user_id = data.get('tg_user_id')
    username = data.get('username')
    
    text = (
        f"✅ User ID: `{user_id}`\n"
        f"✅ Username: {f'@{username}' if username else 'не указан'}\n"
        f"✅ Имя: {name if name else 'не указано'}\n\n"
        "Шаг 4 из 4: Введите имя пригласителя или пропустите:"
    )
    
    skip_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data="skip:inviter")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:back")]
    ])
    
    await message.answer(text, reply_markup=skip_kb, parse_mode="Markdown")
    await state.set_state(UIStates.waiting_inviter)


@flows_router.callback_query(F.data == "skip:name")
async def skip_name(callback: CallbackQuery, state: FSMContext, **kwargs):
    """Пропустить ввод имени."""
    await state.update_data(name=None)
    
    data = await state.get_data()
    user_id = data.get('tg_user_id')
    username = data.get('username')
    
    text = (
        f"✅ User ID: `{user_id}`\n"
        f"✅ Username: {f'@{username}' if username else 'не указан'}\n"
        f"✅ Имя: не указано\n\n"
        "Шаг 4 из 4: Введите имя пригласителя или пропустите:"
    )
    
    skip_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data="skip:inviter")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:back")]
    ])
    
    await callback.message.edit_text(text, reply_markup=skip_kb, parse_mode="Markdown")
    await state.set_state(UIStates.waiting_inviter)
    await callback.answer()


@flows_router.message(UIStates.waiting_inviter)
async def handle_inviter_input(message: Message, state: FSMContext, **kwargs):
    """Обработка ввода пригласителя и завершение добавления."""
    if not is_admin(message.from_user.id):
        await message.reply("❌ Недостаточно прав")
        return
    
    try:
        inviter_name = message.text.strip()
        
        data = await state.get_data()
        data['inviter_name'] = inviter_name if inviter_name else None
        
        # Добавляем пользователя
        success = add_user_manual(data)
        
        if success:
            text = (
                "✅ **Пользователь успешно добавлен!**\n\n"
                f"👤 User ID: `{data['tg_user_id']}`\n"
                f"👤 Username: {'@' + data['username'] if data.get('username') else 'не указан'}\n"
                f"👤 Имя: {data.get('name', 'не указано')}\n"
                f"👤 Пригласитель: {data.get('inviter_name', 'не указан')}\n"
            )
        else:
            text = "❌ **Ошибка при добавлении пользователя**\n\nПопробуйте еще раз."
        
        await message.answer(text, reply_markup=main_admin_kb(), parse_mode="Markdown")
        await state.clear()
        
        if success:
            logger.info(f"Manually added user {data['tg_user_id']} by admin {message.from_user.id}")
        
    except Exception as e:
        logger.exception(f"Error in manual add flow: {e}")
        await message.reply("❌ Ошибка при добавлении пользователя")


@flows_router.callback_query(F.data == "skip:inviter")
async def skip_inviter(callback: CallbackQuery, state: FSMContext, **kwargs):
    """Пропустить ввод пригласителя и завершить добавление."""
    try:
        data = await state.get_data()
        data['inviter_name'] = None
        
        # Добавляем пользователя
        success = add_user_manual(data)
        
        if success:
            text = (
                "✅ **Пользователь успешно добавлен!**\n\n"
                f"👤 User ID: `{data['tg_user_id']}`\n"
                f"👤 Username: {'@' + data['username'] if data.get('username') else 'не указан'}\n"
                f"👤 Имя: {data.get('name', 'не указано')}\n"
                f"👤 Пригласитель: не указан\n"
            )
        else:
            text = "❌ **Ошибка при добавлении пользователя**\n\nПопробуйте еще раз."
        
        await callback.message.edit_text(text, reply_markup=main_admin_kb(), parse_mode="Markdown")
        await state.clear()
        await callback.answer("✅ Пользователь добавлен!")
        
        if success:
            logger.info(f"Manually added user {data['tg_user_id']} by admin {callback.from_user.id}")
        
    except Exception as e:
        logger.exception(f"Error in manual add flow: {e}")
        await callback.answer("❌ Ошибка при добавлении", show_alert=True)


# === DELETE USER FLOW ===

@flows_router.callback_query(F.data == "menu:delete_user")
async def start_delete_user_flow(callback: CallbackQuery, state: FSMContext, **kwargs):
    """Начать процесс удаления пользователя."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    text = (
        "❌ **Удаление пользователя**\n\n"
        "Введите @username или user_id пользователя для удаления:"
    )
    
    await callback.message.edit_text(text, reply_markup=back_kb(), parse_mode="Markdown")
    await state.set_state(UIStates.waiting_delete_user)
    await callback.answer()


@flows_router.message(UIStates.waiting_delete_user)
async def handle_delete_user_input(message: Message, state: FSMContext, **kwargs):
    """Обработка ввода пользователя для удаления."""
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
        
        # Сохраняем данные пользователя для подтверждения
        await state.update_data(
            search_query=search_query,
            user_data=user_data
        )
        
        username_display = f"@{user_data.get('username')}" if user_data.get('username') else f"ID: {user_data['tg_user_id']}"
        name_display = user_data.get('name', 'не указано')
        
        text = (
            f"⚠️ **Подтверждение удаления**\n\n"
            f"Вы точно хотите удалить пользователя?\n\n"
            f"👤 **{username_display}**\n"
            f"📝 Имя: {name_display}\n"
            f"📊 Подписок: {user_data.get('subscribe_count', 0)}\n"
            f"📊 Отписок: {user_data.get('unsubscribe_count', 0)}\n\n"
            "❗️ Это действие нельзя отменить!"
        )
        
        await message.answer(
            text, 
            reply_markup=confirm_delete_kb(search_query), 
            parse_mode="Markdown"
        )
        await state.set_state(UIStates.waiting_delete_confirm)
        
    except Exception as e:
        logger.exception(f"Error in delete user flow: {e}")
        await message.reply("❌ Ошибка при поиске пользователя")


@flows_router.callback_query(F.data.startswith("confirm:delete:"))
async def handle_delete_confirmation(callback: CallbackQuery, state: FSMContext, **kwargs):
    """Обработка подтверждения удаления."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    try:
        # Извлекаем username из callback data
        search_query = callback.data.split(":", 2)[2]
        
        success = delete_user(search_query)
        
        if success:
            text = f"✅ **Пользователь {search_query} успешно удален!**"
            await callback.answer("✅ Пользователь удален!")
        else:
            text = f"❌ **Ошибка при удалении пользователя {search_query}**"
            await callback.answer("❌ Ошибка при удалении", show_alert=True)
        
        await callback.message.edit_text(text, reply_markup=main_admin_kb(), parse_mode="Markdown")
        await state.clear()
        
        if success:
            logger.info(f"Deleted user {search_query} by admin {callback.from_user.id}")
        
    except Exception as e:
        logger.exception(f"Error confirming delete: {e}")
        await callback.answer("❌ Ошибка при удалении", show_alert=True)


# === SETTINGS FLOW ===

@flows_router.callback_query(F.data == "menu:settings")
async def handle_settings_menu(callback: CallbackQuery, **kwargs):
    """Меню настроек уведомлений."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    try:
        settings = get_settings_manager().get_current_settings()
        
        text = (
            "🔔 **Настройки уведомлений**\n\n"
            f"⏰ Время отчетов: {settings['report_time']}\n"
            f"👥 Получатели: {len(settings['target_chats'])} чат(ов)\n"
            f"📊 Планировщик: {'включен' if settings['scheduler_enabled'] else 'выключен'}\n"
            f"👑 Администраторы: {len(settings['admin_ids'])}"
        )
        
        buttons = [
            [InlineKeyboardButton(text="⏰ Изменить время", callback_data="settings:time")],
            [InlineKeyboardButton(text="👑 Добавить админа", callback_data="settings:add_admin")],
            [InlineKeyboardButton(text="📊 Показать текущие", callback_data="settings:show")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:back")]
        ]
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
        await callback.answer()
        
    except Exception as e:
        logger.exception(f"Error in settings menu: {e}")
        await callback.answer("❌ Ошибка при загрузке настроек", show_alert=True)


@flows_router.callback_query(F.data == "settings:time")
async def handle_change_time(callback: CallbackQuery, state: FSMContext, **kwargs):
    """Изменение времени отчетов."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    text = (
        "⏰ **Изменение времени отчетов**\n\n"
        "Введите новое время в формате ЧЧ:ММ (например, 09:30):"
    )
    
    await callback.message.edit_text(text, reply_markup=back_kb(), parse_mode="Markdown")
    await state.set_state(UIStates.waiting_report_time)
    await callback.answer()


@flows_router.message(UIStates.waiting_report_time)
async def handle_report_time_input(message: Message, state: FSMContext, **kwargs):
    """Обработка ввода времени отчетов."""
    if not is_admin(message.from_user.id):
        await message.reply("❌ Недостаточно прав")
        return
    
    try:
        time_str = message.text.strip()
        
        success = get_settings_manager().set_report_time(time_str)
        
        if success:
            text = f"✅ **Время отчетов изменено на {time_str}**"
        else:
            text = "❌ **Неверный формат времени**\n\nИспользуйте формат ЧЧ:ММ (например, 09:30)"
        
        await message.answer(text, reply_markup=main_admin_kb(), parse_mode="Markdown")
        await state.clear()
        
        if success:
            logger.info(f"Report time changed to {time_str} by admin {message.from_user.id}")
        
    except Exception as e:
        logger.exception(f"Error setting report time: {e}")
        await message.reply("❌ Ошибка при изменении времени")


@flows_router.callback_query(F.data == "settings:add_admin")
async def handle_add_admin(callback: CallbackQuery, state: FSMContext, **kwargs):
    """Добавление администратора."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    text = (
        "👑 **Добавление администратора**\n\n"
        "Введите Telegram ID нового администратора:"
    )
    
    await callback.message.edit_text(text, reply_markup=back_kb(), parse_mode="Markdown")
    await state.set_state(UIStates.waiting_admin_id)
    await callback.answer()


@flows_router.message(UIStates.waiting_admin_id)
async def handle_admin_id_input(message: Message, state: FSMContext, **kwargs):
    """Обработка ввода ID администратора."""
    if not is_admin(message.from_user.id):
        await message.reply("❌ Недостаточно прав")
        return
    
    try:
        admin_id_str = message.text.strip()
        admin_id = int(admin_id_str)
        
        if admin_id <= 0:
            raise ValueError("Invalid admin ID")
        
        success = get_settings_manager().add_admin(admin_id)
        
        if success:
            text = f"✅ **Администратор {admin_id} добавлен!**"
        else:
            text = "❌ **Ошибка при добавлении администратора**"
        
        await message.answer(text, reply_markup=main_admin_kb(), parse_mode="Markdown")
        await state.clear()
        
        if success:
            logger.info(f"Added admin {admin_id} by admin {message.from_user.id}")
        
    except ValueError:
        await message.reply(
            "❌ Неверный формат. Введите числовой Telegram ID:",
            reply_markup=back_kb()
        )
    except Exception as e:
        logger.exception(f"Error adding admin: {e}")
        await message.reply("❌ Ошибка при добавлении администратора")


@flows_router.callback_query(F.data == "settings:show")
async def handle_show_settings(callback: CallbackQuery, **kwargs):
    """Показать подробные настройки."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    try:
        settings = get_settings_manager().get_current_settings()
        
        text = "🔔 **Подробные настройки**\n\n"
        text += f"⏰ **Время отчетов:** {settings['report_time']}\n"
        text += f"📊 **Планировщик:** {'включен' if settings['scheduler_enabled'] else 'выключен'}\n\n"
        
        if settings['target_chats']:
            text += f"👥 **Чаты для отчетов ({len(settings['target_chats'])}):**\n"
            for chat_id in settings['target_chats'][:5]:  # Показываем первые 5
                text += f"• `{chat_id}`\n"
            if len(settings['target_chats']) > 5:
                text += f"• ... и еще {len(settings['target_chats']) - 5}\n"
        else:
            text += "👥 **Чаты для отчетов:** не настроены\n"
        
        text += "\n"
        
        if settings['admin_ids']:
            text += f"👑 **Администраторы ({len(settings['admin_ids'])}):**\n"
            for admin_id in settings['admin_ids'][:5]:  # Показываем первых 5
                text += f"• `{admin_id}`\n"
            if len(settings['admin_ids']) > 5:
                text += f"• ... и еще {len(settings['admin_ids']) - 5}\n"
        else:
            text += "👑 **Администраторы:** не настроены\n"
        
        back_settings_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К настройкам", callback_data="menu:settings")]
        ])
        
        await callback.message.edit_text(text, reply_markup=back_settings_kb, parse_mode="Markdown")
        await callback.answer()
        
    except Exception as e:
        logger.exception(f"Error showing settings: {e}")
        await callback.answer("❌ Ошибка при показе настроек", show_alert=True)