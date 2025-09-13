#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Обработчики событий Telegram для отслеживания изменений в чатах.

Отслеживает:
- Подписки/отписки от каналов
- Изменения приватности чатов  
- Вход/выход участников из групп
- Создание/удаление чатов
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from aiogram import Router, F
from aiogram.types import (
    ChatMemberUpdated, 
    ChatMemberOwner, 
    ChatMemberAdministrator,
    ChatMemberMember, 
    ChatMemberRestricted,
    ChatMemberLeft, 
    ChatMemberBanned
)
from aiogram.enums import ChatType, ChatMemberStatus

from db.db import get_db
from utils.time_utils import get_almaty_now
from utils.logging_conf import get_logger

logger = get_logger(__name__)

# Создаём роутер для обработчиков событий
events_router = Router(name="events")


@events_router.chat_member()
async def handle_chat_member_update(chat_member: ChatMemberUpdated):
    """
    Обработчик изменений статуса участников чата.
    
    Отслеживает:
    - Подписки/отписки от каналов
    - Вход/выход из групп
    - Изменения прав участников
    - Блокировки/разблокировки
    """
    try:
        # Извлекаем основную информацию
        chat = chat_member.chat
        user = chat_member.new_chat_member.user
        old_status = chat_member.old_chat_member.status
        new_status = chat_member.new_chat_member.status
        
        # Логируем событие с подробной информацией
        chat_type_str = chat.type.value if hasattr(chat.type, 'value') else str(chat.type)
        logger.info(f"📝 [Events] Chat member update: chat_id={chat.id}, chat_type={chat_type_str}, "
                   f"user_id={user.id}, old_status={old_status}, new_status={new_status}")
        
        # Получаем invite_link если доступна
        invite_link = getattr(chat_member, 'invite_link', None)
        if invite_link:
            logger.info(f"🔗 [Events] Invite link detected: {invite_link}")
        
        # Получаем update_id для идемпотентности (без fallback на timestamp)
        update_id = None
        
        # Определяем тип события
        event_type = _determine_event_type(chat.type, old_status, new_status)
        
        if not event_type:
            logger.debug(f"⏭️ [Events] Skipping non-tracked status change: {old_status} → {new_status}")
            return
        
        # Определяем пригласителя из invite_link
        inviter_id = None
        if invite_link:
            try:
                db = get_db()
                inviter_id = db.get_inviter_by_link(invite_link)
                if inviter_id:
                    logger.info(f"👤 [Events] Found inviter_id={inviter_id} for link: {invite_link}")
                else:
                    logger.warning(f"⚠️ [Events] No inviter found for link: {invite_link}")
            except Exception as e:
                logger.error(f"❌ [Events] Error finding inviter for link {invite_link}: {e}")

        # Подготавливаем данные для записи в БД
        event_data = {
            'telegram_update_id': update_id,
            'chat_id': chat.id,
            'chat_title': chat.title or chat.username or f"Chat_{chat.id}",
            'chat_type': chat_type_str,
            'user_id': user.id,
            'username': user.username,
            'first_name': user.first_name,
            'last_name': user.last_name,
            'event_type': event_type,
            'old_status': old_status.value if hasattr(old_status, 'value') else str(old_status),
            'new_status': new_status.value if hasattr(new_status, 'value') else str(new_status),
            'timestamp': get_almaty_now(),
            'is_bot': user.is_bot,
            'raw_data': {
                'chat': {
                    'id': chat.id,
                    'type': chat_type_str,
                    'title': chat.title,
                    'username': chat.username,
                    'description': getattr(chat, 'description', None),
                },
                'user': {
                    'id': user.id,
                    'username': user.username,
                    'first_name': user.first_name,
                    'last_name': user.last_name,
                    'is_bot': user.is_bot,
                    'language_code': getattr(user, 'language_code', None),
                },
                'status_change': {
                    'old': old_status.value if hasattr(old_status, 'value') else str(old_status),
                    'new': new_status.value if hasattr(new_status, 'value') else str(new_status),
                    'date': chat_member.date.isoformat() if chat_member.date else None,
                }
            }
        }
        
        # Записываем в базу данных
        if 'db' not in locals():
            db = get_db()
        db.insert_journal_event(
            event_type=event_type,
            tg_user_id=user.id,
            username=user.username,
            name=user.first_name,
            status=new_status.value if hasattr(new_status, 'value') else str(new_status),
            telegram_update_id=update_id,
            inviter_id=inviter_id
        )
        
        # Добавляем событие в Excel файл согласно ТЗ
        try:
            from reports.subscribers_database_manager import SubscribersDatabaseManager
            excel_manager = SubscribersDatabaseManager(db)
            
            # Получаем имя пригласителя
            inviter_name = "Не указан"
            if inviter_id:
                inviter_data = db.get_inviter_name_by_id(inviter_id)
                if inviter_data:
                    inviter_name = inviter_data
            
            # Подготавливаем данные для Excel согласно ТЗ
            excel_event_data = {
                'event_time': get_almaty_now().isoformat(),
                'event_type': event_type,
                'tg_user_id': user.id,
                'username': user.username or '',
                'user_name': user.first_name or '',
                'inviter_name': inviter_name,
                'status': new_status.value if hasattr(new_status, 'value') else str(new_status)
            }
            
            # Добавляем в единый Excel файл subscribers_database.xlsx
            excel_manager.add_history_event(excel_event_data)
            logger.info(f"📊 [Events] Added to Excel: {event_type} for user {user.id}")
            
        except Exception as excel_error:
            # Не прерываем основной поток если Excel дает ошибку
            logger.error(f"❌ [Events] Excel update error: {excel_error}")
        
        logger.info(f"✅ [Events] Recorded {event_type} event: "
                   f"chat_id={chat.id}, user_id={user.id}")
        
    except Exception as e:
        logger.exception(f"❌ [Events] Error handling chat member update: {e}")


@events_router.my_chat_member()
async def handle_bot_chat_member_update(my_chat_member: ChatMemberUpdated):
    """
    Обработчик изменений статуса бота в чатах.
    
    Отслеживает когда бота:
    - Добавляют в чат
    - Удаляют из чата
    - Изменяют его права
    - Блокируют/разблокируют
    """
    try:
        # Извлекаем основную информацию
        chat = my_chat_member.chat
        bot_user = my_chat_member.new_chat_member.user
        old_status = my_chat_member.old_chat_member.status
        new_status = my_chat_member.new_chat_member.status
        
        # Получаем update_id для идемпотентности (используем timestamp как fallback)
        update_id = getattr(my_chat_member, 'update_id', None) or int(get_almaty_now().timestamp())
        
        # Логируем событие
        logger.info(f"🤖 [Events] Bot status update: chat_id={chat.id}, "
                   f"old_status={old_status}, new_status={new_status}")
        
        # Определяем тип события для бота
        event_type = _determine_bot_event_type(chat.type, old_status, new_status)
        
        if not event_type:
            logger.debug(f"⏭️ [Events] Skipping non-tracked bot status change: {old_status} → {new_status}")
            return
        
        # Подготавливаем данные для записи в БД
        event_data = {
            'telegram_update_id': update_id,
            'chat_id': chat.id,
            'chat_title': chat.title or chat.username or f"Chat_{chat.id}",
            'chat_type': chat_type_str,
            'user_id': bot_user.id,
            'username': bot_user.username,
            'first_name': bot_user.first_name,
            'last_name': bot_user.last_name,
            'event_type': event_type,
            'old_status': old_status.value if hasattr(old_status, 'value') else str(old_status),
            'new_status': new_status.value if hasattr(new_status, 'value') else str(new_status),
            'timestamp': get_almaty_now(),
            'is_bot': True,
            'raw_data': {
                'chat': {
                    'id': chat.id,
                    'type': chat_type_str,
                    'title': chat.title,
                    'username': chat.username,
                    'description': getattr(chat, 'description', None),
                },
                'bot': {
                    'id': bot_user.id,
                    'username': bot_user.username,
                    'first_name': bot_user.first_name,
                    'last_name': bot_user.last_name,
                    'is_bot': True,
                },
                'status_change': {
                    'old': old_status.value if hasattr(old_status, 'value') else str(old_status),
                    'new': new_status.value if hasattr(new_status, 'value') else str(new_status),
                    'date': my_chat_member.date.isoformat() if my_chat_member.date else None,
                }
            }
        }
        
        # Записываем в базу данных
        db = get_db()
        db.insert_journal_event(
            event_type=event_type,
            tg_user_id=bot_user.id,
            username=bot_user.username,
            name=bot_user.first_name,
            status=new_status.value if hasattr(new_status, 'value') else str(new_status),
            telegram_update_id=update_id
        )
        
        logger.info(f"✅ [Events] Recorded bot {event_type} event: chat_id={chat.id}")
        
    except Exception as e:
        logger.exception(f"❌ [Events] Error handling bot chat member update: {e}")


def _determine_event_type(chat_type: ChatType, old_status: ChatMemberStatus, 
                         new_status: ChatMemberStatus) -> Optional[str]:
    """
    Определить тип события на основе типа чата и изменения статуса.
    
    Args:
        chat_type: Тип чата (channel, group, supergroup, private)
        old_status: Старый статус участника
        new_status: Новый статус участника
        
    Returns:
        str или None: Тип события для записи в БД
    """
    # Для каналов
    if chat_type == ChatType.CHANNEL:
        if old_status == ChatMemberStatus.LEFT and new_status == ChatMemberStatus.MEMBER:
            return "subscribe"
        elif old_status == ChatMemberStatus.MEMBER and new_status == ChatMemberStatus.LEFT:
            return "unsubscribe"
        elif old_status == ChatMemberStatus.MEMBER and new_status == ChatMemberStatus.BANNED:
            return "channel_banned"
        elif old_status == ChatMemberStatus.BANNED and new_status == ChatMemberStatus.MEMBER:
            return "channel_unbanned"
    
    # Для групп и супергрупп
    elif chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        if old_status == ChatMemberStatus.LEFT and new_status in [
            ChatMemberStatus.MEMBER, ChatMemberStatus.RESTRICTED
        ]:
            return "group_join"
        elif old_status in [
            ChatMemberStatus.MEMBER, ChatMemberStatus.RESTRICTED
        ] and new_status == ChatMemberStatus.LEFT:
            return "group_leave"
        elif old_status in [
            ChatMemberStatus.MEMBER, ChatMemberStatus.RESTRICTED
        ] and new_status == ChatMemberStatus.BANNED:
            return "group_banned"
        elif old_status == ChatMemberStatus.BANNED and new_status in [
            ChatMemberStatus.MEMBER, ChatMemberStatus.RESTRICTED
        ]:
            return "group_unbanned"
        elif old_status == ChatMemberStatus.MEMBER and new_status == ChatMemberStatus.ADMINISTRATOR:
            return "group_promoted"
        elif old_status == ChatMemberStatus.ADMINISTRATOR and new_status == ChatMemberStatus.MEMBER:
            return "group_demoted"
    
    # Приватные чаты (хотя там изменения статуса редки)
    elif chat_type == ChatType.PRIVATE:
        if old_status == ChatMemberStatus.LEFT and new_status == ChatMemberStatus.MEMBER:
            return "private_unblocked"
        elif old_status == ChatMemberStatus.MEMBER and new_status == ChatMemberStatus.BANNED:
            return "private_blocked"
    
    return None


def _determine_bot_event_type(chat_type: ChatType, old_status: ChatMemberStatus, 
                             new_status: ChatMemberStatus) -> Optional[str]:
    """
    Определить тип события для изменений статуса бота.
    
    Args:
        chat_type: Тип чата
        old_status: Старый статус бота
        new_status: Новый статус бота
        
    Returns:
        str или None: Тип события для записи в БД
    """
    # Для всех типов чатов
    if old_status == ChatMemberStatus.LEFT and new_status in [
        ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR
    ]:
        return "bot_added"
    elif old_status in [
        ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR
    ] and new_status == ChatMemberStatus.LEFT:
        return "bot_removed"
    elif old_status in [
        ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR
    ] and new_status == ChatMemberStatus.BANNED:
        return "bot_banned"
    elif old_status == ChatMemberStatus.BANNED and new_status in [
        ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR
    ]:
        return "bot_unbanned"
    elif old_status == ChatMemberStatus.MEMBER and new_status == ChatMemberStatus.ADMINISTRATOR:
        return "bot_promoted"
    elif old_status == ChatMemberStatus.ADMINISTRATOR and new_status == ChatMemberStatus.MEMBER:
        return "bot_demoted"
    
    return None


async def log_event_statistics():
    """Логировать статистику обработанных событий."""
    try:
        db = get_db()
        
        # Получаем статистику за последний час
        now = get_almaty_now()
        stats = db.get_events_for_period(
            start_date=(now.replace(minute=0, second=0, microsecond=0) - 
                       timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),
            end_date=now.strftime("%Y-%m-%d %H:%M:%S")
        )
        
        if stats:
            logger.info(f"📊 [Events] Processed {len(stats)} events in the last hour")
        
    except Exception as e:
        logger.exception(f"❌ [Events] Error logging event statistics: {e}")


# Экспорт роутера для регистрации в основном приложении
__all__ = ['events_router', 'log_event_statistics']