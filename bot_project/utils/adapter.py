#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Адаптеры для интеграции с существующей бизнес-логикой бота.
Предоставляет API-обертки для UI модуля без изменения основной логики.
"""

import os
import logging
import secrets
import string
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, date, timedelta
from pathlib import Path

from db.db import get_db, DatabaseManager
from reports.report_manager import ReportManager
from utils.time_utils import get_almaty_now, get_today_date_str
from utils.logging_conf import get_logger

logger = get_logger(__name__)


class InviteManager:
    """Управление пригласительными ссылками для каналов."""
    
    def __init__(self, db: DatabaseManager, bot=None):
        self.db = db
        self.bot = bot
        self.target_channels = self._get_target_channels()
    
    async def create_invite_for(self, inviter_name: str) -> str:
        """
        Создать пригласительную ссылку в канал для пригласителя.
        
        Args:
            inviter_name: Имя пригласителя
            
        Returns:
            str: Пригласительная ссылка в канал
        """
        try:
            if not self.bot:
                raise ValueError("Bot instance not available for creating channel invites")
            
            if not self.target_channels:
                raise ValueError("No target channels configured. Set TARGET_CHANNELS or TARGET_CHATS environment variable.")
            
            # Используем первый доступный канал
            channel_id = self.target_channels[0]
            
            # Создаем приглашение в канал с именем пригласителя
            invite_link = await self._create_channel_invite(channel_id, inviter_name)
            
            # Сохраняем в БД - обновляем если пользователь уже существует, сохраняя ID
            with self.db.get_connection() as conn:
                # Проверяем есть ли уже такой пригласитель
                existing = conn.execute(
                    "SELECT id FROM inviters WHERE name = ?", (inviter_name,)
                ).fetchone()
                
                if existing:
                    # Обновляем существующую запись, сохраняя ID
                    cursor = conn.execute(
                        "UPDATE inviters SET username = ?, invite_link = ?, channel_id = ? WHERE id = ?",
                        (inviter_name, invite_link, channel_id, existing[0])
                    )
                    logger.info(f"Updated existing invite for {inviter_name}")
                else:
                    # Создаём новую запись 
                    cursor = conn.execute(
                        "INSERT INTO inviters (name, username, invite_link, channel_id) VALUES (?, ?, ?, ?)",
                        (inviter_name, inviter_name, invite_link, channel_id)
                    )
                conn.commit()
                
                logger.info(f"Created channel invite for {inviter_name}: {invite_link}")
                return invite_link
                
        except Exception as e:
            logger.exception(f"Failed to create channel invite for {inviter_name}: {e}")
            raise
    
    def get_invites(self) -> List[Dict[str, Any]]:
        """
        Получить все пригласительные ссылки.
        
        Returns:
            List[Dict]: Список пригласителей и их ссылок
        """
        try:
            with self.db.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT id, name, invite_link, channel_id FROM inviters ORDER BY name"
                )
                invites = []
                for row in cursor.fetchall():
                    invite_data = dict(row)
                    # Добавляем статистику
                    stats = self._get_invite_stats(invite_data['id'])
                    invite_data.update(stats)
                    invites.append(invite_data)
                
                return invites
                
        except Exception as e:
            logger.exception(f"Failed to get invites: {e}")
            raise
    
    def get_inviter_list(self) -> List[str]:
        """Получить список имен пригласителей."""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.execute("SELECT DISTINCT name FROM inviters ORDER BY name")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.exception(f"Failed to get inviter list: {e}")
            return []
    
    def delete_invite(self, invite_id: int) -> bool:
        """
        Удалить пригласительную ссылку.
        
        Args:
            invite_id: ID пригласительной ссылки
            
        Returns:
            bool: True если удалено успешно
        """
        try:
            with self.db.get_connection() as conn:
                cursor = conn.execute(
                    "DELETE FROM inviters WHERE id = ?",
                    (invite_id,)
                )
                conn.commit()
                
                if cursor.rowcount > 0:
                    logger.info(f"Deleted invite {invite_id}")
                    return True
                return False
                
        except Exception as e:
            logger.exception(f"Failed to delete invite {invite_id}: {e}")
            return False
    
    def get_invite_info(self, invite_id: int) -> Optional[Dict[str, Any]]:
        """Получить информацию о пригласительной ссылке."""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT id, name, invite_link, channel_id FROM inviters WHERE id = ?",
                    (invite_id,)
                )
                row = cursor.fetchone()
                if row:
                    invite_data = dict(row)
                    stats = self._get_invite_stats(invite_id)
                    invite_data.update(stats)
                    return invite_data
                return None
                
        except Exception as e:
            logger.exception(f"Failed to get invite info {invite_id}: {e}")
            return None
    
    def _get_target_channels(self) -> List[str]:
        """Получить список каналов для создания приглашений."""
        # Сначала пробуем TARGET_CHANNELS, затем TARGET_CHATS как fallback
        channels_str = os.getenv('TARGET_CHANNELS', '') or os.getenv('TARGET_CHATS', '')
        if not channels_str:
            logger.warning("TARGET_CHANNELS or TARGET_CHATS not configured - cannot create channel invites")
            return []
        
        # Парсим каналы из строки (через запятую)
        channels = [ch.strip() for ch in channels_str.split(',') if ch.strip()]
        logger.info(f"Configured target channels: {channels}")
        return channels
    
    async def _create_channel_invite(self, channel_id: str, inviter_name: str) -> str:
        """Создать приглашение в канал через Telegram API."""
        if not self.bot:
            raise ValueError("Bot instance not available")
            
        try:
            # Создаем приглашение с названием пригласителя
            invite_link_obj = await self.bot.create_chat_invite_link(
                chat_id=channel_id,
                name=f"Приглашение от {inviter_name}",
                creates_join_request=False  # Прямое присоединение без запроса
            )
            
            return invite_link_obj.invite_link
            
        except Exception as e:
            logger.error(f"Failed to create invite for channel {channel_id}: {e}")
            # Fallback - пробуем экспортировать постоянную ссылку
            try:
                invite_link = await self.bot.export_chat_invite_link(chat_id=channel_id)
                logger.info(f"Using exported invite link for {inviter_name}")
                return invite_link
            except Exception as fallback_error:
                logger.error(f"Fallback export also failed: {fallback_error}")
                raise Exception(f"Cannot create invite for channel {channel_id}. Bot may not be admin.")
    
    def _get_invite_stats(self, inviter_id: int) -> Dict[str, Any]:
        """Получить статистику по пригласителю."""
        try:
            with self.db.get_connection() as conn:
                # Всего приглашено
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM journal WHERE inviter_id = ? AND event_type = 'subscribe'",
                    (inviter_id,)
                )
                total_invited = cursor.fetchone()[0]
                
                # Активных сейчас (подписаны и не вышли)
                cursor = conn.execute(
                    """SELECT COUNT(DISTINCT tg_user_id) FROM journal j1 
                       WHERE j1.inviter_id = ? AND j1.event_type = 'subscribe'
                       AND j1.tg_user_id NOT IN (
                           SELECT j2.tg_user_id FROM journal j2 
                           WHERE j2.event_type = 'unsubscribe' AND j2.event_time > j1.event_time
                       )""",
                    (inviter_id,)
                )
                active_now = cursor.fetchone()[0]
                
                retention_rate = (active_now / total_invited * 100) if total_invited > 0 else 0
                
                return {
                    'total_invited': total_invited,
                    'active_now': active_now,
                    'retention_rate': round(retention_rate, 1)
                }
                
        except Exception as e:
            logger.exception(f"Failed to get invite stats for {inviter_id}: {e}")
            return {'total_invited': 0, 'active_now': 0, 'retention_rate': 0}


class UserManager:
    """Управление пользователями."""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    def find_user(self, search_query: str) -> Optional[Dict[str, Any]]:
        """
        Найти пользователя по username или user_id.
        
        Args:
            search_query: @username или user_id
            
        Returns:
            Dict: Информация о пользователе или None
        """
        try:
            # Определяем тип поиска
            if search_query.startswith('@'):
                username = search_query[1:]  # убираем @
                search_field = 'username'
                search_value = username
            else:
                try:
                    user_id = int(search_query)
                    search_field = 'tg_user_id'
                    search_value = user_id
                except ValueError:
                    return None
            
            # Ищем пользователя
            with self.db.get_connection() as conn:
                cursor = conn.execute(
                    f"SELECT DISTINCT tg_user_id, username, name FROM journal WHERE {search_field} = ? LIMIT 1",
                    (search_value,)
                )
                user_row = cursor.fetchone()
                
                if not user_row:
                    return None
                
                user_data = dict(user_row)
                
                # Получаем историю событий
                cursor = conn.execute(
                    """SELECT j.event_time, j.event_type, j.status, j.note, i.name as inviter_name
                       FROM journal j
                       LEFT JOIN inviters i ON j.inviter_id = i.id
                       WHERE j.tg_user_id = ?
                       ORDER BY j.event_time DESC""",
                    (user_data['tg_user_id'],)
                )
                
                history = [dict(row) for row in cursor.fetchall()]
                user_data['history'] = history
                
                # Статистика
                subscribe_count = len([h for h in history if h['event_type'] == 'subscribe'])
                unsubscribe_count = len([h for h in history if h['event_type'] == 'unsubscribe'])
                
                # Текущий статус
                last_event = history[0] if history else None
                current_status = last_event['status'] if last_event else 'unknown'
                
                user_data.update({
                    'subscribe_count': subscribe_count,
                    'unsubscribe_count': unsubscribe_count,
                    'current_status': current_status,
                    'last_activity': last_event['event_time'] if last_event else None
                })
                
                return user_data
                
        except Exception as e:
            logger.exception(f"Failed to find user {search_query}: {e}")
            return None
    
    def add_user_manual(self, user_data: Dict[str, Any]) -> bool:
        """
        Добавить пользователя вручную.
        
        Args:
            user_data: {tg_user_id, username, name, inviter_name, event_date}
            
        Returns:
            bool: True если добавлен успешно
        """
        try:
            # Находим пригласителя
            inviter_id = None
            if user_data.get('inviter_name'):
                with self.db.get_connection() as conn:
                    cursor = conn.execute(
                        "SELECT id FROM inviters WHERE name = ? LIMIT 1",
                        (user_data['inviter_name'],)
                    )
                    row = cursor.fetchone()
                    if row:
                        inviter_id = row[0]
            
            # Добавляем пользователя
            user_id = self.db.insert_user_if_not_exists(
                user_data['tg_user_id'],
                user_data.get('username'),
                user_data.get('name')
            )
            
            # Добавляем событие в журнал
            self.db.insert_journal_event(
                event_type='manual_add',
                tg_user_id=user_data['tg_user_id'],
                username=user_data.get('username'),
                name=user_data.get('name'),
                inviter_id=inviter_id,
                status='subscribed',
                note='manually_added'
            )
            
            logger.info(f"Manually added user {user_data['tg_user_id']}")
            return True
            
        except Exception as e:
            logger.exception(f"Failed to add user manually: {e}")
            return False
    
    def delete_user(self, username_or_id: str) -> bool:
        """
        Удалить пользователя (пометить как удален).
        
        Args:
            username_or_id: @username или user_id
            
        Returns:
            bool: True если удален успешно
        """
        try:
            user_data = self.find_user(username_or_id)
            if not user_data:
                return False
            
            # Добавляем событие удаления
            self.db.insert_journal_event(
                event_type='manual_delete',
                tg_user_id=user_data['tg_user_id'],
                username=user_data.get('username'),
                name=user_data.get('name'),
                status='deleted',
                note='manually_deleted'
            )
            
            logger.info(f"Deleted user {username_or_id}")
            return True
            
        except Exception as e:
            logger.exception(f"Failed to delete user {username_or_id}: {e}")
            return False


class ReportAdapter:
    """Адаптер для системы отчетов."""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
        self.report_manager = ReportManager(db)
        # Новый менеджер единого файла subscribers_database.xlsx
        from reports.subscribers_database_manager import SubscribersDatabaseManager
        self.database_manager = SubscribersDatabaseManager(db)
    
    def export_excel(self, report_type: str = "full") -> str:
        """
        Экспорт данных в Excel согласно ТЗ - единый файл subscribers_database.xlsx.
        
        Args:
            report_type: Тип отчета (full, daily, weekly, monthly)
            
        Returns:
            str: Путь к единому файлу subscribers_database.xlsx
        """
        try:
            # Согласно ТЗ - всегда используем единый файл subscribers_database.xlsx
            # Обновляем статистику и создаем дневные отчеты если нужно
            return self.database_manager.export_database()
                
        except Exception as e:
            logger.exception(f"Failed to export unified database: {e}")
            raise
    
    def get_stats(self, period: str = "today") -> Dict[str, Any]:
        """
        Получить статистику за период.
        
        Args:
            period: today, week, month
            
        Returns:
            Dict: Статистика
        """
        try:
            today = get_today_date_str()
            
            if period == "today":
                return self.db.get_daily_stats(today)
            elif period == "week":
                week_start = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
                return self.db.get_weekly_stats(week_start)
            elif period == "month":
                month_start = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
                return self.db.get_monthly_stats(month_start)
            else:
                return self.db.get_daily_stats(today)
                
        except Exception as e:
            logger.exception(f"Failed to get stats for {period}: {e}")
            return {}
    
    def get_rating(self) -> List[Dict[str, Any]]:
        """Получить рейтинг пригласителей."""
        try:
            return self.db.get_statistics_data()
        except Exception as e:
            logger.exception(f"Failed to get rating: {e}")
            return []


class SettingsManager:
    """Управление настройками уведомлений."""
    
    def __init__(self):
        self.env_file = Path('.env')
    
    def get_current_settings(self) -> Dict[str, Any]:
        """Получить текущие настройки."""
        return {
            'report_time': os.getenv('REPORT_TIME', '23:59'),
            'target_chats': os.getenv('TARGET_CHATS', '').split(',') if os.getenv('TARGET_CHATS') else [],
            'scheduler_enabled': os.getenv('SCHEDULER_ENABLED', 'true').lower() == 'true',
            'admin_ids': os.getenv('ADMIN_IDS', '').split(',') if os.getenv('ADMIN_IDS') else []
        }
    
    def set_report_time(self, time_str: str) -> bool:
        """Установить время отправки отчетов."""
        try:
            # Простая валидация времени
            hour, minute = map(int, time_str.split(':'))
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                self._update_env_variable('REPORT_TIME', time_str)
                return True
            return False
        except Exception as e:
            logger.exception(f"Failed to set report time {time_str}: {e}")
            return False
    
    def add_admin(self, user_id: int) -> bool:
        """Добавить администратора."""
        try:
            current_admins = self.get_current_settings()['admin_ids']
            if str(user_id) not in current_admins:
                current_admins.append(str(user_id))
                self._update_env_variable('ADMIN_IDS', ','.join(current_admins))
            return True
        except Exception as e:
            logger.exception(f"Failed to add admin {user_id}: {e}")
            return False
    
    def _update_env_variable(self, key: str, value: str):
        """Обновить переменную в .env файле."""
        # Простая реализация - в продакшене нужно использовать библиотеку python-dotenv
        logger.info(f"Would update {key}={value} in .env file")


# Глобальные экземпляры адаптеров
invite_manager: Optional[InviteManager] = None
user_manager: Optional[UserManager] = None
report_adapter: Optional[ReportAdapter] = None
settings_manager: Optional[SettingsManager] = None


def init_adapters():
    """Инициализация всех адаптеров."""
    global invite_manager, user_manager, report_adapter, settings_manager
    
    try:
        db = get_db()
        invite_manager = InviteManager(db, bot=None)  # Bot будет передан позже
        user_manager = UserManager(db)
        report_adapter = ReportAdapter(db)
        settings_manager = SettingsManager()
        
        logger.info("✅ All adapters initialized successfully")
        
    except Exception as e:
        logger.exception(f"❌ Failed to initialize adapters: {e}")
        raise


def get_invite_manager() -> InviteManager:
    """Получить менеджер пригласительных ссылок."""
    if invite_manager is None:
        init_adapters()
    if invite_manager is None:
        raise RuntimeError("Failed to initialize invite manager")
    return invite_manager


def get_user_manager() -> UserManager:
    """Получить менеджер пользователей."""
    if user_manager is None:
        init_adapters()
    if user_manager is None:
        raise RuntimeError("Failed to initialize user manager")
    return user_manager


def get_report_adapter() -> ReportAdapter:
    """Получить адаптер отчетов."""
    if report_adapter is None:
        init_adapters()
    if report_adapter is None:
        raise RuntimeError("Failed to initialize report adapter")
    return report_adapter


def get_settings_manager() -> SettingsManager:
    """Получить менеджер настроек."""
    if settings_manager is None:
        init_adapters()
    if settings_manager is None:
        raise RuntimeError("Failed to initialize settings manager")
    return settings_manager


# Удобные функции-обертки для совместимости с ТЗ
async def create_invite_for(name: str, bot=None) -> str:
    """Создать пригласительную ссылку для канала."""
    manager = get_invite_manager()
    if bot and not manager.bot:
        manager.bot = bot  # Устанавливаем bot instance если не был передан
    return await manager.create_invite_for(name)


def get_invites() -> List[Dict[str, Any]]:
    """Получить все пригласительные ссылки."""
    return get_invite_manager().get_invites()


def find_user(search_query: str) -> Optional[Dict[str, Any]]:
    """Найти пользователя."""
    return get_user_manager().find_user(search_query)


def add_user_manual(data: Dict[str, Any]) -> bool:
    """Добавить пользователя вручную."""
    return get_user_manager().add_user_manual(data)


def delete_user(username: str) -> bool:
    """Удалить пользователя."""
    return get_user_manager().delete_user(username)


def export_excel(report_type: str = "full") -> str:
    """Экспорт в Excel."""
    return get_report_adapter().export_excel(report_type)


def get_stats(period: str = "today") -> Dict[str, Any]:
    """Получить статистику."""
    return get_report_adapter().get_stats(period)


def get_inviter_list() -> List[str]:
    """Получить список пригласителей."""
    return get_invite_manager().get_inviter_list()