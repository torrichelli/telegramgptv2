#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Модуль планировщика задач для автоматической отправки отчётов.

Обеспечивает:
- Ежедневную отправку отчётов в указанные чаты
- Настройку времени отправки 
- Работу с московским временем
- Логирование работы планировщика
"""

import asyncio
import logging
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional, Set, Any, Union, TYPE_CHECKING
from dataclasses import dataclass, field
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

# Type checking imports
if TYPE_CHECKING:
    from aiogram import Bot as AiogramBot
    from aiogram.types import FSInputFile as AiogramFSInputFile

# Runtime imports
try:
    from aiogram import Bot
    from aiogram.types import FSInputFile
    AIOGRAM_AVAILABLE = True
except ImportError:
    # Fallback for when aiogram is not available
    AIOGRAM_AVAILABLE = False
    Bot = Any
    FSInputFile = Any

from db.db import DatabaseManager
from utils.time_utils import get_almaty_now, format_datetime_for_report
from utils.logging_conf import get_logger
from reports.report_manager import ReportManager
from reports.unified_report_manager import UnifiedReportManager

logger = get_logger(__name__)


@dataclass
class ScheduleConfig:
    """Конфигурация расписания отправки отчётов."""
    report_time: time = field(default_factory=lambda: time(23, 59))  # 23:59 по Алматы
    target_chats: List[int] = field(default_factory=list)  # Список chat_id для отправки
    enabled: bool = True
    report_types: List[str] = field(default_factory=lambda: ['daily'])  # daily, weekly, monthly


class ReportScheduler:
    """
    Планировщик автоматической отправки отчётов по расписанию.
    
    Поддерживает:
    - Ежедневные отчёты в указанное время
    - Еженедельные отчёты (понедельник)
    - Месячные отчёты (1 число месяца)
    - Отправку в несколько чатов
    - Алматинское время
    """
    
    def __init__(self, bot: Optional['AiogramBot'], db_manager: DatabaseManager, 
                 reports_dir: Optional[str] = None):
        """
        Инициализация планировщика.
        
        Args:
            bot: Экземпляр Telegram бота (может быть None если aiogram недоступен)
            db_manager: Менеджер базы данных
            reports_dir: Директория для сохранения отчётов
        """
        self.bot = bot
        self.db_manager = db_manager
        reports_dir_str = reports_dir if reports_dir is not None else "reports_output"
        self.report_manager = ReportManager(db_manager, reports_dir_str)
        # Initialize unified report manager for TZ-compliant reports
        self.unified_report_manager = UnifiedReportManager(db_manager, reports_dir_str)
        self.config = ScheduleConfig()
        self.scheduler = AsyncIOScheduler(timezone='Europe/Moscow')
        self.running = False
        self._sent_today: Set[str] = set()  # Отслеживание отправленных отчётов
        
        logger.info("🕒 [Scheduler] Initialized with default config")
    
    async def configure(self, report_time: Optional[time] = None,
                        target_chats: Optional[List[int]] = None,
                        enabled: Optional[bool] = None,
                        report_types: Optional[List[str]] = None) -> None:
        """
        Настройка планировщика.
        
        Args:
            report_time: Время отправки отчётов (по Москве)
            target_chats: Список chat_id для отправки
            enabled: Включён ли планировщик
            report_types: Типы отчётов для отправки
        """
        if report_time is not None:
            self.config.report_time = report_time
        if target_chats is not None:
            self.config.target_chats = target_chats
        if enabled is not None:
            self.config.enabled = enabled
        if report_types is not None:
            self.config.report_types = report_types
        
        # Auto-enable if we have target chats and use 23:59 timing 
        if len(self.config.target_chats) > 0 and not self.config.enabled:
            self.config.enabled = True
            logger.info(f"🔄 [Scheduler] Auto-enabled scheduler with {len(self.config.target_chats)} target chats")
        
        logger.info(f"🔧 [Scheduler] Configuration updated: time={self.config.report_time}, "
                   f"chats={len(self.config.target_chats)}, enabled={self.config.enabled}")
        
        # Перенастроить расписание если планировщик запущен
        if self.running:
            await self._setup_schedule()
    
    def add_target_chat(self, chat_id: int) -> None:
        """Добавить чат в список для отправки отчётов."""
        if chat_id not in self.config.target_chats:
            self.config.target_chats.append(chat_id)
            logger.info(f"📝 [Scheduler] Added target chat: {chat_id}")
    
    def remove_target_chat(self, chat_id: int) -> None:
        """Удалить чат из списка для отправки отчётов."""
        if chat_id in self.config.target_chats:
            self.config.target_chats.remove(chat_id)
            logger.info(f"🗑️ [Scheduler] Removed target chat: {chat_id}")
    
    async def start(self) -> None:
        """Запустить планировщик."""
        if self.running:
            logger.warning("⚠️ [Scheduler] Already running")
            return
        
        if not self.config.enabled:
            logger.info("⏸️ [Scheduler] Disabled in config, not starting")
            return
        
        if not self.config.target_chats:
            logger.warning("⚠️ [Scheduler] No target chats configured")
            return
        
        if not AIOGRAM_AVAILABLE or self.bot is None:
            logger.warning("⚠️ [Scheduler] Aiogram not available or bot is None, scheduler disabled")
            return
        
        self.running = True
        await self._setup_schedule()
        self.scheduler.start()
        
        logger.info(f"🚀 [Scheduler] Started with {len(self.config.target_chats)} target chats")
    
    async def stop(self) -> None:
        """Остановить планировщик."""
        if not self.running:
            return
        
        self.running = False
        self.scheduler.shutdown(wait=True)
        
        logger.info("🛑 [Scheduler] Stopped")
    
    async def _setup_schedule(self) -> None:
        """Настроить расписание отправки отчётов."""
        # Очистить существующие задачи
        self.scheduler.remove_all_jobs()
        
        # Очистить старые записи о отправленных отчётах
        self._cleanup_sent_today()
        
        if not self.config.enabled:
            return
        
        # Ежедневные отчёты
        if 'daily' in self.config.report_types:
            self.scheduler.add_job(
                self._send_daily_reports,
                CronTrigger(
                    hour=self.config.report_time.hour,
                    minute=self.config.report_time.minute,
                    timezone='Europe/Moscow'
                ),
                id='daily_reports',
                replace_existing=True,
                misfire_grace_time=300
            )
            logger.info(f"📅 [Scheduler] Daily reports scheduled for {self.config.report_time}")
        
        # Еженедельные отчёты (понедельник)
        if 'weekly' in self.config.report_types:
            self.scheduler.add_job(
                self._send_weekly_reports,
                CronTrigger(
                    day_of_week='mon',  # Monday
                    hour=self.config.report_time.hour,
                    minute=self.config.report_time.minute,
                    timezone='Europe/Moscow'
                ),
                id='weekly_reports',
                replace_existing=True,
                misfire_grace_time=300
            )
            logger.info(f"📊 [Scheduler] Weekly reports scheduled for Monday {self.config.report_time}")
        
        # Месячные отчёты (1 число месяца)
        if 'monthly' in self.config.report_types:
            self.scheduler.add_job(
                self._send_monthly_reports,
                CronTrigger(
                    day=1,  # 1st of month
                    hour=self.config.report_time.hour,
                    minute=self.config.report_time.minute,
                    timezone='Europe/Moscow'
                ),
                id='monthly_reports',
                replace_existing=True,
                misfire_grace_time=300
            )
            logger.info(f"📈 [Scheduler] Monthly reports scheduled for 1st of month {self.config.report_time}")
    
    def _cleanup_sent_today(self) -> None:
        """Очистить старые записи о отправленных отчётах для предотвращения роста памяти."""
        current_date = get_almaty_now().strftime('%Y-%m-%d')
        current_week = get_almaty_now().strftime('%Y-W%W')
        current_month = get_almaty_now().strftime('%Y-%m')
        
        # Оставить только записи за текущий день/неделю/месяц
        keys_to_keep = {
            f"daily_{current_date}",
            f"weekly_{current_week}", 
            f"monthly_{current_month}"
        }
        
        old_count = len(self._sent_today)
        self._sent_today = {key for key in self._sent_today if key in keys_to_keep}
        
        cleaned_count = old_count - len(self._sent_today)
        if cleaned_count > 0:
            logger.info(f"🧹 [Scheduler] Cleaned up {cleaned_count} old sent_today entries")
    
    async def _send_daily_reports(self) -> None:
        """Отправить ежедневные отчёты."""
        report_key = f"daily_{get_almaty_now().strftime('%Y-%m-%d')}"
        
        if report_key in self._sent_today:
            logger.info(f"⏭️ [Scheduler] Daily report already sent today: {report_key}")
            return
        
        logger.info("📅 [Scheduler] Generating daily report")
        
        try:
            # Генерация отчёта за вчерашний день с помощью unified system
            yesterday = get_almaty_now() - timedelta(days=1)
            target_date = yesterday.strftime("%Y-%m-%d")
            
            # Get unified daily report message with download button
            message_text, download_keyboard = self.unified_report_manager.get_daily_message_with_button(target_date)
            file_path = self.unified_report_manager.get_excel_file_path()
            
            if not file_path:
                logger.error("❌ [Scheduler] No excel file path in unified report data")
                return
            
            # Отправка во все настроенные чаты с сообщением согласно ТЗ
            await self._send_unified_daily_reports(
                message_text=message_text,
                file_path=file_path,
                keyboard=download_keyboard
            )
            
            self._sent_today.add(report_key)
            logger.info(f"✅ [Scheduler] Daily report sent successfully: {target_date}")
            
        except Exception as e:
            logger.exception(f"❌ [Scheduler] Failed to send daily report: {e}")
    
    async def _send_unified_daily_reports(self, message_text: str, file_path: str, 
                                         keyboard: 'InlineKeyboardMarkup') -> None:
        """Отправить ежедневные отчёты в формате ТЗ с кнопкой скачивания."""
        if not self.bot or not AIOGRAM_AVAILABLE:
            logger.warning("⚠️ [Scheduler] Bot not available for unified daily reports")
            return
        
        for chat_id in self.config.target_chats:
            try:
                # Send message with keyboard
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=message_text,
                    reply_markup=keyboard,
                    parse_mode=None
                )
                
                logger.info(f"📤 [Scheduler] Unified daily report sent to chat {chat_id}")
                
            except Exception as e:
                logger.error(f"❌ [Scheduler] Failed to send unified daily report to chat {chat_id}: {e}")
    
    async def _send_weekly_reports(self) -> None:
        """Отправить еженедельные отчёты."""
        report_key = f"weekly_{get_almaty_now().strftime('%Y-W%W')}"
        
        if report_key in self._sent_today:
            logger.info(f"⏭️ [Scheduler] Weekly report already sent this week: {report_key}")
            return
        
        logger.info("📊 [Scheduler] Generating weekly report")
        
        try:
            # Отчёт за прошлую неделю
            now = get_almaty_now()
            days_since_monday = now.weekday()
            last_monday = now - timedelta(days=days_since_monday + 7)
            target_date = last_monday.strftime("%Y-%m-%d")
            
            report_data = self.report_manager.generate_weekly_report(target_date)
            file_path = report_data.get('excel_file')
            
            if not file_path:
                logger.error("❌ [Scheduler] No excel file path in weekly report data")
                return
            
            # Отправка во все настроенные чаты
            await self._send_report_to_chats(
                file_path=file_path,
                caption=f"📊 Еженедельный отчёт с {format_datetime_for_report(last_monday, False)}",
                report_type="weekly"
            )
            
            self._sent_today.add(report_key)
            logger.info(f"✅ [Scheduler] Weekly report sent successfully: {target_date}")
            
        except Exception as e:
            logger.exception(f"❌ [Scheduler] Failed to send weekly report: {e}")
    
    async def _send_monthly_reports(self) -> None:
        """Отправить месячный отчёт."""
        now = get_almaty_now()
        report_key = f"monthly_{now.strftime('%Y-%m')}"
        
        if report_key in self._sent_today:
            logger.info(f"⏭️ [Scheduler] Monthly report already sent this month: {report_key}")
            return
        
        logger.info("📈 [Scheduler] Generating monthly report")
        
        try:
            # Отчёт за прошлый месяц
            if now.month == 1:
                last_month = now.replace(year=now.year - 1, month=12, day=1)
            else:
                last_month = now.replace(month=now.month - 1, day=1)
            
            target_date = last_month.strftime("%Y-%m-%d")
            
            report_data = self.report_manager.generate_monthly_report(target_date)
            file_path = report_data.get('excel_file')
            
            if not file_path:
                logger.error("❌ [Scheduler] No excel file path in monthly report data")
                return
            
            # Отправка во все настроенные чаты
            await self._send_report_to_chats(
                file_path=file_path,
                caption=f"📈 Месячный отчёт за {format_datetime_for_report(last_month, False)}",
                report_type="monthly"
            )
            
            self._sent_today.add(report_key)
            logger.info(f"✅ [Scheduler] Monthly report sent successfully: {target_date}")
            
        except Exception as e:
            logger.exception(f"❌ [Scheduler] Failed to send monthly report: {e}")
    
    async def _send_report_to_chats(self, file_path: str, caption: str, 
                                   report_type: str) -> None:
        """
        Отправить отчёт во все настроенные чаты.
        
        Args:
            file_path: Путь к файлу отчёта
            caption: Подпись к файлу
            report_type: Тип отчёта для логирования
        """
        if not self.config.target_chats:
            logger.warning(f"⚠️ [Scheduler] No target chats for {report_type} report")
            return
        
        if not AIOGRAM_AVAILABLE or self.bot is None:
            logger.error(f"❌ [Scheduler] Aiogram not available or bot is None")
            return
        
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            logger.error(f"❌ [Scheduler] Report file not found: {file_path}")
            return
        
        success_count = 0
        
        for chat_id in self.config.target_chats:
            try:
                # Отправка файла
                if not AIOGRAM_AVAILABLE or FSInputFile is Any:
                    logger.error("❌ [Scheduler] FSInputFile not available")
                    continue
                document = FSInputFile(file_path_obj, filename=file_path_obj.name)
                await self.bot.send_document(
                    chat_id=chat_id,
                    document=document,
                    caption=caption
                )
                
                success_count += 1
                logger.info(f"📤 [Scheduler] {report_type.title()} report sent to chat {chat_id}")
                
            except Exception as e:
                logger.exception(f"❌ [Scheduler] Failed to send {report_type} report to chat {chat_id}: {e}")
        
        logger.info(f"📊 [Scheduler] {report_type.title()} report sent to {success_count}/{len(self.config.target_chats)} chats")
    
    async def send_test_report(self, chat_id: int, report_type: str = "daily") -> bool:
        """
        Отправить тестовый отчёт в указанный чат.
        
        Args:
            chat_id: ID чата для отправки
            report_type: Тип отчёта (daily, weekly, monthly)
            
        Returns:
            bool: Успешность отправки
        """
        logger.info(f"🧪 [Scheduler] Sending test {report_type} report to chat {chat_id}")
        
        try:
            # Генерация тестового отчёта
            now = get_almaty_now()
            
            if report_type == "daily":
                yesterday = now - timedelta(days=1)
                target_date = yesterday.strftime("%Y-%m-%d")
                report_data = self.report_manager.generate_daily_report(target_date)
                caption = f"🧪 Тестовый ежедневный отчёт за {format_datetime_for_report(yesterday, False)}"
                
            elif report_type == "weekly":
                days_since_monday = now.weekday()
                last_monday = now - timedelta(days=days_since_monday + 7)
                target_date = last_monday.strftime("%Y-%m-%d")
                report_data = self.report_manager.generate_weekly_report(target_date)
                caption = f"🧪 Тестовый еженедельный отчёт с {format_datetime_for_report(last_monday, False)}"
                
            elif report_type == "monthly":
                if now.month == 1:
                    last_month = now.replace(year=now.year - 1, month=12, day=1)
                else:
                    last_month = now.replace(month=now.month - 1, day=1)
                target_date = last_month.strftime("%Y-%m-%d")
                report_data = self.report_manager.generate_monthly_report(target_date)
                caption = f"🧪 Тестовый месячный отчёт за {format_datetime_for_report(last_month, False)}"
                
            else:
                logger.error(f"❌ [Scheduler] Unknown report type: {report_type}")
                return False
            
            file_path = report_data.get('excel_file')
            if not file_path:
                logger.error("❌ [Scheduler] No excel file path in test report data")
                return False
                
            # Отправка отчёта
            file_path_obj = Path(file_path)
            if not file_path_obj.exists():
                logger.error(f"❌ [Scheduler] Test report file not found: {file_path}")
                return False
            
            if not AIOGRAM_AVAILABLE or self.bot is None:
                logger.error("❌ [Scheduler] Aiogram not available or bot is None")
                return False
            
            if FSInputFile is Any:
                logger.error("❌ [Scheduler] FSInputFile not available")
                return False
            
            document = FSInputFile(file_path_obj, filename=file_path_obj.name)
            await self.bot.send_document(
                chat_id=chat_id,
                document=document,
                caption=caption
            )
            
            logger.info(f"✅ [Scheduler] Test {report_type} report sent successfully to chat {chat_id}")
            return True
            
        except Exception as e:
            logger.exception(f"❌ [Scheduler] Failed to send test {report_type} report to chat {chat_id}: {e}")
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """
        Получить статус планировщика.
        
        Returns:
            Dict с информацией о состоянии планировщика
        """
        return {
            'running': self.running,
            'config': {
                'enabled': self.config.enabled,
                'report_time': self.config.report_time.strftime('%H:%M'),
                'target_chats_count': len(self.config.target_chats),
                'report_types': self.config.report_types
            },
            'jobs': [job.id for job in self.scheduler.get_jobs()] if self.running else [],
            'sent_today': list(self._sent_today),
            'aiogram_available': AIOGRAM_AVAILABLE,
            'bot_available': self.bot is not None
        }
    
    # Методы для ручной отправки отчётов (для тестирования)
    
    async def send_daily_report_now(self, target_date: Optional[str] = None) -> str:
        """
        Отправить ежедневный отчёт прямо сейчас.
        
        Args:
            target_date: Дата отчёта в формате YYYY-MM-DD (по умолчанию - вчера)
            
        Returns:
            str: Путь к сгенерированному файлу
        """
        logger.info("🚀 [Scheduler] Manual daily report generation requested")
        
        if target_date is None:
            yesterday = get_almaty_now() - timedelta(days=1)
            target_date = yesterday.strftime("%Y-%m-%d")
        
        try:
            report_data = self.report_manager.generate_daily_report(target_date)
            file_path = report_data.get('excel_file')
            
            if not file_path:
                raise ValueError("No excel file path in report data")
            
            if self.config.target_chats and AIOGRAM_AVAILABLE and self.bot is not None:
                await self._send_report_to_chats(
                    file_path=file_path,
                    caption=f"📅 Ежедневный отчёт за {target_date}",
                    report_type="daily"
                )
            
            logger.info(f"✅ [Scheduler] Manual daily report completed: {target_date}")
            return file_path
            
        except Exception as e:
            logger.exception(f"❌ [Scheduler] Manual daily report failed: {e}")
            raise
    
    async def send_weekly_report_now(self, target_date: Optional[str] = None) -> str:
        """
        Отправить еженедельный отчёт прямо сейчас.
        
        Args:
            target_date: Начальная дата недели в формате YYYY-MM-DD
            
        Returns:
            str: Путь к сгенерированному файлу
        """
        logger.info("🚀 [Scheduler] Manual weekly report generation requested")
        
        try:
            report_data = self.report_manager.generate_weekly_report(target_date)
            file_path = report_data.get('excel_file')
            
            if not file_path:
                raise ValueError("No excel file path in report data")
            
            if self.config.target_chats and AIOGRAM_AVAILABLE and self.bot is not None:
                await self._send_report_to_chats(
                    file_path=file_path,
                    caption=f"📊 Еженедельный отчёт",
                    report_type="weekly"
                )
            
            logger.info(f"✅ [Scheduler] Manual weekly report completed")
            return file_path
            
        except Exception as e:
            logger.exception(f"❌ [Scheduler] Manual weekly report failed: {e}")
            raise
    
    async def send_monthly_report_now(self, target_date: Optional[str] = None) -> str:
        """
        Отправить месячный отчёт прямо сейчас.
        
        Args:
            target_date: Начальная дата месяца в формате YYYY-MM-DD
            
        Returns:
            str: Путь к сгенерированному файлу
        """
        logger.info("🚀 [Scheduler] Manual monthly report generation requested")
        
        try:
            report_data = self.report_manager.generate_monthly_report(target_date)
            file_path = report_data.get('excel_file')
            
            if not file_path:
                raise ValueError("No excel file path in report data")
            
            if self.config.target_chats and AIOGRAM_AVAILABLE and self.bot is not None:
                await self._send_report_to_chats(
                    file_path=file_path,
                    caption=f"📈 Месячный отчёт",
                    report_type="monthly"
                )
            
            logger.info(f"✅ [Scheduler] Manual monthly report completed")
            return file_path
            
        except Exception as e:
            logger.exception(f"❌ [Scheduler] Manual monthly report failed: {e}")
            raise


# Функции для упрощения работы с планировщиком в других модулях
async def schedule_daily_report(scheduler: ReportScheduler, target_date: Optional[str] = None) -> str:
    """
    Создать и отправить ежедневный отчёт вручную.
    
    Args:
        scheduler: Экземпляр планировщика
        target_date: Дата для отчёта (YYYY-MM-DD), по умолчанию вчера
        
    Returns:
        str: Путь к созданному файлу
    """
    if target_date is None:
        yesterday = get_almaty_now() - timedelta(days=1)
        target_date = yesterday.strftime("%Y-%m-%d")
    
    logger.info(f"📅 [Manual] Generating daily report for {target_date}")
    report_data = scheduler.report_manager.generate_daily_report(target_date)
    file_path = report_data.get('excel_file')
    if not file_path:
        raise ValueError("No excel file path in report data")
    return file_path


async def schedule_weekly_report(scheduler: ReportScheduler, target_date: Optional[str] = None) -> str:
    """
    Создать и отправить еженедельный отчёт вручную.
    
    Args:
        scheduler: Экземпляр планировщика
        target_date: Дата начала недели (YYYY-MM-DD), по умолчанию прошлый понедельник
        
    Returns:
        str: Путь к созданному файлу
    """
    if target_date is None:
        now = get_almaty_now()
        days_since_monday = now.weekday()
        last_monday = now - timedelta(days=days_since_monday + 7)
        target_date = last_monday.strftime("%Y-%m-%d")
    
    logger.info(f"📊 [Manual] Generating weekly report for {target_date}")
    report_data = scheduler.report_manager.generate_weekly_report(target_date)
    file_path = report_data.get('excel_file')
    if not file_path:
        raise ValueError("No excel file path in report data")
    return file_path


async def schedule_monthly_report(scheduler: ReportScheduler, target_date: Optional[str] = None) -> str:
    """
    Создать и отправить месячный отчёт вручную.
    
    Args:
        scheduler: Экземпляр планировщика
        target_date: Дата начала месяца (YYYY-MM-DD), по умолчанию прошлый месяц
        
    Returns:
        str: Путь к созданному файлу
    """
    if target_date is None:
        now = get_almaty_now()
        if now.month == 1:
            last_month = now.replace(year=now.year - 1, month=12, day=1)
        else:
            last_month = now.replace(month=now.month - 1, day=1)
        target_date = last_month.strftime("%Y-%m-%d")
    
    logger.info(f"📈 [Manual] Generating monthly report for {target_date}")
    report_data = scheduler.report_manager.generate_monthly_report(target_date)
    file_path = report_data.get('excel_file')
    if not file_path:
        raise ValueError("No excel file path in report data")
    return file_path