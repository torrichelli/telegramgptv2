#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Обработчики команд Telegram бота для управления отчётами.

Команды администратора:
- /start, /help - справка
- /stats - статистика за период  
- /report - генерация отчётов вручную
- /export - полный экспорт базы данных
- /schedule - управление расписанием
- /status - статус системы
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Optional, List
import json

from aiogram import Router, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, FSInputFile
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from pathlib import Path

from db.db import get_db
from reports.report_manager import ReportManager
from reports.scheduler import ReportScheduler
from utils.time_utils import get_almaty_now, format_datetime_for_report
from utils.logging_conf import get_logger

logger = get_logger(__name__)

# Создаём роутер для обработчиков команд
commands_router = Router(name="commands")

# Глобальный планировщик отчётов
report_scheduler: Optional[ReportScheduler] = None


def initialize_scheduler(bot) -> None:
    """Инициализация планировщика отчётов."""
    global report_scheduler
    
    try:
        db = get_db()
        report_scheduler = ReportScheduler(bot, db)
        logger.info("🕒 [Commands] ReportScheduler initialized successfully")
    except Exception as e:
        logger.exception(f"❌ [Commands] Failed to initialize ReportScheduler: {e}")


def get_scheduler() -> Optional[ReportScheduler]:
    """Получить экземпляр планировщика."""
    return report_scheduler

# Список ID администраторов (должен настраиваться через переменные окружения)
ADMIN_IDS: List[int] = []  # Будет заполнен при инициализации


def initialize_admin_ids() -> None:
    """Инициализация списка администраторов из переменных окружения."""
    global ADMIN_IDS
    
    admin_ids_str = os.environ.get('ADMIN_IDS', '')
    if not admin_ids_str:
        logger.warning("⚠️ [Commands] ADMIN_IDS environment variable not set - no administrators configured")
        return
    
    try:
        # Поддерживаем формат: "123456789,987654321" или "123456789, 987654321"
        admin_ids = [int(id_str.strip()) for id_str in admin_ids_str.split(',') if id_str.strip()]
        ADMIN_IDS.clear()
        ADMIN_IDS.extend(admin_ids)
        
        logger.info(f"✅ [Commands] Initialized {len(ADMIN_IDS)} administrators: {ADMIN_IDS}")
        
    except ValueError as e:
        logger.error(f"❌ [Commands] Invalid ADMIN_IDS format in environment variable: {admin_ids_str} - {e}")
    except Exception as e:
        logger.exception(f"❌ [Commands] Error initializing admin IDs: {e}")


# Инициализируем admin IDs при импорте модуля
initialize_admin_ids()

# FSM состояния для диалогов
class ReportStates(StatesGroup):
    waiting_for_period = State()
    waiting_for_date_range = State()
    waiting_for_schedule_time = State()
    waiting_for_chat_id = State()


def is_admin(user_id: int) -> bool:
    """Проверить, является ли пользователь администратором."""
    return user_id in ADMIN_IDS


def admin_only(handler):
    """Декоратор для команд только для администраторов."""
    async def wrapper(message: Message, *args, **kwargs):
        if not is_admin(message.from_user.id):
            await message.reply(
                "❌ У вас нет прав для выполнения этой команды.\n"
                "Доступ разрешён только администраторам."
            )
            return
        return await handler(message, *args, **kwargs)
    return wrapper


@commands_router.message(CommandStart())
async def handle_start(message: Message):
    """Обработчик команды /start."""
    user = message.from_user
    
    welcome_text = f"""
👋 Привет, {user.first_name}!

🤖 Я бот для отслеживания активности в Telegram чатах и каналах.

📊 **Что я умею:**
• Отслеживать подписки/отписки от каналов
• Мониторить активность в группах
• Генерировать отчёты в Excel
• Отправлять статистику по расписанию

📋 **Доступные команды:**
/help - показать все команды
/stats - получить статистику
/status - статус системы

🔒 **Для администраторов:**
/report - генерация отчётов
/export - экспорт базы данных
/schedule - управление расписанием

Добавьте меня в ваши чаты и каналы, чтобы я мог отслеживать активность!
    """
    
    await message.reply(welcome_text, parse_mode="Markdown")
    
    logger.info(f"👋 [Commands] Start command from user {user.id} (@{user.username})")


@commands_router.message(Command("help"))
async def handle_help(message: Message):
    """Обработчик команды /help."""
    help_text = """
📋 **Доступные команды:**

👤 **Для всех пользователей:**
/start - приветствие и информация о боте
/help - эта справка
/stats [период] - статистика активности
/status - статус работы системы

🔧 **Для администраторов:**
/report [тип] [дата] - генерация отчётов
  • daily YYYY-MM-DD - ежедневный отчёт
  • weekly YYYY-MM-DD - еженедельный отчёт  
  • monthly YYYY-MM-DD - месячный отчёт
  • retention [дни] [дата] - анализ удержания

/export - полный экспорт базы данных в Excel

/schedule - управление расписанием отправки:
  • config - настроить время и чаты
  • status - статус планировщика
  • test [тип] - отправить тестовый отчёт
  • enable/disable - включить/выключить

**Примеры:**
`/stats today` - статистика за сегодня
`/report daily 2024-01-15` - отчёт за 15 января
`/schedule config 09:00` - настроить отправку на 9:00
    """
    
    await message.reply(help_text, parse_mode="Markdown")


@commands_router.message(Command("stats"))
async def handle_stats(message: Message):
    """Обработчик команды /stats для получения статистики."""
    try:
        # Парсим аргументы команды
        args = message.text.split()[1:] if len(message.text.split()) > 1 else ["today"]
        period = args[0].lower()
        
        # Определяем период для статистики
        now = get_almaty_now()
        
        if period == "today":
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = now
            period_name = "сегодня"
        elif period == "yesterday":
            yesterday = now - timedelta(days=1)
            start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
            period_name = "вчера"
        elif period == "week":
            week_start = now - timedelta(days=now.weekday())
            start_date = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = now
            period_name = "за эту неделю"
        elif period == "month":
            month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            start_date = month_start
            end_date = now
            period_name = "за этот месяц"
        else:
            await message.reply(
                "❌ Неизвестный период. Используйте: today, yesterday, week, month"
            )
            return
        
        # Получаем статистику из базы данных
        db = get_db()
        events = db.get_events_for_period(
            start_date.strftime("%Y-%m-%d %H:%M:%S"),
            end_date.strftime("%Y-%m-%d %H:%M:%S")
        )
        
        if not events:
            await message.reply(f"📊 Статистика {period_name}: нет данных")
            return
        
        # Анализируем события
        stats = _analyze_events(events)
        
        # Формируем ответ
        stats_text = f"""
📊 **Статистика {period_name}:**

📈 **Общие показатели:**
• Всего событий: {stats['total_events']}
• Уникальных пользователей: {stats['unique_users']}
• Затронуто чатов: {stats['unique_chats']}

📢 **Каналы:**
• Подписки: {stats['channel_subscribes']}
• Отписки: {stats['channel_unsubscribes']}
• Чистый прирост: {stats['channel_net_growth']}

👥 **Группы:**  
• Присоединения: {stats['group_joins']}
• Выходы: {stats['group_leaves']}
• Чистый прирост: {stats['group_net_growth']}

🤖 **Бот:**
• Добавлен в чатов: {stats['bot_added']}
• Удалён из чатов: {stats['bot_removed']}

⏰ **Период:** {format_datetime_for_report(start_date)} - {format_datetime_for_report(end_date)}
        """
        
        await message.reply(stats_text, parse_mode="Markdown")
        
        logger.info(f"📊 [Commands] Stats request from user {message.from_user.id} for period {period}")
        
    except Exception as e:
        logger.exception(f"❌ [Commands] Error handling stats command: {e}")
        await message.reply("❌ Ошибка при получении статистики. Попробуйте позже.")


@commands_router.message(Command("status"))
async def handle_status(message: Message):
    """Обработчик команды /status для проверки работы системы."""
    try:
        db = get_db()
        
        # Проверяем подключение к БД
        try:
            db.get_user_stats_summary()
            db_status = "✅ Подключена"
        except Exception as e:
            db_status = f"❌ Ошибка: {str(e)[:50]}..."
        
        # Получаем общую статистику
        now = get_almaty_now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        try:
            today_events = db.get_events_for_period(
                today_start.strftime("%Y-%m-%d %H:%M:%S"),
                now.strftime("%Y-%m-%d %H:%M:%S")
            )
            events_today = len(today_events) if today_events else 0
        except:
            events_today = "Неизвестно"
        
        # Проверяем статус планировщика
        scheduler = get_scheduler()
        if scheduler:
            config = scheduler.config
            if scheduler.running and config.enabled:
                scheduler_status = f"✅ Работает (время: {config.report_time.strftime('%H:%M')}, чатов: {len(config.target_chats)})"
            elif config.enabled:
                scheduler_status = "⚠️ Включён, но не запущен"
            else:
                scheduler_status = "❌ Выключен"
        else:
            scheduler_status = "❌ Не инициализирован"
        
        status_text = f"""
🔧 **Статус системы:**

💾 **База данных:** {db_status}
📊 **События сегодня:** {events_today}
⏰ **Планировщик:** {scheduler_status}

⏰ **Время сервера:** {format_datetime_for_report(now)}
🌍 **Часовой пояс:** Москва (UTC+3)

💡 **Версия:** 1.0.0
🤖 **Бот ID:** {message.bot.id}
        """
        
        await message.reply(status_text, parse_mode="Markdown")
        
        logger.info(f"🔧 [Commands] Status request from user {message.from_user.id}")
        
    except Exception as e:
        logger.exception(f"❌ [Commands] Error handling status command: {e}")
        await message.reply("❌ Ошибка при получении статуса системы.")


@commands_router.message(Command("report"))
@admin_only
async def handle_report(message: Message, **kwargs):
    """Обработчик команды /report для генерации отчётов."""
    try:
        args = message.text.split()[1:]
        
        if not args:
            # Показываем интерактивное меню
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="📅 Ежедневный", callback_data="report_daily"),
                    InlineKeyboardButton(text="📊 Еженедельный", callback_data="report_weekly")
                ],
                [
                    InlineKeyboardButton(text="📈 Месячный", callback_data="report_monthly"),
                    InlineKeyboardButton(text="🔄 Удержание", callback_data="report_retention")
                ]
            ])
            
            await message.reply(
                "📋 **Генерация отчётов**\n\n"
                "Выберите тип отчёта или используйте команду:\n"
                "`/report [тип] [дата]`\n\n"
                "Примеры:\n"
                "• `/report daily 2024-01-15`\n"
                "• `/report weekly 2024-01-08`\n"
                "• `/report monthly 2024-01-01`\n"
                "• `/report retention 7 2024-01-15`",
                reply_markup=keyboard,
                parse_mode="Markdown"
            )
            return
        
        # Парсим аргументы
        report_type = args[0].lower()
        
        if report_type not in ["daily", "weekly", "monthly", "retention"]:
            await message.reply(
                "❌ Неизвестный тип отчёта. Доступны: daily, weekly, monthly, retention"
            )
            return
        
        # Обрабатываем дату
        target_date = None
        retention_days = 7
        
        if len(args) > 1:
            if report_type == "retention" and len(args) > 2:
                try:
                    retention_days = int(args[1])
                    target_date = args[2]
                except ValueError:
                    await message.reply("❌ Неверный формат. Используйте: /report retention [дни] [дата]")
                    return
            else:
                target_date = args[1]
        
        # Генерируем отчёт
        await _generate_and_send_report(message, report_type, target_date, retention_days)
        
    except Exception as e:
        logger.exception(f"❌ [Commands] Error handling report command: {e}")
        await message.reply("❌ Ошибка при генерации отчёта. Попробуйте позже.")


@commands_router.message(Command("export"))
@admin_only
async def handle_export(message: Message, **kwargs):
    """Обработчик команды /export для полного экспорта БД."""
    try:
        # Отправляем уведомление о начале экспорта
        status_msg = await message.reply("⏳ Генерирую полный экспорт базы данных...")
        
        # Создаём экспорт
        db = get_db()
        report_manager = ReportManager(db)
        file_path = report_manager.export_full_database()
        
        # Отправляем файл
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            await message.reply("❌ Файл экспорта не найден")
            return
            
        document = FSInputFile(file_path_obj, filename=file_path_obj.name)
        await message.bot.send_document(
            chat_id=message.chat.id,
            document=document,
            caption=f"📁 Полный экспорт базы данных\n"
                   f"🕐 Создан: {format_datetime_for_report(get_almaty_now())}"
        )
        
        # Удаляем сообщение о статусе
        await status_msg.delete()
        
        logger.info(f"📁 [Commands] Database export sent to user {message.from_user.id}")
        
    except Exception as e:
        logger.exception(f"❌ [Commands] Error handling export command: {e}")
        await message.reply("❌ Ошибка при экспорте базы данных. Попробуйте позже.")


@commands_router.message(Command("schedule"))
@admin_only  
async def handle_schedule(message: Message, **kwargs):
    """Обработчик команды /schedule для управления расписанием."""
    try:
        args = message.text.split()[1:] if len(message.text.split()) > 1 else []
        
        if not args:
            # Показать справку по команде
            help_text = """
⏰ **Управление расписанием отчётов**

📋 **Доступные команды:**
• `/schedule status` - статус планировщика
• `/schedule config [время] [чаты]` - настроить расписание
• `/schedule enable` - включить планировщик
• `/schedule disable` - выключить планировщик
• `/schedule test [тип]` - отправить тестовый отчёт

**Примеры:**
• `/schedule config 09:00` - настроить на 9:00
• `/schedule test daily` - тестовый ежедневный отчёт
            """
            await message.reply(help_text, parse_mode="Markdown")
            return
        
        command = args[0].lower()
        
        if command == "status":
            await _handle_schedule_status(message)
        elif command == "config":
            await _handle_schedule_config(message, args[1:] if len(args) > 1 else [])
        elif command == "enable":
            await _handle_schedule_enable(message)
        elif command == "disable":
            await _handle_schedule_disable(message)
        elif command == "test":
            report_type = args[1] if len(args) > 1 else "daily"
            await _handle_schedule_test(message, report_type)
        else:
            await message.reply(
                "❌ Неизвестная команда. Используйте `/schedule` для справки.",
                parse_mode="Markdown"
            )
            
    except Exception as e:
        logger.exception(f"❌ [Commands] Error handling schedule command: {e}")
        await message.reply("❌ Ошибка при обработке команды планировщика.")


@commands_router.callback_query(F.data.startswith("report_"))
async def handle_report_callback(callback: CallbackQuery, **kwargs):
    """Обработчик нажатий на кнопки отчётов."""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    try:
        report_type = callback.data.split("_")[1]
        
        # Генерируем отчёт с датой по умолчанию
        await _generate_and_send_report(callback.message, report_type)
        
        await callback.answer("✅ Генерирую отчёт...")
        
    except Exception as e:
        logger.exception(f"❌ [Commands] Error handling report callback: {e}")
        await callback.answer("❌ Ошибка при генерации отчёта", show_alert=True)


async def _generate_and_send_report(message: Message, report_type: str, 
                                  target_date: Optional[str] = None,
                                  retention_days: int = 7):
    """
    Генерировать и отправить отчёт пользователю.
    
    Args:
        message: Сообщение для ответа
        report_type: Тип отчёта (daily, weekly, monthly, retention)
        target_date: Целевая дата (YYYY-MM-DD)
        retention_days: Дни для анализа удержания
    """
    status_msg = None
    try:
        # Отправляем уведомление о начале генерации
        status_msg = await message.reply(f"⏳ Генерирую {report_type} отчёт...")
        
        # Создаём менеджер отчётов
        db = get_db()
        report_manager = ReportManager(db)
        
        # Определяем дату по умолчанию если не указана
        if not target_date:
            now = get_almaty_now()
            if report_type == "daily":
                yesterday = now - timedelta(days=1)
                target_date = yesterday.strftime("%Y-%m-%d")
            elif report_type == "weekly":
                days_since_monday = now.weekday()
                last_monday = now - timedelta(days=days_since_monday + 7)
                target_date = last_monday.strftime("%Y-%m-%d")
            elif report_type == "monthly":
                if now.month == 1:
                    last_month = now.replace(year=now.year - 1, month=12, day=1)
                else:
                    last_month = now.replace(month=now.month - 1, day=1)
                target_date = last_month.strftime("%Y-%m-%d")
            else:  # retention
                yesterday = now - timedelta(days=1)
                target_date = yesterday.strftime("%Y-%m-%d")
        
        # Генерируем отчёт
        file_path = None
        caption = None
        
        if report_type == "daily":
            result = report_manager.generate_daily_report(target_date)
            file_path = result.get('excel_file')
            caption = f"📅 Ежедневный отчёт за {target_date}"
        elif report_type == "weekly":
            result = report_manager.generate_weekly_report(target_date)
            file_path = result.get('excel_file')
            caption = f"📊 Еженедельный отчёт с {target_date}"
        elif report_type == "monthly":
            result = report_manager.generate_monthly_report(target_date)
            file_path = result.get('excel_file')
            caption = f"📈 Месячный отчёт с {target_date}"
        elif report_type == "retention":
            result = report_manager.generate_retention_report(retention_days, target_date)
            file_path = result.get('excel_file')
            caption = f"🔄 Анализ удержания ({retention_days} дней) за {target_date}"
        else:
            await status_msg.edit_text("❌ Неизвестный тип отчёта")
            return
        
        if not file_path:
            await status_msg.edit_text("❌ Ошибка: не удалось получить путь к файлу")
            return
        
        # Отправляем файл
        file_path_obj = Path(str(file_path))
        if not file_path_obj.exists():
            await status_msg.edit_text("❌ Файл отчёта не найден")
            return
            
        document = FSInputFile(file_path_obj, filename=file_path_obj.name)
        await message.bot.send_document(
            chat_id=message.chat.id,
            document=document,
            caption=f"{caption}\n🕐 Создан: {format_datetime_for_report(get_almaty_now())}"
        )
        
        # Удаляем сообщение о статусе
        await status_msg.delete()
        
        logger.info(f"📋 [Commands] {report_type.title()} report sent to user {message.from_user.id}")
        
    except Exception as e:
        logger.exception(f"❌ [Commands] Error generating {report_type} report: {e}")
        if status_msg:
            try:
                await status_msg.edit_text(f"❌ Ошибка при генерации {report_type} отчёта")
            except Exception:
                await message.reply(f"❌ Ошибка при генерации {report_type} отчёта")
        else:
            await message.reply(f"❌ Ошибка при генерации {report_type} отчёта")


def _analyze_events(events: List[dict]) -> dict:
    """
    Анализировать список событий и вернуть статистику.
    
    Args:
        events: Список событий из БД
        
    Returns:
        dict: Словарь со статистикой
    """
    stats = {
        'total_events': len(events),
        'unique_users': len(set(event['user_id'] for event in events)),
        'unique_chats': len(set(event['chat_id'] for event in events)),
        'channel_subscribes': 0,
        'channel_unsubscribes': 0,
        'group_joins': 0,
        'group_leaves': 0,
        'bot_added': 0,
        'bot_removed': 0,
    }
    
    for event in events:
        event_type = event['event_type']
        
        if event_type == 'channel_subscribe':
            stats['channel_subscribes'] += 1
        elif event_type == 'channel_unsubscribe':
            stats['channel_unsubscribes'] += 1
        elif event_type == 'group_join':
            stats['group_joins'] += 1
        elif event_type == 'group_leave':
            stats['group_leaves'] += 1
        elif event_type == 'bot_added':
            stats['bot_added'] += 1
        elif event_type == 'bot_removed':
            stats['bot_removed'] += 1
    
    # Вычисляем чистый прирост
    stats['channel_net_growth'] = stats['channel_subscribes'] - stats['channel_unsubscribes']
    stats['group_net_growth'] = stats['group_joins'] - stats['group_leaves']
    
    return stats


def configure_admin_ids(admin_ids: List[int]):
    """Настроить список ID администраторов."""
    global ADMIN_IDS
    ADMIN_IDS = admin_ids
    logger.info(f"🔧 [Commands] Configured {len(admin_ids)} admin IDs")


async def _handle_schedule_status(message: Message) -> None:
    """Обработать команду /schedule status."""
    scheduler = get_scheduler()
    
    if not scheduler:
        await message.reply("❌ Планировщик не инициализирован")
        return
    
    config = scheduler.config
    status_text = f"""
🕒 **Статус планировщика отчётов:**

📊 **Состояние:** {'✅ Включён' if config.enabled else '❌ Выключен'}
⏰ **Время отправки:** {config.report_time.strftime('%H:%M')} (МСК)
📨 **Целевые чаты:** {len(config.target_chats)} шт.
📋 **Типы отчётов:** {', '.join(config.report_types)}
🔄 **Запущен:** {'✅ Да' if scheduler.running else '❌ Нет'}

💡 **Чаты для отправки:**
{chr(10).join([f'• {chat_id}' for chat_id in config.target_chats]) if config.target_chats else '• Не настроены'}
    """
    
    await message.reply(status_text, parse_mode="Markdown")


async def _handle_schedule_config(message: Message, args: List[str]) -> None:
    """Обработать команду /schedule config."""
    scheduler = get_scheduler()
    
    if not scheduler:
        await message.reply("❌ Планировщик не инициализирован")
        return
    
    try:
        if not args:
            # Показать текущую конфигурацию
            config = scheduler.config
            config_text = f"""
⚙️ **Текущая конфигурация:**

⏰ **Время:** {config.report_time.strftime('%H:%M')} (МСК)
📨 **Чаты:** {', '.join(map(str, config.target_chats)) if config.target_chats else 'Не настроены'}
📋 **Типы:** {', '.join(config.report_types)}

**Для настройки используйте:**
`/schedule config [время] [chat_id1,chat_id2,...]`
            """
            await message.reply(config_text, parse_mode="Markdown")
            return
        
        # Парсим время
        time_str = args[0]
        try:
            from datetime import time
            hour, minute = map(int, time_str.split(':'))
            report_time = time(hour, minute)
        except ValueError:
            await message.reply("❌ Неверный формат времени. Используйте HH:MM (например, 09:00)")
            return
        
        # Парсим чаты если указаны
        target_chats = []
        if len(args) > 1:
            try:
                chat_ids_str = args[1]
                target_chats = [int(chat_id.strip()) for chat_id in chat_ids_str.split(',') if chat_id.strip()]
            except ValueError:
                await message.reply("❌ Неверный формат ID чатов. Используйте числа через запятую")
                return
        else:
            # Если чаты не указаны, добавляем текущий чат
            target_chats = [message.chat.id]
        
        # Применяем конфигурацию
        await scheduler.configure(
            report_time=report_time,
            target_chats=target_chats
        )
        
        await message.reply(
            f"✅ Планировщик настроен:\n"
            f"⏰ Время: {report_time.strftime('%H:%M')} (МСК)\n"
            f"📨 Чаты: {', '.join(map(str, target_chats))}",
            parse_mode="Markdown"
        )
        
    except Exception as e:
        logger.exception(f"❌ [Commands] Error configuring scheduler: {e}")
        await message.reply("❌ Ошибка при настройке планировщика")


async def _handle_schedule_enable(message: Message) -> None:
    """Обработать команду /schedule enable."""
    scheduler = get_scheduler()
    
    if not scheduler:
        await message.reply("❌ Планировщик не инициализирован")
        return
    
    try:
        await scheduler.configure(enabled=True)
        await scheduler.start()
        await message.reply("✅ Планировщик включён")
        
    except Exception as e:
        logger.exception(f"❌ [Commands] Error enabling scheduler: {e}")
        await message.reply("❌ Ошибка при включении планировщика")


async def _handle_schedule_disable(message: Message) -> None:
    """Обработать команду /schedule disable."""
    scheduler = get_scheduler()
    
    if not scheduler:
        await message.reply("❌ Планировщик не инициализирован")
        return
    
    try:
        await scheduler.configure(enabled=False)
        await scheduler.stop()
        await message.reply("✅ Планировщик выключён")
        
    except Exception as e:
        logger.exception(f"❌ [Commands] Error disabling scheduler: {e}")
        await message.reply("❌ Ошибка при выключении планировщика")


async def _handle_schedule_test(message: Message, report_type: str) -> None:
    """Обработать команду /schedule test."""
    scheduler = get_scheduler()
    
    if not scheduler:
        await message.reply("❌ Планировщик не инициализирован")
        return
    
    if report_type not in ["daily", "weekly", "monthly"]:
        await message.reply("❌ Неизвестный тип отчёта. Используйте: daily, weekly, monthly")
        return
    
    try:
        status_msg = await message.reply(f"⏳ Генерирую тестовый {report_type} отчёт...")
        
        success = await scheduler.send_test_report(message.chat.id, report_type)
        
        if success:
            await status_msg.edit_text(f"✅ Тестовый {report_type} отчёт отправлен")
        else:
            await status_msg.edit_text(f"❌ Ошибка при отправке тестового {report_type} отчёта")
            
    except Exception as e:
        logger.exception(f"❌ [Commands] Error sending test report: {e}")
        await message.reply(f"❌ Ошибка при отправке тестового {report_type} отчёта")


@commands_router.message(Command("create_test_data"))
@admin_only
async def handle_create_test_data(message: Message):
    """Создать тестовые данные согласно ТЗ для демонстрации Excel отчета."""
    try:
        db = get_db()
        
        # Создаем тестовых пригласителей с username согласно ТЗ
        vadim_id = db.upsert_inviter(name="Vadim", username="@vadim")
        anel_id = db.upsert_inviter(name="Anel", username="@anel") 
        petr_id = db.upsert_inviter(name="Petr", username="@petr")
        
        # События согласно ТЗ
        test_events = [
            # Alex подписался через Vadim, потом отписался
            {'event_type': 'subscribe', 'tg_user_id': 1234567, 'username': '@alex', 'name': 'Alex Ivanov', 'inviter_id': vadim_id, 'status': 'subscribed'},
            {'event_type': 'unsubscribe', 'tg_user_id': 1234567, 'username': '@alex', 'name': 'Alex Ivanov', 'inviter_id': vadim_id, 'status': 'left'},
            
            # Maria подписалась через Anel и осталась  
            {'event_type': 'subscribe', 'tg_user_id': 7654321, 'username': '@masha', 'name': 'Maria Petrova', 'inviter_id': anel_id, 'status': 'subscribed'},
            
            # Дополнительные события для статистики
            {'event_type': 'subscribe', 'tg_user_id': 1111111, 'username': '@ivan', 'name': 'Ivan Petrov', 'inviter_id': vadim_id, 'status': 'subscribed'},
            {'event_type': 'subscribe', 'tg_user_id': 2222222, 'username': '@elena', 'name': 'Elena Sidorova', 'inviter_id': petr_id, 'status': 'subscribed'},
            {'event_type': 'subscribe', 'tg_user_id': 3333333, 'username': '@dmitry', 'name': 'Dmitry Volkov', 'inviter_id': anel_id, 'status': 'subscribed'},
        ]
        
        # Добавляем события в журнал
        for event in test_events:
            db.insert_journal_event(
                event_type=event['event_type'],
                tg_user_id=event['tg_user_id'],
                username=event['username'],
                name=event['name'],
                inviter_id=event['inviter_id'],
                status=event['status']
            )
        
        await message.answer(
            f"✅ Тестовые данные созданы!\n\n"
            f"📊 Добавлено:\n"
            f"• 3 пригласителя с username (@vadim, @anel, @petr)\n" 
            f"• {len(test_events)} событий согласно ТЗ\n\n"
            f"Используйте /unified_report для генерации Excel отчета"
        )
        
    except Exception as e:
        logger.exception(f"Error creating test data: {e}")
        await message.answer(f"❌ Ошибка создания тестовых данных: {str(e)}")


@commands_router.message(Command("unified_report"))
@admin_only
async def handle_unified_report(message: Message):
    """Сгенерировать unified Excel отчет согласно ТЗ."""
    try:
        from reports.unified_report_manager import UnifiedReportManager
        db = get_db()
        unified_manager = UnifiedReportManager(db)
        
        # Обновляем данные в Excel файле
        stats = unified_manager.get_stats_summary()
        
        # Отправляем статистику
        stats_text = f"📊 **Статистика по пригласителям:**\n\n"
        
        if stats['inviters_data']:
            for inviter_data in stats['inviters_data']:
                stats_text += f"**{inviter_data['inviter_name']}:**\n"
                stats_text += f"  • Всего приглашено: {inviter_data['total_invited']}\n"
                stats_text += f"  • Подписаны сейчас: {inviter_data['currently_subscribed']}\n" 
                stats_text += f"  • Отписались: {inviter_data['unsubscribed']}\n"
                retention = 0
                if inviter_data['total_invited'] > 0:
                    retention = round((inviter_data['currently_subscribed'] / inviter_data['total_invited']) * 100)
                stats_text += f"  • % удержания: {retention}%\n\n"
        else:
            stats_text += "Нет данных для отображения."
        
        # Отправляем Excel файл
        excel_path = unified_manager.export_excel_file()
        
        # Создаем кнопку для скачивания
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📤 Скачать Excel-файл", callback_data="download_unified_excel")]
        ])
        
        await message.answer(stats_text, parse_mode="Markdown")
        
        # Отправляем файл
        if Path(excel_path).exists():
            document = FSInputFile(excel_path, filename="subscribers_report.xlsx")
            await message.answer_document(
                document=document,
                caption="📂 **subscribers_report.xlsx** - Unified отчет согласно ТЗ\n\n"
                       "**Содержит:**\n"
                       "• Лист **История** - полный журнал событий\n" 
                       "• Лист **Статистика** - сводка по пригласителям\n"
                       "• Ежедневные листы в формате ДД-ММ-ГГГГ",
                parse_mode="Markdown"
            )
        else:
            await message.answer("❌ Excel файл не найден")
            
    except Exception as e:
        logger.exception(f"Error generating unified report: {e}")
        await message.answer(f"❌ Ошибка генерации unified отчета: {str(e)}")


# ===== КОМАНДЫ УПРАВЛЕНИЯ ССЫЛКАМИ =====

@commands_router.message(Command("create_link"))
@admin_only
async def handle_create_link(message: Message):
    """Создать пригласительную ссылку для пользователя."""
    try:
        # Получаем имя пользователя из аргументов команды
        args = message.text.split()[1:] if message.text else []
        if not args:
            await message.answer(
                "❌ Укажите имя пользователя для создания ссылки.\n\n"
                "📝 **Использование:** `/create_link username`\n"
                "📝 **Пример:** `/create_link @vadim`\n\n"
                "ℹ️ Система автоматически использует TARGET_CHATS для каналов",
                parse_mode="Markdown"
            )
            return
        
        username = args[0].strip()
        if not username.startswith('@'):
            username = f'@{username}'
        
        # Импортируем адаптер
        from utils.adapter import get_invite_manager
        
        invite_manager = get_invite_manager()
        if not invite_manager:
            await message.answer("❌ Система управления ссылками недоступна")
            return
        
        # Убеждаемся что bot instance доступен
        if not invite_manager.bot:
            invite_manager.bot = message.bot
        
        # Создаем ссылку
        invite_link = await invite_manager.create_invite_for(username)
        
        await message.answer(
            f"✅ Ссылка создана для пользователя {username}!\n\n"
            f"🔗 **Ссылка:** `{invite_link}`\n\n"
            f"📊 Используйте /list_links чтобы посмотреть все ссылки",
            parse_mode="Markdown"
        )
        
    except Exception as e:
        logger.exception(f"Error creating invite link: {e}")
        await message.answer(f"❌ Ошибка при создании ссылки: {str(e)}")


@commands_router.message(Command("delete_link"))
@admin_only
async def handle_delete_link(message: Message):
    """Удалить пригласительную ссылку."""
    try:
        args = message.text.split()[1:] if message.text else []
        if not args:
            await message.answer(
                "❌ Укажите ID ссылки для удаления.\n\n"
                "📝 **Использование:** `/delete_link ID`\n"
                "📝 **Пример:** `/delete_link 1`\n\n"
                "📋 Используйте `/list_links` чтобы посмотреть все ID ссылок",
                parse_mode="Markdown"
            )
            return
        
        try:
            link_id = int(args[0])
        except ValueError:
            await message.answer("❌ ID должен быть числом")
            return
        
        from utils.adapter import get_invite_manager
        
        invite_manager = get_invite_manager()
        if not invite_manager:
            await message.answer("❌ Система управления ссылками недоступна")
            return
        
        # Получаем информацию о ссылке перед удалением
        link_info = invite_manager.get_invite_info(link_id)
        if not link_info:
            await message.answer("❌ Ссылка с таким ID не найдена")
            return
        
        # Удаляем ссылку
        success = invite_manager.delete_invite(link_id)
        
        if success:
            await message.answer(
                f"✅ Ссылка удалена!\n\n"
                f"👤 Пользователь: {link_info['name']}\n"
                f"🔗 Ссылка: `{link_info['invite_link']}`",
                parse_mode="Markdown"
            )
        else:
            await message.answer("❌ Не удалось удалить ссылку")
            
    except Exception as e:
        logger.exception(f"Error deleting invite link: {e}")
        await message.answer(f"❌ Ошибка при удалении ссылки: {str(e)}")


@commands_router.message(Command("list_links"))
@admin_only
async def handle_list_links(message: Message):
    """Показать все пригласительные ссылки."""
    try:
        from utils.adapter import get_invite_manager
        
        invite_manager = get_invite_manager()
        if not invite_manager:
            await message.answer("❌ Система управления ссылками недоступна")
            return
        
        # Получаем все ссылки
        invites = invite_manager.get_invites()
        
        if not invites:
            await message.answer(
                "📝 **Список пригласительных ссылок пуст**\n\n"
                "Используйте `/create_link username` для создания ссылки",
                parse_mode="Markdown"
            )
            return
        
        # Формируем сообщение
        response = "📋 **Все пригласительные ссылки:**\n\n"
        
        for invite in invites:
            response += f"**ID {invite['id']}** - {invite['name']}\n"
            response += f"🔗 `{invite['invite_link']}`\n"
            response += f"📊 Приглашено: {invite.get('total_invited', 0)}, "
            response += f"Активных: {invite.get('active_now', 0)}\n\n"
        
        response += "💡 **Команды управления ссылками:**\n"
        response += "• `/create_link @username` - создать ссылку для пользователя\n"
        response += "• `/delete_link ID` - удалить ссылку по ID\n" 
        response += "• `/list_links` - показать все ссылки с ID и статистикой"
        
        await message.answer(response, parse_mode="Markdown")
        
    except Exception as e:
        logger.exception(f"Error listing invite links: {e}")
        await message.answer(f"❌ Ошибка при получении списка ссылок: {str(e)}")


# Экспорт роутера для регистрации в основном приложении
__all__ = ['commands_router', 'configure_admin_ids', 'initialize_scheduler']