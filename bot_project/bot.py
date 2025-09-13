#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Основной файл Telegram бота для отслеживания активности в чатах.

Функции:
- Инициализация всех модулей системы
- Настройка обработчиков событий и команд
- Запуск планировщика отчётов
- Graceful shutdown при остановке
"""

import asyncio
import logging
import signal
import sys
import os
from pathlib import Path
from typing import Optional

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
from dotenv import load_dotenv

# Импорты модулей проекта
from db.db import get_db, init_database
from utils.logging_conf import setup_logging, get_logger
from utils.time_utils import get_almaty_now
from utils.adapter import init_adapters
from handlers.events import events_router
from handlers.commands import commands_router, initialize_admin_ids, initialize_scheduler
from handlers.ui import ui_router
from handlers.flows import flows_router
from reports.scheduler import ReportScheduler

# Загружаем переменные окружения
load_dotenv()

# Настраиваем логирование
setup_logging()
logger = get_logger(__name__)

# Глобальные переменные для управления состоянием
bot: Optional[Bot] = None
dp: Optional[Dispatcher] = None
scheduler: Optional[ReportScheduler] = None
web_app: Optional[web.Application] = None


async def create_bot() -> Bot:
    """Создать экземпляр бота с настройками."""
    bot_token = os.getenv('BOT_TOKEN')
    if not bot_token:
        logger.error("❌ BOT_TOKEN not found in environment variables")
        sys.exit(1)
    
    # Создаём бота с настройками по умолчанию
    bot = Bot(
        token=bot_token,
        default=DefaultBotProperties(
            parse_mode=ParseMode.MARKDOWN
        )
    )
    
    logger.info(f"🤖 Bot created with token: {bot_token[:8]}...")
    return bot


async def setup_dispatcher() -> Dispatcher:
    """Настроить диспетчер с роутерами и middleware."""
    dp = Dispatcher()
    
    # Регистрируем роутеры
    dp.include_router(ui_router)        # UI с inline клавиатурами (приоритет)
    dp.include_router(flows_router)     # FSM flows для диалогов
    dp.include_router(events_router)    # Обработка событий чата
    dp.include_router(commands_router)  # Старые команды (fallback)
    
    logger.info("📡 Dispatcher configured with routers")
    return dp


async def initialize_database():
    """Инициализировать базу данных."""
    try:
        # Инициализируем базу данных
        await init_database()
        
        # Проверяем подключение
        db = get_db()
        db.get_user_stats_summary()
        
        logger.info("💾 Database initialized successfully")
        
    except Exception as e:
        logger.exception(f"❌ Failed to initialize database: {e}")
        sys.exit(1)


async def initialize_components(bot: Bot):
    """Инициализировать все компоненты системы."""
    global scheduler
    
    try:
        # Инициализируем базу данных
        await initialize_database()
        
        # Инициализируем адаптеры для интеграции
        init_adapters()
        
        # Инициализируем админов из переменных окружения
        initialize_admin_ids()
        
        # Инициализируем планировщик
        initialize_scheduler(bot)
        
        # Настраиваем планировщик если есть конфигурация
        from handlers.commands import get_scheduler
        scheduler = get_scheduler()
        if scheduler:
            await configure_scheduler_from_env(scheduler)
        
        logger.info("🚀 All components initialized successfully")
        
    except Exception as e:
        logger.exception(f"❌ Failed to initialize components: {e}")
        sys.exit(1)


async def configure_scheduler_from_env(scheduler: ReportScheduler):
    """Настроить планировщик из переменных окружения."""
    try:
        # Получаем настройки из переменных окружения
        report_time_str = os.getenv('REPORT_TIME', '23:59')
        target_chats_str = os.getenv('TARGET_CHATS', '')
        scheduler_enabled = os.getenv('SCHEDULER_ENABLED', 'true').lower() == 'true'
        report_types_str = os.getenv('REPORT_TYPES', 'daily')
        
        # Парсим время
        try:
            from datetime import time as time_class
            hour, minute = map(int, report_time_str.split(':'))
            report_time = time_class(hour, minute)
        except ValueError:
            logger.warning(f"⚠️ Invalid REPORT_TIME format: {report_time_str}, using default 23:59")
            from datetime import time as time_class
            report_time = time_class(23, 59)
        
        # Парсим чаты
        target_chats = []
        if target_chats_str:
            try:
                target_chats = [int(chat_id.strip()) for chat_id in target_chats_str.split(',') if chat_id.strip()]
            except ValueError:
                logger.warning(f"⚠️ Invalid TARGET_CHATS format: {target_chats_str}")
        
        # Парсим типы отчётов
        report_types = [t.strip() for t in report_types_str.split(',') if t.strip()]
        
        # Настраиваем планировщик
        await scheduler.configure(
            report_time=report_time,
            target_chats=target_chats,
            enabled=scheduler_enabled,
            report_types=report_types
        )
        
        # Запускаем планировщик если включён и есть чаты
        if scheduler_enabled and target_chats:
            await scheduler.start()
            logger.info(f"⏰ Scheduler started: {report_time}, {len(target_chats)} chats")
        else:
            logger.info("⏸️ Scheduler configured but not started (disabled or no target chats)")
        
    except Exception as e:
        logger.exception(f"❌ Failed to configure scheduler: {e}")


async def setup_webhook_mode(bot: Bot, dp: Dispatcher) -> web.Application:
    """Настроить webhook режим (для продакшена)."""
    webhook_url = os.getenv('WEBHOOK_URL')
    webhook_path = os.getenv('WEBHOOK_PATH', '/webhook')
    webhook_secret = os.getenv('WEBHOOK_SECRET')
    port = int(os.getenv('PORT', 8080))
    
    if not webhook_url:
        raise ValueError("WEBHOOK_URL is required for webhook mode")
    
    # Настраиваем webhook
    await bot.set_webhook(
        url=f"{webhook_url}{webhook_path}",
        secret_token=webhook_secret,
        allowed_updates=dp.resolve_used_update_types()
    )
    
    # Создаём веб-приложение
    app = web.Application()
    
    # Настраиваем обработчик webhook
    webhook_requests_handler = SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
        secret_token=webhook_secret
    )
    webhook_requests_handler.register(app, path=webhook_path)
    
    # Добавляем health check endpoint
    async def health_check(request):
        return web.json_response({
            'status': 'ok',
            'timestamp': get_almaty_now().isoformat(),
            'bot_id': bot.id
        })
    
    app.router.add_get('/health', health_check)
    
    logger.info(f"🕸️ Webhook configured: {webhook_url}{webhook_path}")
    return app


async def run_polling_mode(bot: Bot, dp: Dispatcher):
    """Запустить бота в режиме polling (для разработки)."""
    try:
        # Удаляем webhook если был установлен
        await bot.delete_webhook(drop_pending_updates=True)
        
        logger.info("🔄 Starting bot in polling mode...")
        
        # Запускаем polling с явным указанием allowed_updates для chat_member событий
        allowed_updates = [
            "message", "callback_query", "inline_query", "chosen_inline_result",
            "chat_member", "my_chat_member"  # Добавляем обновления участников
        ]
        await dp.start_polling(
            bot,
            allowed_updates=allowed_updates,
            drop_pending_updates=True
        )
        
    except Exception as e:
        logger.exception(f"❌ Error in polling mode: {e}")
        raise


async def run_webhook_mode(bot: Bot, dp: Dispatcher):
    """Запустить бота в режиме webhook (для продакшена)."""
    global web_app
    
    try:
        # Настраиваем webhook
        web_app = await setup_webhook_mode(bot, dp)
        
        # Запускаем веб-сервер
        port = int(os.getenv('PORT', 8080))
        host = os.getenv('HOST', '0.0.0.0')
        
        logger.info(f"🕸️ Starting bot in webhook mode on {host}:{port}...")
        
        runner = web.AppRunner(web_app)
        await runner.setup()
        
        site = web.TCPSite(runner, host, port)
        await site.start()
        
        logger.info(f"✅ Webhook server started on {host}:{port}")
        
        # Ждём сигнала остановки
        stop_event = asyncio.Event()
        
        def signal_handler():
            logger.info("📡 Received shutdown signal")
            stop_event.set()
        
        # Регистрируем обработчики сигналов
        if sys.platform != 'win32':
            for sig in (signal.SIGTERM, signal.SIGINT):
                asyncio.get_event_loop().add_signal_handler(sig, signal_handler)
        
        await stop_event.wait()
        
        # Graceful shutdown
        await runner.cleanup()
        
    except Exception as e:
        logger.exception(f"❌ Error in webhook mode: {e}")
        raise


async def shutdown_components():
    """Graceful shutdown всех компонентов."""
    global bot, scheduler, web_app
    
    logger.info("🛑 Starting graceful shutdown...")
    
    try:
        # Останавливаем планировщик
        if scheduler:
            await scheduler.stop()
            logger.info("⏰ Scheduler stopped")
        
        # Закрываем веб-приложение
        if web_app:
            await web_app.cleanup()
            logger.info("🕸️ Web application cleaned up")
        
        # Закрываем сессию бота
        if bot:
            await bot.session.close()
            logger.info("🤖 Bot session closed")
        
        logger.info("✅ Graceful shutdown completed")
        
    except Exception as e:
        logger.exception(f"❌ Error during shutdown: {e}")


async def main():
    """Главная функция запуска бота."""
    global bot, dp
    
    try:
        logger.info("🚀 Starting Telegram Bot...")
        logger.info(f"📅 Start time: {get_almaty_now()}")
        
        # Создаём бота и диспетчер
        bot = await create_bot()
        dp = await setup_dispatcher()
        
        # Инициализируем все компоненты
        await initialize_components(bot)
        
        # Получаем информацию о боте
        bot_info = await bot.get_me()
        logger.info(f"🤖 Bot started: @{bot_info.username} ({bot_info.first_name})")
        
        # Определяем режим работы
        webhook_mode = os.getenv('WEBHOOK_MODE', 'false').lower() == 'true'
        
        if webhook_mode:
            await run_webhook_mode(bot, dp)
        else:
            await run_polling_mode(bot, dp)
            
    except KeyboardInterrupt:
        logger.info("⌨️ Received keyboard interrupt")
    except Exception as e:
        logger.exception(f"❌ Critical error in main: {e}")
        sys.exit(1)
    finally:
        await shutdown_components()


if __name__ == '__main__':
    try:
        # Проверяем Python версию
        if sys.version_info < (3, 8):
            logger.error("❌ Python 3.8+ is required")
            sys.exit(1)
        
        # Запускаем главную функцию
        asyncio.run(main())
        
    except KeyboardInterrupt:
        logger.info("👋 Bot stopped by user")
    except Exception as e:
        logger.exception(f"❌ Fatal error: {e}")
        sys.exit(1)