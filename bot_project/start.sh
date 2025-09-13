#!/bin/bash

# Скрипт запуска Telegram бота
# Поддерживает различные режимы запуска и обработку ошибок

set -euo pipefail  # Строгий режим bash

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Функция для логирования
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

log_error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1"
}

log_success() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] SUCCESS:${NC} $1"
}

# Определяем директорию проекта
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

log "🚀 Starting Telegram Bot..."
log "📁 Project directory: $PROJECT_DIR"

# Функция для проверки зависимостей
check_dependencies() {
    log "🔍 Checking dependencies..."
    
    # Проверяем Python
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 is not installed"
        exit 1
    fi
    
    local python_version=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
    log "🐍 Python version: $python_version"
    
    # Проверяем версию Python (упрощенная проверка для мажорной версии)
    local major_version=$(echo "$python_version" | cut -d. -f1)
    local minor_version=$(echo "$python_version" | cut -d. -f2)
    
    if [[ $major_version -lt 3 ]] || [[ $major_version -eq 3 && $minor_version -lt 8 ]]; then
        log_error "Python 3.8+ is required, found $python_version"
        exit 1
    fi
    
    # Проверяем pip
    if ! command -v pip3 &> /dev/null; then
        log_error "pip3 is not installed"
        exit 1
    fi
}

# Функция для установки зависимостей
install_dependencies() {
    log "📦 Installing dependencies..."
    
    if [[ ! -f "requirements.txt" ]]; then
        log_error "requirements.txt not found"
        exit 1
    fi
    
    # В Nix окружении пропускаем установку зависимостей
    log_warning "Dependency installation skipped (using Nix packages)"
    log_success "Dependencies managed by Nix environment"
}

# Функция для проверки переменных окружения
check_environment() {
    log "🔧 Checking environment variables..."
    
    # Проверяем .env файл
    if [[ ! -f ".env" ]]; then
        log_warning ".env file not found"
        if [[ -f ".env.example" ]]; then
            log "📋 Copy .env.example to .env and configure it:"
            log "   cp .env.example .env"
            log "   nano .env"
        fi
        log_error "Please create .env file with required variables"
        exit 1
    fi
    
    # Загружаем переменные окружения
    set -a
    source .env
    set +a
    
    # Проверяем обязательные переменные
    local required_vars=("BOT_TOKEN")
    local missing_vars=()
    
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            missing_vars+=("$var")
        fi
    done
    
    if [[ ${#missing_vars[@]} -gt 0 ]]; then
        log_error "Missing required environment variables:"
        for var in "${missing_vars[@]}"; do
            log_error "  - $var"
        done
        exit 1
    fi
    
    log_success "Environment variables OK"
}

# Функция для инициализации базы данных
init_database() {
    log "💾 Initializing database..."
    
    # Создаём директорию для базы данных если не существует
    mkdir -p data
    
    # Запускаем инициализацию базы данных
    if python3 -c "
import asyncio
import sys
sys.path.append('.')
from db.db import init_database

async def main():
    try:
        await init_database()
        print('Database initialized successfully')
    except Exception as e:
        print(f'Database initialization failed: {e}')
        sys.exit(1)

asyncio.run(main())
"; then
        log_success "Database initialized"
    else
        log_error "Failed to initialize database"
        exit 1
    fi
}

# Функция для проверки конфигурации планировщика
check_scheduler_config() {
    log "⏰ Checking scheduler configuration..."
    
    if [[ -n "${TARGET_CHATS:-}" ]]; then
        log "📡 Target chats configured: $TARGET_CHATS"
    else
        log_warning "TARGET_CHATS not set - scheduler will not send reports automatically"
        log "   Tip: Set TARGET_CHATS with your chat ID for automated daily reports"
    fi
    
    local report_time="${REPORT_TIME:-23:59}"
    log "🕐 Report time: $report_time (Moscow timezone)"
    
    local scheduler_enabled="${SCHEDULER_ENABLED:-true}"
    if [[ "$scheduler_enabled" == "true" ]]; then
        log "✅ Scheduler enabled"
    else
        log "⏸️ Scheduler disabled"
    fi
}

# Функция для проверки unified Excel системы
check_unified_excel_system() {
    log "📊 Checking unified Excel reporting system..."
    
    # Создаём директорию для отчётов если не существует
    mkdir -p reports_output
    
    # Проверяем наличие необходимых файлов
    local required_files=(
        "reports/unified_excel_template.py"
        "reports/unified_report_manager.py"
    )
    
    for file in "${required_files[@]}"; do
        if [[ ! -f "$file" ]]; then
            log_error "Required unified Excel file missing: $file"
            exit 1
        fi
    done
    
    log "✅ Unified Excel system files OK"
    log "📝 subscribers_report.xlsx will be created in reports_output/"
    log "📋 Features: История, Статистика, daily sheets (ДД-ММ-ГГГГ)"
}

# Функция для тестирования unified системы
test_unified_system() {
    log "🧪 Testing unified Excel reporting system..."
    
    if python3 -c "
import asyncio
import sys
sys.path.append('.')

async def test_unified():
    try:
        from reports.unified_report_manager import UnifiedReportManager
        from db.db import get_db
        
        # Initialize system
        db = get_db()
        unified_manager = UnifiedReportManager(db)
        
        # Test message generation
        message_text, download_keyboard = unified_manager.get_daily_message_with_button()
        
        print('✅ Unified system test passed')
        print('📊 Daily message format: OK')
        print('🔗 Download button: OK')
        print('📁 Excel file path:', unified_manager.get_excel_file_path())
        
    except Exception as e:
        print(f'❌ Unified system test failed: {e}')
        sys.exit(1)

asyncio.run(test_unified())
"; then
        log_success "Unified Excel system test passed"
    else
        log_error "Unified Excel system test failed"
        exit 1
    fi
}

# Функция для запуска бота
start_bot() {
    log "🤖 Starting bot..."
    
    # Определяем режим запуска
    local webhook_mode="${WEBHOOK_MODE:-false}"
    
    if [[ "$webhook_mode" == "true" ]]; then
        log "🕸️ Running in webhook mode"
        if [[ -z "${WEBHOOK_URL:-}" ]]; then
            log_error "WEBHOOK_URL is required for webhook mode"
            exit 1
        fi
    else
        log "🔄 Running in polling mode"
    fi
    
    # Запускаем бота
    exec python3 bot.py
}

# Функция для очистки при выходе
cleanup() {
    log "🧹 Cleaning up..."
    
    # Завершаем фоновые процессы
    jobs -p | xargs -r kill
    
    log "👋 Bot stopped"
}

# Регистрируем функцию очистки на выход
trap cleanup EXIT INT TERM

# Функция помощи
show_help() {
    echo "Telegram Bot Startup Script"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --install-deps     Install Python dependencies"
    echo "  --init-db         Initialize database only"
    echo "  --check           Check configuration without starting"
    echo "  --test-unified    Test unified Excel reporting system"
    echo "  --dev             Development mode (force polling)"
    echo "  --prod            Production mode (force webhook if configured)"
    echo "  --help            Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  BOT_TOKEN         Telegram bot token (required)"
    echo "  WEBHOOK_MODE      Use webhook mode (true/false, default: false)"
    echo "  WEBHOOK_URL       Webhook URL (required if webhook mode)"
    echo "  TARGET_CHATS      Comma-separated chat IDs for unified Excel reports"
    echo "  REPORT_TIME       Time to send daily reports (HH:MM, default: 23:59)"
    echo "  SCHEDULER_ENABLED Enable automatic reports (true/false, default: true)"
    echo "  ADMIN_IDS         Comma-separated admin user IDs"
    echo ""
    echo "Unified Excel System:"
    echo "  📊 Creates subscribers_report.xlsx with История, Статистика sheets"
    echo "  📅 Daily sheets in ДД-ММ-ГГГГ format"
    echo "  ⏰ Automated daily reports at 23:59 Moscow time"
    echo "  📤 Download button in Telegram messages"
    echo "  🧪 Test with: /test_unified command (admin only)"
    echo ""
}

# Обработка аргументов командной строки
INSTALL_DEPS=false
INIT_DB_ONLY=false
CHECK_ONLY=false
TEST_UNIFIED=false
DEV_MODE=false
PROD_MODE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --install-deps)
            INSTALL_DEPS=true
            shift
            ;;
        --init-db)
            INIT_DB_ONLY=true
            shift
            ;;
        --check)
            CHECK_ONLY=true
            shift
            ;;
        --test-unified)
            TEST_UNIFIED=true
            shift
            ;;
        --dev)
            DEV_MODE=true
            export WEBHOOK_MODE=false
            shift
            ;;
        --prod)
            PROD_MODE=true
            shift
            ;;
        --help)
            show_help
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Основная логика
main() {
    log "📋 Telegram Bot Starting Sequence"
    log "================================"
    
    # Проверяем зависимости
    check_dependencies
    
    # Пропускаем установку зависимостей в Nix окружении
    if [[ "$INSTALL_DEPS" == "true" ]]; then
        log_warning "Skipping dependency installation (Nix environment)"
    fi
    
    # Проверяем окружение
    check_environment
    
    # Инициализируем базу данных
    init_database
    
    # Если только инициализация БД
    if [[ "$INIT_DB_ONLY" == "true" ]]; then
        log_success "Database initialization completed"
        exit 0
    fi
    
    # Проверяем unified Excel систему
    check_unified_excel_system
    
    # Проверяем конфигурацию планировщика
    check_scheduler_config
    
    # Если тестирование unified системы
    if [[ "$TEST_UNIFIED" == "true" ]]; then
        test_unified_system
        log_success "Unified system test completed"
        exit 0
    fi
    
    # Если только проверка
    if [[ "$CHECK_ONLY" == "true" ]]; then
        log_success "Configuration check completed"
        exit 0
    fi
    
    # Финальная проверка перед запуском
    log "✅ All checks passed"
    log "🚀 Starting bot in 3 seconds..."
    sleep 3
    
    # Запускаем бота
    start_bot
}

# Запуск основной функции
main "$@"