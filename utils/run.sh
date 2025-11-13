#!/bin/sh

set -e

# Определяем корневую директорию проекта
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Переходим в корень проекта
cd "$PROJECT_ROOT"

# Проверяем существование директории backend
if [ ! -d "backend" ]; then
    echo "❌ Ошибка: директория backend не найдена в $PROJECT_ROOT"
    exit 1
fi

echo "🔨 Запуск backend в режиме разработки..."
cd backend

# Скачиваем зависимости если нужно
if [ ! -f "go.sum" ]; then
    echo "📦 Загрузка зависимостей..."
    go mod download
fi

# Запускаем сервер через go run
echo "🚀 Запуск сервера (go run)..."
go run main.go &
SERVER_PID=$!

# Ждем немного, чтобы сервер успел запуститься
echo "⏳ Ожидание запуска сервера..."
sleep 2

# Проверяем, что сервер запустился
if kill -0 $SERVER_PID 2>/dev/null; then
    echo "✅ Сервер запущен (PID: $SERVER_PID)"
    
    # Определяем порт из конфига или переменной окружения
    PORT=${PORT:-8081}
    if [ -f "config/app.json" ]; then
        CONFIG_PORT=$(grep -o '"port"[[:space:]]*:[[:space:]]*"[^"]*"' config/app.json | cut -d'"' -f4)
        if [ -n "$CONFIG_PORT" ]; then
            PORT=$CONFIG_PORT
        fi
    fi
    
    # Проверяем, не запущены ли мы в Alpine Linux
    if [ -f /etc/alpine-release ]; then
        echo "ℹ️  Alpine Linux обнаружен, браузер не будет открыт автоматически"
    else
        echo "🌐 Открытие браузера..."
        
        # Определяем команду для открытия браузера в зависимости от ОС
        if [ "$(uname)" = "Darwin" ]; then
            # macOS
            open http://localhost:$PORT
        elif [ "$(uname)" = "Linux" ]; then
            # Linux
            xdg-open http://localhost:$PORT 2>/dev/null || sensible-browser http://localhost:$PORT 2>/dev/null || echo "Откройте браузер вручную: http://localhost:$PORT"
        else
            echo "Откройте браузер вручную: http://localhost:$PORT"
        fi
    fi
    
    echo ""
    echo "📝 Для остановки сервера нажмите Ctrl+C"
    echo ""
    
    # Ждем завершения (Ctrl+C)
    trap "kill $SERVER_PID 2>/dev/null" EXIT
    wait $SERVER_PID
else
    echo "❌ Ошибка запуска сервера"
    exit 1
fi

