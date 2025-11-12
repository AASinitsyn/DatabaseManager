#!/bin/bash

set -e

# Определяем корневую директорию проекта
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Переходим в корень проекта
cd "$PROJECT_ROOT"

echo "🔨 Сборка backend..."
cd backend

# Скачиваем зависимости
echo "📦 Загрузка зависимостей..."
go mod download

# Собираем приложение
echo "⚙️  Компиляция..."
go build -o database-manager main.go

echo "✅ Backend собран успешно!"

# Запускаем сервер в фоне
echo "🚀 Запуск сервера..."
./database-manager &
SERVER_PID=$!

# Ждем немного, чтобы сервер успел запуститься
echo "⏳ Ожидание запуска сервера..."
sleep 2

# Проверяем, что сервер запустился
if ps -p $SERVER_PID > /dev/null; then
    echo "✅ Сервер запущен (PID: $SERVER_PID)"
    echo "🌐 Открытие браузера..."
    
    # Определяем команду для открытия браузера в зависимости от ОС
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        open http://localhost:8080
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux
        xdg-open http://localhost:8080 2>/dev/null || sensible-browser http://localhost:8080 2>/dev/null || echo "Откройте браузер вручную: http://localhost:8080"
    elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
        # Windows (Git Bash)
        start http://localhost:8080
    else
        echo "Откройте браузер вручную: http://localhost:8080"
    fi
    
    echo ""
    echo "📝 Для остановки сервера выполните: kill $SERVER_PID"
    echo "   Или нажмите Ctrl+C"
    
    # Ждем завершения (Ctrl+C)
    wait $SERVER_PID
else
    echo "❌ Ошибка запуска сервера"
    exit 1
fi

