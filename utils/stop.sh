#!/bin/bash

echo "🛑 Остановка сервера..."

# Ищем процесс database-manager
PID=$(ps aux | grep '[d]atabase-manager' | awk '{print $2}')

if [ -z "$PID" ]; then
    # Ищем процесс go run main.go
    PID=$(ps aux | grep '[g]o run main.go' | awk '{print $2}')
fi

if [ -z "$PID" ]; then
    echo "❌ Сервер не найден"
    exit 1
fi

echo "Найден процесс с PID: $PID"
kill $PID 2>/dev/null

sleep 1

# Проверяем, что процесс остановлен
if ps -p $PID > /dev/null 2>&1; then
    echo "⚠️  Процесс не остановился, принудительное завершение..."
    kill -9 $PID 2>/dev/null
fi

echo "✅ Сервер остановлен"

