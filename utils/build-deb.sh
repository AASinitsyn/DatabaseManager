#!/bin/bash

set -e

echo "🔨 Сборка deb пакета для Database Manager..."

# Определяем корневую директорию проекта
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Переходим в корень проекта
cd "$PROJECT_ROOT"

# Проверяем наличие необходимых инструментов
if ! command -v dpkg-buildpackage &> /dev/null; then
    echo "❌ Ошибка: dpkg-buildpackage не найден"
    echo "Установите пакет: sudo apt-get install build-essential devscripts debhelper"
    exit 1
fi

if ! command -v go &> /dev/null; then
    echo "❌ Ошибка: Go не найден"
    echo "Установите Go: sudo apt-get install golang-go"
    exit 1
fi

# Очищаем предыдущие сборки
echo "🧹 Очистка предыдущих сборок..."
rm -rf debian/database-manager
rm -f ../database-manager_*.deb
rm -f ../database-manager_*.changes
rm -f ../database-manager_*.buildinfo
rm -f ../database-manager_*.dsc
rm -f ../database-manager_*.tar.gz

# Собираем deb пакет
echo "📦 Сборка deb пакета..."
dpkg-buildpackage -us -uc -b

echo ""
echo "✅ Deb пакет успешно собран!"
DEB_FILE=$(ls -1 "$PROJECT_ROOT/../database-manager_*.deb" 2>/dev/null | head -1)
if [ -n "$DEB_FILE" ]; then
    echo "📦 Файл: $DEB_FILE"
    echo ""
    echo "Для установки выполните:"
    echo "  sudo dpkg -i $DEB_FILE"
    echo ""
    echo "Для установки с разрешением зависимостей:"
    echo "  sudo apt-get install -f"
    echo ""
    echo "После установки для запуска сервиса:"
    echo "  sudo systemctl start database-manager"
    echo "  sudo systemctl enable database-manager"
else
    echo "❌ Ошибка: deb пакет не найден"
    exit 1
fi

