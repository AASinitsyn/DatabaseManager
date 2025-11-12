# Сборка deb пакета для Debian/Ubuntu

## Требования

Для сборки deb пакета необходимо установить следующие пакеты:

```bash
sudo apt-get update
sudo apt-get install build-essential devscripts debhelper golang-go
```

## Сборка пакета

Выполните скрипт сборки из корня проекта:

```bash
./utils/build-deb.sh
```

Или из директории utils:

```bash
cd utils
./build-deb.sh
```

Скрипт автоматически:
1. Проверит наличие необходимых инструментов
2. Соберет Go приложение
3. Создаст deb пакет

Результат будет в родительской директории: `../database-manager_*.deb`

## Установка пакета

```bash
sudo dpkg -i ../database-manager_*.deb
```

Если возникнут проблемы с зависимостями:

```bash
sudo apt-get install -f
```

## Управление сервисом

После установки сервис будет автоматически включен, но не запущен.

Запуск сервиса:
```bash
sudo systemctl start database-manager
```

Включение автозапуска:
```bash
sudo systemctl enable database-manager
```

Проверка статуса:
```bash
sudo systemctl status database-manager
```

Остановка сервиса:
```bash
sudo systemctl stop database-manager
```

Перезапуск:
```bash
sudo systemctl restart database-manager
```

## Расположение файлов

После установки файлы будут находиться в следующих местах:

- Исполняемый файл: `/usr/bin/database-manager`
- Конфигурация: `/etc/database-manager/`
- Данные: `/var/lib/database-manager/`
- Статические файлы: `/usr/share/database-manager/htmx/`
- Systemd unit: `/lib/systemd/system/database-manager.service`

## Настройка

По умолчанию сервис слушает порт 8080. Для изменения порта отредактируйте файл:

```bash
sudo nano /lib/systemd/system/database-manager.service
```

Измените строку:
```
Environment="PORT=8080"
```

Затем перезагрузите конфигурацию и перезапустите сервис:
```bash
sudo systemctl daemon-reload
sudo systemctl restart database-manager
```

## Доступ к веб-интерфейсу

После запуска сервиса веб-интерфейс будет доступен по адресу:

http://localhost:8080

По умолчанию создается пользователь `root` с паролем `1234567890`.

## Удаление пакета

```bash
sudo systemctl stop database-manager
sudo systemctl disable database-manager
sudo dpkg -r database-manager
```

Для полного удаления с конфигурационными файлами:

```bash
sudo dpkg -P database-manager
```

