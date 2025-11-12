# Database Manager Backend

Backend сервер на Go для управления базами данных через веб-интерфейс.

## Возможности

- Поддержка 6 типов БД: PostgreSQL, MongoDB, Elasticsearch, ClickHouse, Cassandra, Aerospike
- JWT аутентификация
- Хранение конфигурации подключений в JSON файлах
- Восстановление активных подключений при перезапуске
- REST API для управления подключениями, выполнения запросов, создания БД, таблиц и пользователей

## Установка и запуск

1. Установите зависимости:
```bash
cd backend
go mod download
```

2. Запустите сервер:
```bash
go run main.go
```

Сервер запустится на порту 8080 (или на порту, указанном в переменной окружения PORT).

3. Откройте браузер по адресу `http://localhost:8080`

## Конфигурация

Конфигурация хранится в файлах:
- `config/connections.json` - подключения к базам данных
- `config/users.json` - пользователи системы

При первом запуске эти файлы будут созданы автоматически.

## Переменные окружения

- `PORT` - порт для запуска сервера (по умолчанию 8080)
- `JWT_SECRET` - секретный ключ для JWT токенов (по умолчанию используется встроенный ключ)

## API Эндпоинты

### Аутентификация
- `POST /api/auth/register` - Регистрация
- `POST /api/auth/login` - Вход

### Подключения
- `GET /api/connections` - Список подключений
- `POST /api/connections` - Создание подключения
- `GET /api/connections/:id` - Получение подключения
- `PUT /api/connections/:id` - Обновление подключения
- `DELETE /api/connections/:id` - Удаление подключения
- `POST /api/connections/:id/connect` - Подключение к БД
- `POST /api/connections/:id/disconnect` - Отключение от БД
- `GET /api/connections/:id/status` - Статус подключения

### Работа с БД
- `POST /api/query` - Выполнение запроса
- `POST /api/databases` - Создание базы данных
- `POST /api/tables` - Создание таблицы
- `POST /api/users` - Создание пользователя БД

Все эндпоинты кроме `/api/auth/*` требуют JWT токен в заголовке `Authorization: Bearer <token>`.

## Структура проекта

```
backend/
├── main.go              # Точка входа
├── config/              # Конфигурация
│   ├── config.go
│   ├── connections.json
│   └── users.json
├── models/              # Модели данных
│   ├── connection.go
│   ├── user.go
│   └── request.go
├── handlers/            # HTTP handlers
│   ├── auth.go
│   ├── connections.go
│   ├── query.go
│   ├── databases.go
│   ├── tables.go
│   └── users.go
├── database/            # Драйверы БД
│   ├── driver.go
│   ├── manager.go
│   ├── postgres.go
│   ├── mongodb.go
│   ├── elasticsearch.go
│   ├── clickhouse.go
│   ├── cassandra.go
│   └── aerospike.go
├── middleware/          # Middleware
│   ├── auth.go
│   └── cors.go
└── utils/              # Утилиты
    └── jwt.go
```

