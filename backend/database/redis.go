package database

import (
	"context"
	"crypto/tls"
	"database-manager/models"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisDriver struct {
	client *redis.Client
	conn   models.Connection
}

func NewRedisDriver() *RedisDriver {
	return &RedisDriver{}
}

func (d *RedisDriver) Connect(ctx context.Context, conn models.Connection) error {
	dbNum := 0
	if conn.Database != "" {
		if num, err := strconv.Atoi(conn.Database); err == nil {
			dbNum = num
		}
	}

	opts := &redis.Options{
		Addr:     fmt.Sprintf("%s:%s", conn.Host, conn.Port),
		Password: conn.Password,
		DB:       dbNum,
	}

	if conn.SSL {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: false,
		}
	}

	client := redis.NewClient(opts)

	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ошибка подключения к Redis: %w", err)
	}

	d.client = client
	d.conn = conn
	return nil
}

func (d *RedisDriver) Disconnect(ctx context.Context) error {
	if d.client != nil {
		return d.client.Close()
	}
	return nil
}

func (d *RedisDriver) IsConnected(ctx context.Context) bool {
	if d.client == nil {
		return false
	}
	return d.client.Ping(ctx).Err() == nil
}

func (d *RedisDriver) Ping(ctx context.Context) error {
	if d.client == nil {
		return fmt.Errorf("подключение не установлено")
	}
	return d.client.Ping(ctx).Err()
}

func (d *RedisDriver) ExecuteQuery(ctx context.Context, query string) (*models.QueryResponse, error) {
	if d.client == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	startTime := time.Now()

	query = strings.TrimSpace(query)
	parts := strings.Fields(query)
	if len(parts) == 0 {
		return &models.QueryResponse{
			Error: "пустой запрос",
		}, nil
	}

	command := strings.ToUpper(parts[0])
	args := make([]interface{}, len(parts)-1)
	for i, part := range parts[1:] {
		args[i] = part
	}

	var result interface{}
	var err error

	switch command {
	case "GET", "HGET", "LINDEX", "SMEMBERS", "ZRANGE":
		if len(args) == 0 {
			return &models.QueryResponse{Error: fmt.Sprintf("команда %s требует аргументы", command)}, nil
		}
		result, err = d.executeReadCommand(ctx, command, args)
	case "KEYS", "SCAN":
		result, err = d.executeKeysCommand(ctx, command, args)
	case "INFO":
		result, err = d.executeInfoCommand(ctx)
	case "DBSIZE":
		result, err = d.client.DBSize(ctx).Result()
	default:
		args := make([]interface{}, len(parts))
		for i, part := range parts {
			args[i] = part
		}
		result, err = d.client.Do(ctx, args...).Result()
	}

	if err != nil {
		return &models.QueryResponse{
			Error: err.Error(),
		}, nil
	}

	columns := []string{"key", "value", "type"}
	rowsData := make([]map[string]interface{}, 0)

	if result != nil {
		switch v := result.(type) {
		case string:
			rowsData = append(rowsData, map[string]interface{}{
				"key":   "result",
				"value": v,
				"type":  "string",
			})
		case int64:
			rowsData = append(rowsData, map[string]interface{}{
				"key":   "result",
				"value": v,
				"type":  "integer",
			})
		case []interface{}:
			for _, item := range v {
				if str, ok := item.(string); ok {
					rowsData = append(rowsData, map[string]interface{}{
						"key":   str,
						"value": "",
						"type":  "string",
					})
				}
			}
		case []string:
			for _, item := range v {
				rowsData = append(rowsData, map[string]interface{}{
					"key":   item,
					"value": "",
					"type":  "string",
				})
			}
		case map[string]interface{}:
			for key, val := range v {
				rowsData = append(rowsData, map[string]interface{}{
					"key":   key,
					"value": fmt.Sprintf("%v", val),
					"type":  fmt.Sprintf("%T", val),
				})
			}
		default:
			rowsData = append(rowsData, map[string]interface{}{
				"key":   "result",
				"value": fmt.Sprintf("%v", result),
				"type":  fmt.Sprintf("%T", result),
			})
		}
	}

	executionTime := time.Since(startTime).Milliseconds()

	return &models.QueryResponse{
		Columns:       columns,
		Rows:          rowsData,
		RowCount:      len(rowsData),
		ExecutionTime: executionTime,
	}, nil
}

func (d *RedisDriver) executeReadCommand(ctx context.Context, command string, args []interface{}) (interface{}, error) {
	switch command {
	case "GET":
		return d.client.Get(ctx, args[0].(string)).Result()
	case "HGET":
		if len(args) < 2 {
			return nil, fmt.Errorf("HGET требует key и field")
		}
		return d.client.HGet(ctx, args[0].(string), args[1].(string)).Result()
	case "LINDEX":
		if len(args) < 2 {
			return nil, fmt.Errorf("LINDEX требует key и index")
		}
		index, _ := strconv.Atoi(args[1].(string))
		return d.client.LIndex(ctx, args[0].(string), int64(index)).Result()
	case "SMEMBERS":
		return d.client.SMembers(ctx, args[0].(string)).Result()
	case "ZRANGE":
		if len(args) < 3 {
			return nil, fmt.Errorf("ZRANGE требует key, start и stop")
		}
		start, _ := strconv.Atoi(args[1].(string))
		stop, _ := strconv.Atoi(args[2].(string))
		return d.client.ZRange(ctx, args[0].(string), int64(start), int64(stop)).Result()
	}
	return nil, fmt.Errorf("неподдерживаемая команда: %s", command)
}

func (d *RedisDriver) executeKeysCommand(ctx context.Context, command string, args []interface{}) (interface{}, error) {
	if command == "KEYS" && len(args) > 0 {
		return d.client.Keys(ctx, args[0].(string)).Result()
	}
	return d.client.Keys(ctx, "*").Result()
}

func (d *RedisDriver) executeInfoCommand(ctx context.Context) (interface{}, error) {
	info, err := d.client.Info(ctx).Result()
	if err != nil {
		return nil, err
	}
	result := make(map[string]interface{})
	lines := strings.Split(info, "\n")
	for _, line := range lines {
		if strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				result[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
	}
	return result, nil
}

func (d *RedisDriver) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	dbNum, err := strconv.Atoi(name)
	if err != nil {
		return fmt.Errorf("номер базы данных должен быть числом от 0 до 15")
	}
	if dbNum < 0 || dbNum > 15 {
		return fmt.Errorf("номер базы данных должен быть от 0 до 15")
	}
	return nil
}

func (d *RedisDriver) ListDatabases(ctx context.Context) ([]models.DatabaseInfo, error) {
	if d.client == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	databases := make([]models.DatabaseInfo, 0)
	for i := 0; i < 16; i++ {
		client := redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%s", d.conn.Host, d.conn.Port),
			Password: d.conn.Password,
			DB:       i,
		})
		defer client.Close()

		size, err := client.DBSize(ctx).Result()
		if err == nil {
			databases = append(databases, models.DatabaseInfo{
				Name: fmt.Sprintf("db%d", i),
				Size: fmt.Sprintf("%d ключей", size),
			})
		}
	}

	return databases, nil
}

func (d *RedisDriver) UpdateDatabase(ctx context.Context, oldName, newName string, options map[string]interface{}) error {
	return fmt.Errorf("Redis не поддерживает переименование баз данных")
}

func (d *RedisDriver) DeleteDatabase(ctx context.Context, name string) error {
	if d.client == nil {
		return fmt.Errorf("подключение не установлено")
	}

	dbNum, err := strconv.Atoi(strings.TrimPrefix(name, "db"))
	if err != nil {
		return fmt.Errorf("неверный формат имени базы данных")
	}

	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", d.conn.Host, d.conn.Port),
		Password: d.conn.Password,
		DB:       dbNum,
	})
	defer client.Close()

	return client.FlushDB(ctx).Err()
}

func (d *RedisDriver) CreateTable(ctx context.Context, name string, columns []models.TableColumn) error {
	return fmt.Errorf("Redis не поддерживает создание таблиц")
}

func (d *RedisDriver) ListTables(ctx context.Context) ([]models.TableInfo, error) {
	if d.client == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	keys, err := d.client.Keys(ctx, "*").Result()
	if err != nil {
		return nil, err
	}

	tables := make([]models.TableInfo, 0)
	keyTypes := make(map[string]string)

	for _, key := range keys {
		keyType, err := d.client.Type(ctx, key).Result()
		if err == nil {
			keyTypes[key] = keyType
		}
	}

	for key, keyType := range keyTypes {
		tables = append(tables, models.TableInfo{
			Name: key,
			Size: keyType,
		})
	}

	return tables, nil
}

func (d *RedisDriver) DeleteTable(ctx context.Context, name string) error {
	if d.client == nil {
		return fmt.Errorf("подключение не установлено")
	}
	return d.client.Del(ctx, name).Err()
}

func (d *RedisDriver) UpdateTable(ctx context.Context, oldName, newName string, columns []models.TableColumn) error {
	return fmt.Errorf("Redis не поддерживает переименование ключей напрямую. Используйте команду RENAME")
}

func (d *RedisDriver) CreateUser(ctx context.Context, username, password, database string, permissions []string) error {
	return fmt.Errorf("Redis не поддерживает управление пользователями через этот интерфейс")
}

func (d *RedisDriver) ListUsers(ctx context.Context) ([]models.UserInfo, error) {
	return nil, fmt.Errorf("Redis не поддерживает управление пользователями через этот интерфейс")
}

func (d *RedisDriver) UpdateUser(ctx context.Context, username, password string, permissions []string) error {
	return fmt.Errorf("Redis не поддерживает управление пользователями через этот интерфейс")
}

func (d *RedisDriver) DeleteUser(ctx context.Context, username string) error {
	return fmt.Errorf("Redis не поддерживает управление пользователями через этот интерфейс")
}

