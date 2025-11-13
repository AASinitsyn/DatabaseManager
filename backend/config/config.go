package config

import (
	"database-manager/models"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var (
	ConnectionsFile = getConfigPath("connections.json")
	UsersFile       = getConfigPath("users.json")
	AppConfigFile   = getConfigPath("app.json")
)

func getConfigPath(filename string) string {
	// Проверяем, установлен ли пакет (путь /etc/database-manager существует)
	if _, err := os.Stat("/etc/database-manager"); err == nil {
		return filepath.Join("/etc/database-manager", filename)
	}
	// Иначе используем локальный путь для разработки
	return filepath.Join("config", filename)
}

type AppConfig struct {
	Host string `json:"host"`
	Port string `json:"port"`
}

var (
	mu          sync.RWMutex
	connections []models.Connection
	users       []models.User
	appConfig   *AppConfig
)

func LoadConnections() ([]models.Connection, error) {
	mu.Lock()
	defer mu.Unlock()

	data, err := os.ReadFile(ConnectionsFile)
	if err != nil {
		if os.IsNotExist(err) {
			connections = []models.Connection{}
			return []models.Connection{}, nil
		}
		return nil, fmt.Errorf("ошибка чтения файла подключений: %w", err)
	}

	if len(data) == 0 {
		connections = []models.Connection{}
		return []models.Connection{}, nil
	}

	var conns []models.Connection
	if err := json.Unmarshal(data, &conns); err != nil {
		return nil, fmt.Errorf("ошибка парсинга подключений: %w", err)
	}

	connections = conns
	return conns, nil
}

func SaveConnections(conns []models.Connection) error {
	mu.Lock()
	defer mu.Unlock()

	data, err := json.MarshalIndent(conns, "", "  ")
	if err != nil {
		return fmt.Errorf("ошибка сериализации подключений: %w", err)
	}

	if err := os.WriteFile(ConnectionsFile, data, 0644); err != nil {
		return fmt.Errorf("ошибка записи файла подключений: %w", err)
	}

	connections = conns
	return nil
}

func GetConnections() []models.Connection {
	mu.RLock()
	defer mu.RUnlock()
	return connections
}

func GetConnectionByID(id string) (*models.Connection, error) {
	mu.RLock()
	defer mu.RUnlock()

	for i := range connections {
		if connections[i].ID == id {
			return &connections[i], nil
		}
	}
	return nil, fmt.Errorf("подключение с ID %s не найдено", id)
}

func AddConnection(conn models.Connection) error {
	conns := GetConnections()
	conns = append(conns, conn)
	return SaveConnections(conns)
}

func UpdateConnection(id string, conn models.Connection) error {
	mu.Lock()
	defer mu.Unlock()
	
	for i := range connections {
		if connections[i].ID == id {
			// Сохраняем пароль из существующего подключения, если новый пустой
			if conn.Password == "" {
				conn.Password = connections[i].Password
			}
			conn.ID = id
			connections[i] = conn
			
			// Сохраняем в файл без повторной блокировки мьютекса
			data, err := json.MarshalIndent(connections, "", "  ")
			if err != nil {
				return fmt.Errorf("ошибка сериализации подключений: %w", err)
			}

			if err := os.WriteFile(ConnectionsFile, data, 0644); err != nil {
				return fmt.Errorf("ошибка записи файла подключений: %w", err)
			}
			
			return nil
		}
	}
	return fmt.Errorf("подключение с ID %s не найдено", id)
}

func DeleteConnection(id string) error {
	conns := GetConnections()
	for i := range conns {
		if conns[i].ID == id {
			conns = append(conns[:i], conns[i+1:]...)
			return SaveConnections(conns)
		}
	}
	return fmt.Errorf("подключение с ID %s не найдено", id)
}

func LoadUsers() ([]models.User, error) {
	mu.RLock()
	defer mu.RUnlock()

	data, err := os.ReadFile(UsersFile)
	if err != nil {
		if os.IsNotExist(err) {
			return []models.User{}, nil
		}
		return nil, fmt.Errorf("ошибка чтения файла пользователей: %w", err)
	}

	if len(data) == 0 {
		return []models.User{}, nil
	}

	var usrs []models.User
	if err := json.Unmarshal(data, &usrs); err != nil {
		return nil, fmt.Errorf("ошибка парсинга пользователей: %w", err)
	}

	users = usrs
	return usrs, nil
}

func SaveUsers(usrs []models.User) error {
	mu.Lock()
	defer mu.Unlock()

	data, err := json.MarshalIndent(usrs, "", "  ")
	if err != nil {
		return fmt.Errorf("ошибка сериализации пользователей: %w", err)
	}

	if err := os.WriteFile(UsersFile, data, 0644); err != nil {
		return fmt.Errorf("ошибка записи файла пользователей: %w", err)
	}

	users = usrs
	return nil
}

func GetUsers() []models.User {
	mu.RLock()
	defer mu.RUnlock()
	return users
}

func GetUserByUsername(username string) (*models.User, error) {
	mu.RLock()
	defer mu.RUnlock()

	for i := range users {
		if users[i].Username == username {
			return &users[i], nil
		}
	}
	return nil, fmt.Errorf("пользователь %s не найден", username)
}

func AddUser(user models.User) error {
	usrs := GetUsers()
	usrs = append(usrs, user)
	return SaveUsers(usrs)
}

func LoadAppConfig() (*AppConfig, error) {
	mu.Lock()
	defer mu.Unlock()

	data, err := os.ReadFile(AppConfigFile)
	if err != nil {
		if os.IsNotExist(err) {
			defaultConfig := &AppConfig{Host: "0.0.0.0", Port: "8081"}
			appConfig = defaultConfig
			if err := SaveAppConfig(defaultConfig); err != nil {
				return defaultConfig, nil
			}
			return defaultConfig, nil
		}
		return nil, fmt.Errorf("ошибка чтения файла конфигурации: %w", err)
	}

	if len(data) == 0 {
		defaultConfig := &AppConfig{Host: "0.0.0.0", Port: "8081"}
		appConfig = defaultConfig
		return defaultConfig, nil
	}

	var cfg AppConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("ошибка парсинга конфигурации: %w", err)
	}

	if cfg.Host == "" {
		cfg.Host = "0.0.0.0"
	}
	if cfg.Port == "" {
		cfg.Port = "8081"
	}

	appConfig = &cfg
	return &cfg, nil
}

func SaveAppConfig(cfg *AppConfig) error {
	mu.Lock()
	defer mu.Unlock()

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("ошибка сериализации конфигурации: %w", err)
	}

	if err := os.WriteFile(AppConfigFile, data, 0644); err != nil {
		return fmt.Errorf("ошибка записи файла конфигурации: %w", err)
	}

	appConfig = cfg
	return nil
}

func GetAppConfig() *AppConfig {
	mu.RLock()
	defer mu.RUnlock()
	if appConfig == nil {
		return &AppConfig{Host: "0.0.0.0", Port: "8081"}
	}
	return appConfig
}

