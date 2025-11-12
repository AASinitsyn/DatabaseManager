package handlers

import (
	"context"
	"database-manager/config"
	"database-manager/database"
	"database-manager/models"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

var connManager *database.ConnectionManager

func InitConnectionManager(manager *database.ConnectionManager) {
	connManager = manager
}

func GetConnectionsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	connections := config.GetConnections()
	// Создаем копию массива, чтобы не изменять оригинал
	result := make([]models.Connection, len(connections))
	copy(result, connections)
	
	for i := range result {
		result[i].Password = ""
		result[i].Connected = connManager.IsConnected(result[i].ID)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func GetConnectionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	path := r.URL.Path
	id := strings.TrimPrefix(path, "/api/connections/")
	conn, err := config.GetConnectionByID(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Проверяем, есть ли параметр для редактирования (возвращаем пароль)
	// Если параметра нет, пароль скрываем для безопасности
	includePassword := r.URL.Query().Get("edit") == "true"
	if !includePassword {
		conn.Password = ""
	}
	
	conn.Connected = connManager.IsConnected(id)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(conn)
}

func CreateConnectionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	var conn models.Connection
	if err := json.NewDecoder(r.Body).Decode(&conn); err != nil {
		http.Error(w, "Ошибка парсинга запроса", http.StatusBadRequest)
		return
	}

	// Проверяем, что пароль передан
	if conn.Password == "" {
		http.Error(w, "Пароль обязателен для создания подключения", http.StatusBadRequest)
		return
	}

	conn.ID = uuid.New().String()
	conn.Connected = false
	conn.CreatedAt = time.Now()
	conn.UpdatedAt = time.Now()

	// Сохраняем пароль для использования
	savedPassword := conn.Password

	// Пробуем подключиться для проверки параметров
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	
	if err := connManager.Connect(ctx, conn); err != nil {
		// Сохраняем подключение даже если не удалось подключиться
		// но возвращаем предупреждение с детальной информацией
		conn.Password = savedPassword
		if saveErr := config.AddConnection(conn); saveErr != nil {
			http.Error(w, saveErr.Error(), http.StatusInternalServerError)
			return
		}
		conn.Password = ""
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"connection": conn,
			"warning":    fmt.Sprintf("Не удалось подключиться: %v", err),
			"error":      err.Error(),
		})
		return
	}

	// Если подключение успешно, отключаемся (сохраняем только конфигурацию)
	connManager.Disconnect(conn.ID)
	conn.Connected = false
	conn.Password = savedPassword

	if err := config.AddConnection(conn); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	conn.Password = ""
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(conn)
}

func UpdateConnectionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	path := r.URL.Path
	id := strings.TrimPrefix(path, "/api/connections/")
	
	// Получаем существующее подключение для сохранения пароля, если новый не указан
	existingConn, err := config.GetConnectionByID(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	var conn models.Connection
	if err := json.NewDecoder(r.Body).Decode(&conn); err != nil {
		http.Error(w, "Ошибка парсинга запроса", http.StatusBadRequest)
		return
	}

	conn.ID = id
	conn.CreatedAt = existingConn.CreatedAt
	conn.UpdatedAt = time.Now()
	
	// Сохраняем значения из существующего подключения, если новые не указаны
	// Используем значения из запроса, если они переданы, иначе берем из существующего
	if conn.Name == "" {
		conn.Name = existingConn.Name
	}
	if conn.Type == "" {
		conn.Type = existingConn.Type
	}
	if conn.Host == "" {
		conn.Host = existingConn.Host
	}
	// Порт - если пустой, используем существующий
	if conn.Port == "" {
		conn.Port = existingConn.Port
	}
	if conn.Database == "" {
		conn.Database = existingConn.Database
	}
	if conn.Username == "" {
		conn.Username = existingConn.Username
	}
	// Если пароль не указан, используем существующий
	if conn.Password == "" {
		conn.Password = existingConn.Password
	}
	// SSL сохраняем как есть из запроса (false тоже валидное значение)

	// Если подключение активно, отключаем его перед обновлением
	if connManager.IsConnected(id) {
		connManager.Disconnect(id)
		conn.Connected = false
	}

	// Пробуем подключиться для проверки новых параметров
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	
	connectErr := connManager.Connect(ctx, conn)
	if connectErr != nil {
		// Сохраняем подключение даже если не удалось подключиться
		if err := config.UpdateConnection(id, conn); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		conn.Password = ""
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"connection": conn,
			"warning":    fmt.Sprintf("Не удалось подключиться: %v", connectErr),
			"error":      connectErr.Error(),
		})
		return
	}

	// Если подключение успешно, отключаемся (сохраняем только конфигурацию)
	connManager.Disconnect(id)
	conn.Connected = false

	if err := config.UpdateConnection(id, conn); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	conn.Password = ""
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(conn)
}

func DeleteConnectionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	path := r.URL.Path
	id := strings.TrimPrefix(path, "/api/connections/")
	
	if connManager.IsConnected(id) {
		connManager.Disconnect(id)
	}

	if err := config.DeleteConnection(id); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func ConnectHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	path := r.URL.Path
	id := strings.TrimPrefix(path, "/api/connections/")
	id = strings.TrimSuffix(id, "/connect")

	conn, err := config.GetConnectionByID(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Создаем копию подключения с паролем для безопасности
	connCopy := *conn
	
	// Проверяем, что пароль присутствует
	if connCopy.Password == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":   "пароль не указан для подключения",
			"id":      id,
			"connected": false,
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	
	// Используем копию подключения с паролем
	if err := connManager.Connect(ctx, connCopy); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":   err.Error(),
			"id":      id,
			"connected": false,
		})
		return
	}

	// Обновляем статус подключения, сохраняя пароль
	connCopy.Connected = true
	config.UpdateConnection(id, connCopy)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":       id,
		"connected": true,
	})
}

func DisconnectHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	path := r.URL.Path
	id := strings.TrimPrefix(path, "/api/connections/")
	id = strings.TrimSuffix(id, "/disconnect")

	if err := connManager.Disconnect(id); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	conn, _ := config.GetConnectionByID(id)
	if conn != nil {
		// Сохраняем пароль перед обновлением
		savedPassword := conn.Password
		conn.Connected = false
		conn.Password = savedPassword
		config.UpdateConnection(id, *conn)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":       id,
		"connected": false,
	})
}

func ConnectionStatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	path := r.URL.Path
	id := strings.TrimPrefix(path, "/api/connections/")
	id = strings.TrimSuffix(id, "/status")

	isConnected := connManager.IsConnected(id)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":       id,
		"connected": isConnected,
	})
}

