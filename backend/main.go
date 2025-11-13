package main

import (
	"context"
	"database-manager/config"
	"database-manager/database"
	"database-manager/handlers"
	"database-manager/middleware"
	"database-manager/models"
	"database-manager/utils"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	connManager := database.NewConnectionManager()
	handlers.InitConnectionManager(connManager)

	connections, err := config.LoadConnections()
	if err != nil {
		log.Printf("Ошибка загрузки подключений: %v", err)
	}

	ctx := context.Background()
	if err := connManager.RestoreConnections(ctx, connections); err != nil {
		log.Printf("Ошибка восстановления подключений: %v", err)
	}

	_, err = config.LoadUsers()
	if err != nil {
		log.Printf("Ошибка загрузки пользователей: %v", err)
	}
	
	// Создаем тестового пользователя root, если его нет
	_, err = config.GetUserByUsername("root")
	if err != nil {
		hashedPassword, _ := utils.HashPassword("1234567890")
		rootUser := models.User{
			ID:           "00000000-0000-0000-0000-000000000001",
			Username:     "root",
			PasswordHash: hashedPassword,
			Email:        "",
			CreatedAt:    time.Now(),
		}
		if err := config.AddUser(rootUser); err != nil {
			log.Printf("Ошибка создания пользователя root: %v", err)
		} else {
			log.Println("Создан тестовый пользователь root с паролем 1234567890")
		}
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/api/auth/register", handlers.RegisterHandler)
	mux.HandleFunc("/api/auth/login", handlers.LoginHandler)

	mux.HandleFunc("/api/connections", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			middleware.AuthMiddleware(http.HandlerFunc(handlers.GetConnectionsHandler)).ServeHTTP(w, r)
		case http.MethodPost:
			middleware.AuthMiddleware(http.HandlerFunc(handlers.CreateConnectionHandler)).ServeHTTP(w, r)
		default:
			http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/connections/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		
		if strings.HasSuffix(path, "/connect") {
			middleware.AuthMiddleware(http.HandlerFunc(handlers.ConnectHandler)).ServeHTTP(w, r)
			return
		}
		if strings.HasSuffix(path, "/disconnect") {
			middleware.AuthMiddleware(http.HandlerFunc(handlers.DisconnectHandler)).ServeHTTP(w, r)
			return
		}
		if strings.HasSuffix(path, "/status") {
			middleware.AuthMiddleware(http.HandlerFunc(handlers.ConnectionStatusHandler)).ServeHTTP(w, r)
			return
		}

		id := strings.TrimPrefix(path, "/api/connections/")
		if id == "" {
			http.Error(w, "ID подключения не указан", http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodGet:
			middleware.AuthMiddleware(http.HandlerFunc(handlers.GetConnectionHandler)).ServeHTTP(w, r)
		case http.MethodPut:
			middleware.AuthMiddleware(http.HandlerFunc(handlers.UpdateConnectionHandler)).ServeHTTP(w, r)
		case http.MethodDelete:
			middleware.AuthMiddleware(http.HandlerFunc(handlers.DeleteConnectionHandler)).ServeHTTP(w, r)
		default:
			http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/query", middleware.AuthMiddleware(http.HandlerFunc(handlers.ExecuteQueryHandler)).ServeHTTP)
	
	mux.HandleFunc("/api/databases", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			middleware.AuthMiddleware(http.HandlerFunc(handlers.CreateDatabaseHandler)).ServeHTTP(w, r)
		case http.MethodGet:
			middleware.AuthMiddleware(http.HandlerFunc(handlers.ListDatabasesHandler)).ServeHTTP(w, r)
		default:
			http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		}
	})
	
	mux.HandleFunc("/api/databases/update", middleware.AuthMiddleware(http.HandlerFunc(handlers.UpdateDatabaseHandler)).ServeHTTP)
	mux.HandleFunc("/api/databases/delete", middleware.AuthMiddleware(http.HandlerFunc(handlers.DeleteDatabaseHandler)).ServeHTTP)
	
	mux.HandleFunc("/api/tables", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			middleware.AuthMiddleware(http.HandlerFunc(handlers.CreateTableHandler)).ServeHTTP(w, r)
		case http.MethodGet:
			middleware.AuthMiddleware(http.HandlerFunc(handlers.ListTablesHandler)).ServeHTTP(w, r)
		default:
			http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		}
	})
	
	mux.HandleFunc("/api/tables/update", middleware.AuthMiddleware(http.HandlerFunc(handlers.UpdateTableHandler)).ServeHTTP)
	mux.HandleFunc("/api/tables/delete", middleware.AuthMiddleware(http.HandlerFunc(handlers.DeleteTableHandler)).ServeHTTP)
	
	mux.HandleFunc("/api/users", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			middleware.AuthMiddleware(http.HandlerFunc(handlers.CreateUserHandler)).ServeHTTP(w, r)
		case http.MethodGet:
			middleware.AuthMiddleware(http.HandlerFunc(handlers.ListUsersHandler)).ServeHTTP(w, r)
		default:
			http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		}
	})
	
	mux.HandleFunc("/api/users/update", middleware.AuthMiddleware(http.HandlerFunc(handlers.UpdateUserHandler)).ServeHTTP)
	mux.HandleFunc("/api/users/delete", middleware.AuthMiddleware(http.HandlerFunc(handlers.DeleteUserHandler)).ServeHTTP)

	var htmxDir string
	// Проверяем, установлен ли пакет (путь /usr/share/database-manager/htmx существует)
	if _, err := os.Stat("/usr/share/database-manager/htmx"); err == nil {
		htmxDir = "/usr/share/database-manager/htmx"
	} else {
		// Иначе используем локальный путь для разработки
		workDir, _ := os.Getwd()
		htmxDir = filepath.Join(workDir, "..", "htmx")
	}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			indexPath := filepath.Join(htmxDir, "index.html")
			http.ServeFile(w, r, indexPath)
			return
		}

		if strings.HasPrefix(r.URL.Path, "/static/") {
			filePath := strings.TrimPrefix(r.URL.Path, "/static/")
			fullPath := filepath.Join(htmxDir, filePath)
			http.ServeFile(w, r, fullPath)
			return
		}

		if strings.HasSuffix(r.URL.Path, ".js") || strings.HasSuffix(r.URL.Path, ".css") || strings.HasSuffix(r.URL.Path, ".html") {
			fullPath := filepath.Join(htmxDir, r.URL.Path)
			http.ServeFile(w, r, fullPath)
			return
		}

		http.NotFound(w, r)
	})

	handler := middleware.CORSMiddleware(mux)

	appConfig, err := config.LoadAppConfig()
	if err != nil {
		log.Printf("Ошибка загрузки конфигурации: %v", err)
	}

	host := os.Getenv("HOST")
	if host == "" {
		if appConfig != nil && appConfig.Host != "" {
			host = appConfig.Host
		} else {
			host = "0.0.0.0"
		}
	}

	port := os.Getenv("PORT")
	if port == "" {
		if appConfig != nil && appConfig.Port != "" {
			port = appConfig.Port
		} else {
			port = "8081"
		}
	}

	displayHost := host
	if displayHost == "0.0.0.0" {
		displayHost = "localhost"
	}

	fmt.Printf("Сервер запущен на %s:%s\n", host, port)
	fmt.Printf("Откройте http://%s:%s в браузере\n", displayHost, port)
	
	if err := http.ListenAndServe(host+":"+port, handler); err != nil {
		log.Fatal(err)
	}
}

