package handlers

import (
	"database-manager/config"
	"database-manager/models"
	"database-manager/utils"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
)

func RegisterHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	var req models.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Ошибка парсинга запроса", http.StatusBadRequest)
		return
	}

	if req.Username == "" || req.Password == "" {
		http.Error(w, "Имя пользователя и пароль обязательны", http.StatusBadRequest)
		return
	}

	existingUser, _ := config.GetUserByUsername(req.Username)
	if existingUser != nil {
		http.Error(w, "Пользователь уже существует", http.StatusConflict)
		return
	}

	hashedPassword, err := utils.HashPassword(req.Password)
	if err != nil {
		http.Error(w, "Ошибка хеширования пароля", http.StatusInternalServerError)
		return
	}

	user := models.User{
		ID:           uuid.New().String(),
		Username:     req.Username,
		PasswordHash: hashedPassword,
		Email:        req.Email,
		CreatedAt:    time.Now(),
	}

	if err := config.AddUser(user); err != nil {
		http.Error(w, "Ошибка сохранения пользователя", http.StatusInternalServerError)
		return
	}

	token, err := utils.GenerateToken(user)
	if err != nil {
		http.Error(w, "Ошибка генерации токена", http.StatusInternalServerError)
		return
	}

	user.PasswordHash = ""
	response := models.LoginResponse{
		Token: token,
		User:  user,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func LoginHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	var req models.LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Ошибка парсинга запроса", http.StatusBadRequest)
		return
	}

	user, err := config.GetUserByUsername(req.Username)
	if err != nil {
		http.Error(w, "Неверное имя пользователя или пароль", http.StatusUnauthorized)
		return
	}

	if !utils.CheckPasswordHash(req.Password, user.PasswordHash) {
		http.Error(w, "Неверное имя пользователя или пароль", http.StatusUnauthorized)
		return
	}

	token, err := utils.GenerateToken(*user)
	if err != nil {
		http.Error(w, "Ошибка генерации токена", http.StatusInternalServerError)
		return
	}

	user.PasswordHash = ""
	response := models.LoginResponse{
		Token: token,
		User:  *user,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

