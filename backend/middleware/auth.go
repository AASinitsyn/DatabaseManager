package middleware

import (
	"database-manager/utils"
	"net/http"
	"strings"
)

func AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Отсутствует токен авторизации", http.StatusUnauthorized)
			return
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			http.Error(w, "Неверный формат токена", http.StatusUnauthorized)
			return
		}

		token := parts[1]
		claims, err := utils.ValidateToken(token)
		if err != nil {
			http.Error(w, "Невалидный токен", http.StatusUnauthorized)
			return
		}

		r.Header.Set("UserID", claims.UserID)
		r.Header.Set("Username", claims.Username)

		next.ServeHTTP(w, r)
	})
}

