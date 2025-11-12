package models

import "time"

type User struct {
	ID           string    `json:"id"`
	Username     string    `json:"username"`
	PasswordHash string    `json:"-"` // Не возвращаем в JSON
	Email        string    `json:"email,omitempty"`
	CreatedAt    time.Time `json:"createdAt"`
}

