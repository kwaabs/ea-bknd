package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

type User struct {
	bun.BaseModel `bun:"table:users"`
	ID            uuid.UUID  `bun:",pk,type:uuid,default:uuid_generate_v4()" json:"id"`
	Email         string     `json:"email"`
	PasswordHash  string     `json:"password_hash"`
	TokenVersion  int        `bun:"token_version" json:"token_version"`
	Roles         []string   `json:"roles" bun:"type:text[]"`
	Provider      string     `json:"provider"`
	Name          string     `json:"name"`
	CreatedAt     time.Time  `json:"created_at"`
	LastLoginAt   *time.Time `json:"last_login_at"`
}

type RefreshToken struct {
	bun.BaseModel `bun:"table:refresh_tokens"`
	ID            uuid.UUID `bun:",pk,type:uuid,default:uuid_generate_v4()" json:"id"`
	UserID        uuid.UUID `bun:"type:uuid" json:"user_id"`
	JTI           string    `json:"jti"`
	TokenHash     string    `json:"token_hash"`
	DeviceInfo    *string   `json:"device_info"`
	Revoked       bool      `json:"revoked"`
	CreatedAt     time.Time `json:"created_at"`
	ExpiresAt     time.Time `json:"expires_at"`
}
