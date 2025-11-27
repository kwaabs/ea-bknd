package config

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Port        string
	DatabaseURL string
	Environment string
	BunDebug    bool

	// JWT / keys
	JWTPrivateKeyPath string // path to PEM private key
	JWTPublicKeyPath  string // path to PEM public key
	AccessTokenTTL    time.Duration
	RefreshTokenTTL   time.Duration

	// LDAP
	LDAPServer   string
	LDAPBindDN   string
	LDAPBindPass string
	LDAPBaseDN   string

	// CORS - Add this
	AllowedOrigins []string
}

// Load loads environment variables and returns a Config struct
func Load() *Config {
	_ = godotenv.Load()

	accessTTLMin, _ := strconv.Atoi(getEnv("ACCESS_TOKEN_MINUTES", "15"))
	refreshTTLDays, _ := strconv.Atoi(getEnv("REFRESH_TOKEN_DAYS", "10"))

	// Parse allowed origins from env (comma-separated)
	allowedOrigins := strings.Split(
		getEnv("ALLOWED_ORIGINS", "http://localhost:3000,http://localhost:5173"),
		",",
	)

	return &Config{
		Port:              getEnv("APP_PORT", "8780"),
		DatabaseURL:       getEnv("DATABASE_URL", "postgres://postgres:password@localhost:5432/ea-5?sslmode=disable"),
		Environment:       getEnv("ENVIRONMENT", "development"),
		BunDebug:          getEnvAsBool("BUNDEBUG", false),
		JWTPrivateKeyPath: getEnv("JWT_PRIVATE_KEY_PATH", "keys/jwt_private.pem"),
		JWTPublicKeyPath:  getEnv("JWT_PUBLIC_KEY_PATH", "keys/jwt_public.pem"),
		AccessTokenTTL:    time.Duration(accessTTLMin) * time.Minute,      // default 15m
		RefreshTokenTTL:   time.Duration(refreshTTLDays) * 24 * time.Hour, // default 10d
		//LDAPServer:        getEnv("LDAP_SERVER", "ldap://ldap.example.com:389"),
		LDAPServer:     getEnv("LDAP_SERVER", "ldap://localhost:10389"),
		LDAPBindDN:     getEnv("LDAP_BIND_DN", ""),
		LDAPBindPass:   getEnv("LDAP_BIND_PASS", ""),
		LDAPBaseDN:     getEnv("LDAP_BASE_DN", ""),
		AllowedOrigins: allowedOrigins, // Add this
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getEnvAsBool(key string, fallback bool) bool {
	valStr := os.Getenv(key)
	if valStr == "" {
		return fallback
	}
	val, err := strconv.ParseBool(valStr)
	if err != nil {
		log.Printf("invalid bool for %s, defaulting to %v\n", key, fallback)
		return fallback
	}
	return val
}
