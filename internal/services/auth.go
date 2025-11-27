package services

import (
	"bknd-1/internal/auth"
	"bknd-1/internal/config"
	"bknd-1/internal/logger"
	model "bknd-1/internal/models"
	"context"
	"database/sql"
	"fmt"
	"go.uber.org/zap"
	"regexp"
	"strings"
	"time"

	"github.com/go-ldap/ldap/v3"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
	"golang.org/x/crypto/bcrypt"
)

type AuthService struct {
	db   *bun.DB
	jwt  *auth.JWTManager
	cfg  *config.Config
	logr *logger.Logger
}

func NewAuthService(db *bun.DB, jwt *auth.JWTManager, cfg *config.Config, logr *logger.Logger) *AuthService {
	return &AuthService{db: db, jwt: jwt, cfg: cfg, logr: logr}
}

// HashPassword uses bcrypt
func HashPassword(password string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	return string(b), err
}

func ComparePassword(hash, password string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
}

type tokenResp struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	ExpiresAt    time.Time `json:"access_expires_at"`
	User         *UserInfo `json:"user"` // Add this
}

type UserInfo struct {
	ID       string   `json:"id"`
	Email    string   `json:"email"`
	Name     string   `json:"name"`
	Provider string   `json:"provider"`
	Roles    []string `json:"roles"`
}

// Local login
// Local login - Updated to match LoginLDAP signature
func (s *AuthService) LoginLocal(ctx context.Context, email, password, deviceInfo string) (*auth.TokenPair, *UserInfo, error) {
	var u model.User
	err := s.db.NewSelect().Model(&u).Where("email = ?", email).Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil, fmt.Errorf("invalid credentials")
		}
		return nil, nil, err
	}
	if u.PasswordHash == "" {
		return nil, nil, fmt.Errorf("account not configured for local login")
	}
	if err := ComparePassword(u.PasswordHash, password); err != nil {
		return nil, nil, fmt.Errorf("invalid credentials")
	}

	// update last_login
	now := time.Now().UTC()
	_, _ = s.db.NewUpdate().Model(&model.User{LastLoginAt: &now}).Where("id = ?", u.ID).Exec(ctx)

	// issue tokens and store refresh
	pair, err := s.jwt.GenerateTokenPair(u.ID.String(), s.cfg.AccessTokenTTL, s.cfg.RefreshTokenTTL, u.TokenVersion, "local", u.Roles)
	if err != nil {
		return nil, nil, err
	}

	if err := s.storeRefreshToken(ctx, u.ID, pair.RefreshToken, pair.RefreshExp, pair.JTI, deviceInfo); err != nil {
		return nil, nil, err
	}

	// Build UserInfo response
	userInfo := &UserInfo{
		ID:       u.ID.String(),
		Email:    u.Email,
		Name:     u.Name,
		Provider: "local",
		Roles:    u.Roles,
	}

	return pair, userInfo, nil
}

// LDAP login: search then bind (per your request)

// LoginLDAP performs LDAP authentication and returns tokens + user info
func (s *AuthService) LoginLDAP(ctx context.Context, ldapUser, ldapPass, deviceInfo string) (*auth.TokenPair, *UserInfo, error) {
	// Strip @ECGGH.COM if present (case insensitive)
	cleanUsername := ldapUser
	lowerUser := strings.ToLower(ldapUser)
	if strings.Contains(lowerUser, "@ecggh.com") {
		re := regexp.MustCompile(`(?i)@ecggh\.com$`)
		cleanUsername = re.ReplaceAllString(ldapUser, "")
	}

	// Dial LDAP with timeout
	ldap.DefaultTimeout = 10 * time.Second
	l, err := ldap.DialURL(s.cfg.LDAPServer)
	if err != nil {
		s.logr.Error("LDAP dial failed", zap.Error(err), zap.String("server", s.cfg.LDAPServer))
		return nil, nil, fmt.Errorf("ldap connection failed")
	}

	// CRITICAL: Always close connection, even on panic
	defer func() {
		if l != nil {
			if closeErr := l.Close(); closeErr != nil {
				s.logr.Debug("LDAP close error (usually harmless)", zap.Error(closeErr))
			}
		}
	}()

	// Set connection timeout
	l.SetTimeout(30 * time.Second)

	// Form DN: username@ECGGH.COM
	userDN := fmt.Sprintf("%s@ECGGH.COM", cleanUsername)

	// 1) Bind as user to authenticate
	if err = l.Bind(userDN, ldapPass); err != nil {
		s.logr.Warn("LDAP bind failed", zap.String("username", cleanUsername))
		return nil, nil, fmt.Errorf("invalid credentials")
	}

	// 2) Search for user attributes
	searchReq := ldap.NewSearchRequest(
		"dc=ecggh,dc=com",
		ldap.ScopeWholeSubtree,
		ldap.NeverDerefAliases,
		0,
		0,
		false,
		fmt.Sprintf("(sAMAccountName=%s)", ldap.EscapeFilter(cleanUsername)),
		[]string{"cn", "givenName", "sn", "mail", "memberOf", "employeeID", "telephoneNumber", "displayName"},
		nil,
	)

	sr, err := l.Search(searchReq)
	if err != nil {
		s.logr.Error("LDAP search failed", zap.Error(err), zap.String("username", cleanUsername))
		return nil, nil, fmt.Errorf("user lookup failed")
	}

	if len(sr.Entries) == 0 {
		s.logr.Warn("LDAP: no entry found", zap.String("username", cleanUsername))
		return nil, nil, fmt.Errorf("user not found in directory")
	}

	// Extract attributes
	entry := sr.Entries[0]
	displayName := entry.GetAttributeValue("displayName")
	givenName := entry.GetAttributeValue("givenName")
	sn := entry.GetAttributeValue("sn")
	mail := entry.GetAttributeValue("mail")

	if mail == "" {
		s.logr.Error("LDAP user missing email", zap.String("username", cleanUsername))
		return nil, nil, fmt.Errorf("user account missing email")
	}

	// Parse name
	var firstName, lastName string
	if displayName != "" {
		nameParts := strings.Fields(displayName)
		if len(nameParts) > 0 {
			firstName = nameParts[0]
			lastName = nameParts[len(nameParts)-1]
		}
	}
	if firstName == "" {
		firstName = givenName
	}
	if lastName == "" {
		lastName = sn
	}

	fullName := displayName
	if fullName == "" {
		fullName = entry.GetAttributeValue("cn")
	}
	if fullName == "" {
		fullName = cleanUsername
	}

	// Close LDAP connection NOW before DB operations
	l.Close()
	l = nil // Prevent double-close in defer

	s.logr.Debug("LDAP auth successful", zap.String("username", cleanUsername), zap.String("email", mail))

	// Database operations (user provisioning)
	var u model.User
	err = s.db.NewSelect().
		Model(&u).
		Column("id", "email", "provider", "name", "roles", "token_version", "created_at"). // Only needed columns
		Where("email = ?", mail).
		Scan(ctx)

	if err != nil {
		if err == sql.ErrNoRows {
			// Create new user
			u = model.User{
				Email:    mail,
				Provider: "ldap",
				Name:     fullName,
				Roles:    []string{"user"}, // Default role
			}
			_, err = s.db.NewInsert().Model(&u).Exec(ctx)
			if err != nil {
				s.logr.Error("Failed to create user", zap.Error(err), zap.String("email", mail))
				return nil, nil, fmt.Errorf("failed to create user account")
			}
			s.logr.Info("Created new LDAP user", zap.String("email", mail), zap.String("id", u.ID.String()))
		} else {
			s.logr.Error("Database error", zap.Error(err), zap.String("email", mail))
			return nil, nil, fmt.Errorf("database error")
		}
	} else {
		// Update provider if needed
		if u.Provider != "ldap" {
			_, _ = s.db.NewUpdate().Model(&u).
				Set("provider = ?", "ldap").
				Where("id = ?", u.ID).
				Exec(ctx)
		}
	}

	// Update last login
	now := time.Now().UTC()
	_, _ = s.db.NewUpdate().
		Model(&model.User{LastLoginAt: &now}).
		Where("id = ?", u.ID).
		Exec(ctx)

	// Generate tokens
	pair, err := s.jwt.GenerateTokenPair(
		u.ID.String(),
		s.cfg.AccessTokenTTL,
		s.cfg.RefreshTokenTTL,
		u.TokenVersion,
		"ldap",
		u.Roles,
	)
	if err != nil {
		s.logr.Error("Token generation failed", zap.Error(err), zap.String("user_id", u.ID.String()))
		return nil, nil, fmt.Errorf("failed to generate tokens")
	}

	// Store refresh token
	if err := s.storeRefreshToken(ctx, u.ID, pair.RefreshToken, pair.RefreshExp, pair.JTI, deviceInfo); err != nil {
		s.logr.Error("Failed to store refresh token", zap.Error(err), zap.String("user_id", u.ID.String()))
		return nil, nil, fmt.Errorf("failed to store session")
	}

	// Build response
	userInfo := &UserInfo{
		ID:       u.ID.String(),
		Email:    mail,
		Name:     fullName,
		Provider: "ldap",
		Roles:    u.Roles,
	}

	s.logr.Info("LDAP login successful",
		zap.String("user_id", u.ID.String()),
		zap.String("email", mail),
		zap.String("username", cleanUsername))

	return pair, userInfo, nil
}

// storeRefreshToken stores refresh token hashed and enforces 2 sessions per user
func (s *AuthService) storeRefreshToken(ctx context.Context, userID uuid.UUID, refreshToken string, expiresAt time.Time, jti string, deviceInfo string) error {
	// 1) cleanup expired tokens for user
	_, _ = s.db.NewDelete().Model((*model.RefreshToken)(nil)).Where("user_id = ? AND expires_at < now()", userID).Exec(ctx)

	// 2) enforce max 2 active sessions (non-revoked & not expired)
	var count int
	err := s.db.NewSelect().ColumnExpr("count(*)").Table("refresh_tokens").Where("user_id = ? AND revoked = false AND expires_at > now()", userID).Scan(ctx, &count)
	if err == nil && count >= 2 {
		// delete oldest non-revoked token(s) to make room
		// remove oldest until count < 2
		toRemove := count - 1 // leave 1 plus new => 2
		if toRemove <= 0 {
			toRemove = 1
		}
		// delete by created_at order
		_, _ = s.db.NewDelete().Model((*model.RefreshToken)(nil)).
			Where("id IN (SELECT id FROM refresh_tokens WHERE user_id = ? AND revoked = false AND expires_at > now() ORDER BY created_at ASC LIMIT ?)", userID, toRemove).
			Exec(ctx)
	}

	hashed := auth.HashToken(refreshToken)
	rt := model.RefreshToken{
		UserID:     userID,
		JTI:        jti,
		TokenHash:  hashed,
		DeviceInfo: &deviceInfo,
		Revoked:    false,
		CreatedAt:  time.Now().UTC(),
		ExpiresAt:  expiresAt,
	}
	_, err = s.db.NewInsert().Model(&rt).Exec(ctx)
	return err
}

// Refresh: takes refresh token string, verifies JWT, finds DB record by JTI & hash, rotates
func (s *AuthService) Refresh(ctx context.Context, refreshToken string, deviceInfo string) (*auth.TokenPair, error) {
	// verify JWT signature & claims
	claims, err := s.jwt.VerifyToken(refreshToken)
	if err != nil {
		return nil, fmt.Errorf("invalid refresh token: %w", err)
	}
	// ensure typ == refresh
	if claims["typ"] != string(auth.RefreshToken) {
		return nil, fmt.Errorf("not a refresh token")
	}
	_, ok := claims["sub"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid token sub")
	}
	jti, ok := claims["jti"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid token jti")
	}
	// find refresh token record by user_id and jti and token_hash
	hashed := auth.HashToken(refreshToken)

	var rt model.RefreshToken
	err = s.db.NewSelect().Model(&rt).Where("jti = ? AND token_hash = ? AND revoked = false AND expires_at > now()", jti, hashed).Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("refresh token not found or revoked")
	}

	// get user
	var u model.User
	err = s.db.NewSelect().Model(&u).Where("id = ?", rt.UserID).Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("user not found")
	}

	// rotate: revoke old token (mark revoked) and create new token pair & store new refresh token
	_, _ = s.db.NewUpdate().Model(&model.RefreshToken{Revoked: true}).Where("id = ?", rt.ID).Exec(ctx)

	pair, err := s.jwt.GenerateTokenPair(u.ID.String(), s.cfg.AccessTokenTTL, s.cfg.RefreshTokenTTL, u.TokenVersion, "refresh", u.Roles)
	if err != nil {
		return nil, err
	}
	if err := s.storeRefreshToken(ctx, u.ID, pair.RefreshToken, pair.RefreshExp, pair.JTI, deviceInfo); err != nil {
		return nil, err
	}

	return pair, nil
}

// Logout: revoke a refresh token by JTI or token string
func (s *AuthService) Logout(ctx context.Context, refreshToken string) error {
	claims, err := s.jwt.VerifyToken(refreshToken)
	if err != nil {
		return err
	}
	jti, ok := claims["jti"].(string)
	if !ok {
		return fmt.Errorf("invalid jti")
	}
	// mark revoked
	_, err = s.db.NewUpdate().Model(&model.RefreshToken{Revoked: true}).Where("jti = ?", jti).Exec(ctx)
	return err
}

func (s *AuthService) CheckTokenVersion(ctx context.Context, userID string, tokenVersion int) (bool, error) {
	var user model.User
	err := s.db.NewSelect().Model(&user).Where("id = ?", userID).Scan(ctx)
	if err != nil {
		return false, err
	}
	return user.TokenVersion == tokenVersion, nil
}
