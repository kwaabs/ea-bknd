package services

import (
	"bknd-1/internal/auth"
	"bknd-1/internal/config"
	"bknd-1/internal/logger"
	model "bknd-1/internal/models"
	"context"
	"database/sql"
	"fmt"
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

// Local login
func (s *AuthService) LoginLocal(ctx context.Context, email, password, deviceInfo string) (*auth.TokenPair, error) {
	var u model.User
	err := s.db.NewSelect().Model(&u).Where("email = ?", email).Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("invalid credentials")
		}
		return nil, err
	}
	if u.PasswordHash == "" {
		return nil, fmt.Errorf("account not configured for local login")
	}
	if err := ComparePassword(u.PasswordHash, password); err != nil {
		return nil, fmt.Errorf("invalid credentials")
	}

	// update last_login
	now := time.Now().UTC()
	_, _ = s.db.NewUpdate().Model(&model.User{LastLoginAt: &now}).Where("id = ?", u.ID).Exec(ctx)

	// issue tokens and store refresh
	pair, err := s.jwt.GenerateTokenPair(u.ID.String(), s.cfg.AccessTokenTTL, s.cfg.RefreshTokenTTL, u.TokenVersion, "local", u.Roles)
	if err != nil {
		return nil, err
	}

	if err := s.storeRefreshToken(ctx, u.ID, pair.RefreshToken, pair.RefreshExp, pair.JTI, deviceInfo); err != nil {
		return nil, err
	}

	return pair, nil
}

// LDAP login: search then bind (per your request)
func (s *AuthService) LoginLDAP(ctx context.Context, ldapUser, ldapPass, deviceInfo string) (*auth.TokenPair, error) {
	l, err := ldap.DialURL(s.cfg.LDAPServer)
	if err != nil {
		return nil, fmt.Errorf("ldap dial: %w", err)
	}
	defer l.Close()

	// Bind as service user if provided to search
	if s.cfg.LDAPBindDN != "" {
		if err = l.Bind(s.cfg.LDAPBindDN, s.cfg.LDAPBindPass); err != nil {
			return nil, fmt.Errorf("ldap bind service account: %w", err)
		}
	}

	// Search for the user DN
	searchReq := ldap.NewSearchRequest(
		s.cfg.LDAPBaseDN,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		fmt.Sprintf("(uid=%s)", ldap.EscapeFilter(ldapUser)),
		[]string{"dn", "mail", "cn"},
		nil,
	)

	sr, err := l.Search(searchReq)
	if err != nil {
		return nil, fmt.Errorf("ldap search: %w", err)
	}
	if len(sr.Entries) == 0 {
		return nil, fmt.Errorf("ldap: user not found")
	}
	userDN := sr.Entries[0].DN
	mail := sr.Entries[0].GetAttributeValue("mail")
	cn := sr.Entries[0].GetAttributeValue("cn")

	// Try to bind as the user to verify credentials
	if err = l.Bind(userDN, ldapPass); err != nil {
		return nil, fmt.Errorf("ldap bind user failed: %w", err)
	}

	// Provision or fetch local user
	var u model.User
	err = s.db.NewSelect().Model(&u).Where("email = ?", mail).Scan(ctx)
	if err != nil {
		// create user
		u = model.User{
			Email:    mail,
			Provider: "ldap",
			Name:     cn,
		}
		_, err = s.db.NewInsert().Model(&u).Exec(ctx)
		if err != nil {
			return nil, fmt.Errorf("create user: %w", err)
		}
	} else {
		// ensure provider is ldap
		if u.Provider != "ldap" {
			// you may decide to link accounts; for now keep provider as found
			u.Provider = "ldap"
			_, _ = s.db.NewUpdate().Model(&u).Where("id = ?", u.ID).Exec(ctx)
		}
	}

	// update last_login
	now := time.Now().UTC()
	_, _ = s.db.NewUpdate().Model(&model.User{LastLoginAt: &now}).Where("id = ?", u.ID).Exec(ctx)

	// issue token pair
	pair, err := s.jwt.GenerateTokenPair(u.ID.String(), s.cfg.AccessTokenTTL, s.cfg.RefreshTokenTTL, u.TokenVersion, "ldap", u.Roles)
	if err != nil {
		return nil, err
	}

	if err := s.storeRefreshToken(ctx, u.ID, pair.RefreshToken, pair.RefreshExp, pair.JTI, deviceInfo); err != nil {
		return nil, err
	}

	return pair, nil
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
