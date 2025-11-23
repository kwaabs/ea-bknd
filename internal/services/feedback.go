package services

import (
	"context"
	"time"

	"github.com/uptrace/bun"
)

// Feedback model
type Feedback struct {
	bun.BaseModel `bun:"table:app.feedback,alias:fbk"`

	ID        int64     `bun:"id,pk,autoincrement" json:"id"`
	Email     string    `bun:"email,notnull" json:"email"`
	Type      string    `bun:"type,notnull" json:"type"`
	Comments  string    `bun:"comments,notnull" json:"comments"`
	Status    string    `bun:"status,notnull,default:'OPEN'" json:"status"`
	ParentID  *int64    `bun:"parent_id" json:"parent_id,omitempty"`
	CreatedAt time.Time `bun:"created_at,notnull,default:current_timestamp" json:"created_at"`
	UpdatedAt time.Time `bun:"updated_at,notnull,default:current_timestamp" json:"updated_at"`

	// Relations
	Replies []*Feedback `bun:"rel:has-many,join:id=parent_id" json:"replies,omitempty"`
}

// CreateFeedbackRequest represents the request body
type CreateFeedbackRequest struct {
	Email    string `json:"email"`
	Type     string `json:"type"`
	Comments string `json:"comments"`
	ParentID *int64 `json:"parent_id,omitempty"` // For replies
}

// UpdateFeedbackStatusRequest represents status update request
type UpdateFeedbackStatusRequest struct {
	Status string `json:"status"`
}

// FeedbackResponse represents the API response
type FeedbackResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// FeedbackService handles feedback business logic
type FeedbackService struct {
	db *bun.DB
}

// NewFeedbackService creates a new feedback service
func NewFeedbackService(db *bun.DB) *FeedbackService {
	return &FeedbackService{db: db}
}

// CreateFeedback creates a new feedback entry
func (s *FeedbackService) CreateFeedback(ctx context.Context, req CreateFeedbackRequest) error {
	feedback := &Feedback{
		Email:     req.Email,
		Type:      req.Type,
		Comments:  req.Comments,
		ParentID:  req.ParentID,
		Status:    "OPEN",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// If this is a reply, inherit the type from parent
	if req.ParentID != nil {
		var parent Feedback
		err := s.db.NewSelect().
			Model(&parent).
			Where("id = ?", *req.ParentID).
			Scan(ctx)
		if err != nil {
			return err
		}
		feedback.Type = parent.Type
	}

	_, err := s.db.NewInsert().Model(feedback).Exec(ctx)
	return err
}

// GetFeedbackByEmail retrieves all feedback for a specific email (optional, for future use)
func (s *FeedbackService) GetFeedbackByEmail(ctx context.Context, email string) ([]Feedback, error) {
	var feedbacks []Feedback
	err := s.db.NewSelect().
		Model(&feedbacks).
		Where("email = ?", email).
		Where("parent_id IS NULL"). // Only get top-level feedback
		Relation("Replies").
		Order("created_at DESC").
		Scan(ctx)
	return feedbacks, err
}

// GetAllFeedback retrieves all feedback (optional, for admin use)
func (s *FeedbackService) GetAllFeedback(ctx context.Context, limit, offset int) ([]Feedback, error) {
	var feedbacks []Feedback
	err := s.db.NewSelect().
		Model(&feedbacks).
		Where("parent_id IS NULL"). // Only get top-level feedback
		Relation("Replies").
		Order("created_at DESC").
		Limit(limit).
		Offset(offset).
		Scan(ctx)
	return feedbacks, err
}

// GetFeedbackByID retrieves a single feedback with replies
func (s *FeedbackService) GetFeedbackByID(ctx context.Context, id int64) (*Feedback, error) {
	feedback := new(Feedback)
	err := s.db.NewSelect().
		Model(feedback).
		Where("id = ?", id).
		Relation("Replies").
		Scan(ctx)
	return feedback, err
}

// UpdateFeedbackStatus updates the status of a feedback
func (s *FeedbackService) UpdateFeedbackStatus(ctx context.Context, id int64, status string) error {
	_, err := s.db.NewUpdate().
		Model((*Feedback)(nil)).
		Set("status = ?", status).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", id).
		Exec(ctx)
	return err
}
