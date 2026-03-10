package services

import (
	"bknd-1/internal/models"
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun"
)

type FeedbackService struct {
	db *bun.DB
}

func NewFeedbackService(db *bun.DB) *FeedbackService {
	return &FeedbackService{db: db}
}

// GetAllFeedback retrieves all top-level feedback with their replies
func (s *FeedbackService) GetAllFeedback(ctx context.Context, limit, offset int) ([]*models.Feedback, int, error) {
	var feedbacks []*models.Feedback

	// Get total count of top-level feedback
	total, err := s.db.NewSelect().
		Model((*models.Feedback)(nil)).
		Where("parent_id IS NULL").
		Count(ctx)
	if err != nil {
		return nil, 0, err
	}

	// Get top-level feedback with pagination
	err = s.db.NewSelect().
		Model(&feedbacks).
		Where("parent_id IS NULL").
		Order("created_at DESC").
		Limit(limit).
		Offset(offset).
		Scan(ctx)
	if err != nil {
		return nil, 0, err
	}

	// Load replies for each feedback
	for _, f := range feedbacks {
		var replies []*models.Feedback
		err = s.db.NewSelect().
			Model(&replies).
			Where("parent_id = ?", f.ID).
			Order("created_at ASC").
			Scan(ctx)
		if err != nil {
			return nil, 0, err
		}
		f.Replies = replies
	}

	return feedbacks, total, nil
}

// GetFeedbackByID retrieves a single feedback with its replies
func (s *FeedbackService) GetFeedbackByID(ctx context.Context, id int64) (*models.Feedback, error) {
	feedback := new(models.Feedback)

	err := s.db.NewSelect().
		Model(feedback).
		Where("id = ?", id).
		Scan(ctx)
	if err != nil {
		return nil, err
	}

	// Load replies
	var replies []*models.Feedback
	err = s.db.NewSelect().
		Model(&replies).
		Where("parent_id = ?", id).
		Order("created_at ASC").
		Scan(ctx)
	if err != nil {
		return nil, err
	}
	feedback.Replies = replies

	return feedback, nil
}

// GetFeedbackByEmail retrieves feedback by email
func (s *FeedbackService) GetFeedbackByEmail(ctx context.Context, email string) ([]*models.Feedback, error) {
	var feedbacks []*models.Feedback

	err := s.db.NewSelect().
		Model(&feedbacks).
		Where("email = ?", email).
		Where("parent_id IS NULL").
		Order("created_at DESC").
		Scan(ctx)
	if err != nil {
		return nil, err
	}

	// Load replies for each feedback
	for _, f := range feedbacks {
		var replies []*models.Feedback
		err = s.db.NewSelect().
			Model(&replies).
			Where("parent_id = ?", f.ID).
			Order("created_at ASC").
			Scan(ctx)
		if err != nil {
			return nil, err
		}
		f.Replies = replies
	}

	return feedbacks, nil
}

// DeleteFeedback deletes a feedback and its replies
func (s *FeedbackService) DeleteFeedback(ctx context.Context, id int64) error {
	// Start a transaction
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Delete replies first (though CASCADE should handle this)
	_, err = tx.NewDelete().
		Model((*models.Feedback)(nil)).
		Where("parent_id = ?", id).
		Exec(ctx)
	if err != nil {
		return err
	}

	// Delete the main feedback
	_, err = tx.NewDelete().
		Model((*models.Feedback)(nil)).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// CreateFeedback creates a new feedback or reply
func (s *FeedbackService) CreateFeedback(ctx context.Context, req *models.CreateFeedbackRequest) error {
	feedback := &models.Feedback{
		Email:    req.Email,
		ParentID: req.ParentID,
		Comments: &req.Comments,
	}

	// Set type and status based on whether it's top-level or reply
	if req.ParentID == nil {
		// Top-level comment
		feedback.Type = &req.Type

		// Set default status to "PENDING" for top-level comments
		defaultStatus := models.StatusPending
		feedback.Status = &defaultStatus // This is correct: &string gives *string
	} else {
		// Reply - type and status should be NULL
		feedback.Type = nil
		feedback.Status = nil
	}

	_, err := s.db.NewInsert().
		Model(feedback).
		Exec(ctx)

	return err
}

// UpdateFeedbackStatus updates the status of a feedback
func (s *FeedbackService) UpdateFeedbackStatus(ctx context.Context, id int64, status string) (*models.Feedback, error) {
	// Check if feedback exists and is top-level
	feedback := new(models.Feedback)
	err := s.db.NewSelect().
		Model(feedback).
		Where("id = ? AND parent_id IS NULL", id).
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("feedback not found or is a reply: %w", err)
	}

	// Validate status against enum values (compare with string constants)
	validStatuses := map[string]bool{
		models.StatusPending:    true,
		models.StatusInProgress: true,
		models.StatusResolved:   true,
	}

	if !validStatuses[status] {
		return nil, fmt.Errorf("invalid status: %s", status)
	}

	// Update status (status is string, we need *string for the model)
	feedback.Status = &status
	feedback.UpdatedAt = time.Now()

	_, err = s.db.NewUpdate().
		Model(feedback).
		Column("status", "updated_at").
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return nil, err
	}

	return s.GetFeedbackByID(ctx, id)
}
