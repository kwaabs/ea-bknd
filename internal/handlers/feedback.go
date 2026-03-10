// package handlers

// import (
// 	"bknd-1/internal/services"
// 	"encoding/json"
// 	"github.com/go-chi/chi/v5"
// 	"go.uber.org/zap"
// 	"net/http"
// 	"strconv"
// 	"strings"
// )

// // FeedbackHandler handles HTTP requests for feedback
// type FeedbackHandler struct {
// 	service *services.FeedbackService
// 	logr    *zap.Logger
// }

// // NewFeedbackHandler creates a new feedback handler
// func NewFeedbackHandler(svc *services.FeedbackService, logr *zap.Logger) *FeedbackHandler {
// 	return &FeedbackHandler{
// 		service: svc,
// 		logr:    logr,
// 	}
// }

// // CreateFeedback handles POST /feedback
// func (h *FeedbackHandler) CreateFeedback(w http.ResponseWriter, r *http.Request) {
// 	var req services.CreateFeedbackRequest

// 	// Decode request body
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		h.logr.Error("failed to decode request body", zap.Error(err))
// 		writeJSON(w, http.StatusBadRequest, services.FeedbackResponse{
// 			Success: false,
// 			Message: "Invalid request body",
// 		})
// 		return
// 	}

// 	// Basic validation
// 	if strings.TrimSpace(req.Email) == "" {
// 		h.logr.Warn("validation failed: email is required")
// 		writeJSON(w, http.StatusBadRequest, services.FeedbackResponse{
// 			Success: false,
// 			Message: "Email is required",
// 		})
// 		return
// 	}

// 	// Only validate type if this is NOT a reply
// 	if req.ParentID == nil {
// 		if req.Type != "COMPLAINT" && req.Type != "COMMENT" {
// 			h.logr.Warn("validation failed: invalid type", zap.String("type", req.Type))
// 			writeJSON(w, http.StatusBadRequest, services.FeedbackResponse{
// 				Success: false,
// 				Message: "Type must be either COMPLAINT or COMMENT",
// 			})
// 			return
// 		}
// 	}

// 	if strings.TrimSpace(req.Comments) == "" {
// 		h.logr.Warn("validation failed: comments are required")
// 		writeJSON(w, http.StatusBadRequest, services.FeedbackResponse{
// 			Success: false,
// 			Message: "Comments are required",
// 		})
// 		return
// 	}

// 	// Create feedback
// 	if err := h.service.CreateFeedback(r.Context(), req); err != nil {
// 		h.logr.Error("failed to create feedback", zap.Error(err))
// 		writeJSON(w, http.StatusInternalServerError, services.FeedbackResponse{
// 			Success: false,
// 			Message: "Failed to submit feedback",
// 		})
// 		return
// 	}

// 	message := "Feedback submitted successfully"
// 	if req.ParentID != nil {
// 		message = "Reply submitted successfully"
// 	}

// 	h.logr.Info("feedback submitted successfully",
// 		zap.String("email", req.Email),
// 		zap.String("type", req.Type),
// 		zap.Bool("is_reply", req.ParentID != nil))

// 	// Send success response
// 	writeJSON(w, http.StatusOK, services.FeedbackResponse{
// 		Success: true,
// 		Message: message,
// 	})
// }

// // GetFeedbackByEmail handles GET /feedback/user/{email}
// func (h *FeedbackHandler) GetFeedbackByEmail(w http.ResponseWriter, r *http.Request) {
// 	email := chi.URLParam(r, "email")

// 	if strings.TrimSpace(email) == "" {
// 		h.logr.Warn("validation failed: email parameter is required")
// 		writeJSON(w, http.StatusBadRequest, services.FeedbackResponse{
// 			Success: false,
// 			Message: "Email parameter is required",
// 		})
// 		return
// 	}

// 	feedbacks, err := h.service.GetFeedbackByEmail(r.Context(), email)
// 	if err != nil {
// 		h.logr.Error("failed to fetch feedback by email", zap.Error(err), zap.String("email", email))
// 		writeJSON(w, http.StatusInternalServerError, services.FeedbackResponse{
// 			Success: false,
// 			Message: "Failed to fetch feedback",
// 		})
// 		return
// 	}

// 	h.logr.Info("feedback fetched successfully by email",
// 		zap.String("email", email),
// 		zap.Int("count", len(feedbacks)))

// 	writeJSON(w, http.StatusOK, map[string]any{
// 		"success": true,
// 		"data":    feedbacks,
// 		"count":   len(feedbacks),
// 	})
// }

// // GetAllFeedback handles GET /feedback
// func (h *FeedbackHandler) GetAllFeedback(w http.ResponseWriter, r *http.Request) {
// 	q := r.URL.Query()

// 	// Parse pagination parameters
// 	limit, err := strconv.Atoi(q.Get("limit"))
// 	if err != nil || limit <= 0 {
// 		limit = 50 // default
// 	}

// 	offset, err := strconv.Atoi(q.Get("offset"))
// 	if err != nil || offset < 0 {
// 		offset = 0 // default
// 	}

// 	feedbacks, err := h.service.GetAllFeedback(r.Context(), limit, offset)
// 	if err != nil {
// 		h.logr.Error("failed to fetch all feedback", zap.Error(err))
// 		writeJSON(w, http.StatusInternalServerError, services.FeedbackResponse{
// 			Success: false,
// 			Message: "Failed to fetch feedback",
// 		})
// 		return
// 	}

// 	h.logr.Info("all feedback fetched successfully",
// 		zap.Int("count", len(feedbacks)),
// 		zap.Int("limit", limit),
// 		zap.Int("offset", offset))

// 	writeJSON(w, http.StatusOK, map[string]any{
// 		"success": true,
// 		"data":    feedbacks,
// 		"count":   len(feedbacks),
// 		"limit":   limit,
// 		"offset":  offset,
// 	})
// }

// // GetFeedbackByID handles GET /feedback/{id}
// func (h *FeedbackHandler) GetFeedbackByID(w http.ResponseWriter, r *http.Request) {
// 	idStr := chi.URLParam(r, "id")
// 	id, err := strconv.ParseInt(idStr, 10, 64)
// 	if err != nil {
// 		h.logr.Warn("invalid feedback ID", zap.String("id", idStr))
// 		writeJSON(w, http.StatusBadRequest, services.FeedbackResponse{
// 			Success: false,
// 			Message: "Invalid feedback ID",
// 		})
// 		return
// 	}

// 	feedback, err := h.service.GetFeedbackByID(r.Context(), id)
// 	if err != nil {
// 		h.logr.Error("failed to fetch feedback by ID", zap.Error(err), zap.Int64("id", id))
// 		writeJSON(w, http.StatusNotFound, services.FeedbackResponse{
// 			Success: false,
// 			Message: "Feedback not found",
// 		})
// 		return
// 	}

// 	h.logr.Info("feedback fetched successfully by ID", zap.Int64("id", id))

// 	writeJSON(w, http.StatusOK, map[string]any{
// 		"success": true,
// 		"data":    feedback,
// 	})
// }

// // UpdateFeedbackStatus handles PATCH /feedback/{id}/status
// func (h *FeedbackHandler) UpdateFeedbackStatus(w http.ResponseWriter, r *http.Request) {
// 	idStr := chi.URLParam(r, "id")
// 	id, err := strconv.ParseInt(idStr, 10, 64)
// 	if err != nil {
// 		h.logr.Warn("invalid feedback ID", zap.String("id", idStr))
// 		writeJSON(w, http.StatusBadRequest, services.FeedbackResponse{
// 			Success: false,
// 			Message: "Invalid feedback ID",
// 		})
// 		return
// 	}

// 	var req services.UpdateFeedbackStatusRequest
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		h.logr.Error("failed to decode request body", zap.Error(err))
// 		writeJSON(w, http.StatusBadRequest, services.FeedbackResponse{
// 			Success: false,
// 			Message: "Invalid request body",
// 		})
// 		return
// 	}

// 	// Validate status
// 	validStatuses := []string{"OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"}
// 	isValid := false
// 	for _, s := range validStatuses {
// 		if req.Status == s {
// 			isValid = true
// 			break
// 		}
// 	}

// 	if !isValid {
// 		h.logr.Warn("invalid status", zap.String("status", req.Status))
// 		writeJSON(w, http.StatusBadRequest, services.FeedbackResponse{
// 			Success: false,
// 			Message: "Status must be one of: OPEN, IN_PROGRESS, RESOLVED, CLOSED",
// 		})
// 		return
// 	}

// 	if err := h.service.UpdateFeedbackStatus(r.Context(), id, req.Status); err != nil {
// 		h.logr.Error("failed to update feedback status", zap.Error(err), zap.Int64("id", id))
// 		writeJSON(w, http.StatusInternalServerError, services.FeedbackResponse{
// 			Success: false,
// 			Message: "Failed to update status",
// 		})
// 		return
// 	}

// 	h.logr.Info("feedback status updated successfully",
// 		zap.Int64("id", id),
// 		zap.String("status", req.Status))

// 	writeJSON(w, http.StatusOK, services.FeedbackResponse{
// 		Success: true,
// 		Message: "Status updated successfully",
// 	})
// }

package handlers

import (
	"bknd-1/internal/models"
	"bknd-1/internal/services"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

type FeedbackHandler struct {
	service *services.FeedbackService
	logr    *zap.Logger
}

func NewFeedbackHandler(svc *services.FeedbackService, logr *zap.Logger) *FeedbackHandler {
	return &FeedbackHandler{service: svc, logr: logr}
}

// CreateFeedback handles POST /api/v1/feedback
func (h *FeedbackHandler) CreateFeedback(w http.ResponseWriter, r *http.Request) {
	var req models.CreateFeedbackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, models.MessageResponse{
			Success: false,
			Message: "Invalid request body",
		})
		return
	}

	// Validate required fields
	if req.Email == "" {
		writeJSON(w, http.StatusBadRequest, models.MessageResponse{
			Success: false,
			Message: "Email is required",
		})
		return
	}

	if req.Comments == "" {
		writeJSON(w, http.StatusBadRequest, models.MessageResponse{
			Success: false,
			Message: "Comments are required",
		})
		return
	}

	// Validate type for top-level comments
	if req.ParentID == nil {
		if req.Type == "" {
			writeJSON(w, http.StatusBadRequest, models.MessageResponse{
				Success: false,
				Message: "Type is required for top-level comments",
			})
			return
		}
		if req.Type != "COMMENT" && req.Type != "COMPLAINT" {
			writeJSON(w, http.StatusBadRequest, models.MessageResponse{
				Success: false,
				Message: "Type must be COMMENT or COMPLAINT",
			})
			return
		}
	}

	if err := h.service.CreateFeedback(r.Context(), &req); err != nil {
		h.logr.Error("failed to create feedback", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, models.MessageResponse{
			Success: false,
			Message: "Failed to create feedback",
		})
		return
	}

	writeJSON(w, http.StatusOK, models.MessageResponse{
		Success: true,
		Message: "Feedback submitted",
	})
}

// GetAllFeedback handles GET /api/v1/feedback
func (h *FeedbackHandler) GetAllFeedback(w http.ResponseWriter, r *http.Request) {
	// Parse pagination parameters
	limit := 100
	offset := 0

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
			offset = o
		}
	}

	feedbacks, total, err := h.service.GetAllFeedback(r.Context(), limit, offset)
	if err != nil {
		h.logr.Error("failed to fetch feedback", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, models.MessageResponse{
			Success: false,
			Message: "Failed to retrieve feedback",
		})
		return
	}

	writeJSON(w, http.StatusOK, models.FeedbackListResponse{
		Success: true,
		Count:   total,
		Limit:   limit,
		Offset:  offset,
		Data:    feedbacks,
	})
}

// GetFeedbackByEmail handles GET /api/v1/feedback/user/{email}
func (h *FeedbackHandler) GetFeedbackByEmail(w http.ResponseWriter, r *http.Request) {
	email := chi.URLParam(r, "email")
	if email == "" {
		writeJSON(w, http.StatusBadRequest, models.MessageResponse{
			Success: false,
			Message: "Email is required",
		})
		return
	}

	feedbacks, err := h.service.GetFeedbackByEmail(r.Context(), email)
	if err != nil {
		h.logr.Error("failed to fetch feedback by email", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, models.MessageResponse{
			Success: false,
			Message: "Failed to retrieve feedback",
		})
		return
	}

	writeJSON(w, http.StatusOK, models.FeedbackListResponse{
		Success: true,
		Count:   len(feedbacks),
		Limit:   100,
		Offset:  0,
		Data:    feedbacks,
	})
}

// DeleteFeedback handles DELETE /api/v1/feedback/{id}
func (h *FeedbackHandler) DeleteFeedback(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, models.MessageResponse{
			Success: false,
			Message: "Invalid feedback ID",
		})
		return
	}

	if err := h.service.DeleteFeedback(r.Context(), id); err != nil {
		h.logr.Error("failed to delete feedback", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, models.MessageResponse{
			Success: false,
			Message: "Failed to delete feedback",
		})
		return
	}

	writeJSON(w, http.StatusOK, models.MessageResponse{
		Success: true,
		Message: "Deleted successfully",
	})
}

// UpdateFeedbackStatus handles PATCH /api/v1/feedback/{id}/status
func (h *FeedbackHandler) UpdateFeedbackStatus(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, models.MessageResponse{
			Success: false,
			Message: "Invalid feedback ID",
		})
		return
	}

	var req models.UpdateStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, models.MessageResponse{
			Success: false,
			Message: "Invalid request body",
		})
		return
	}

	if req.Status == "" {
		writeJSON(w, http.StatusBadRequest, models.MessageResponse{
			Success: false,
			Message: "Status is required",
		})
		return
	}

	// Validate status against enum values
	validStatuses := map[string]bool{
		models.StatusPending:    true,
		models.StatusInProgress: true,
		models.StatusResolved:   true,
	}

	if !validStatuses[req.Status] {
		writeJSON(w, http.StatusBadRequest, models.MessageResponse{
			Success: false,
			Message: "Status must be PENDING, IN_PROGRESS, or RESOLVED",
		})
		return
	}

	feedback, err := h.service.UpdateFeedbackStatus(r.Context(), id, req.Status)
	if err != nil {
		h.logr.Error("failed to update feedback status", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, models.MessageResponse{
			Success: false,
			Message: err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, models.StatusUpdateResponse{
		Success: true,
		Message: "Status updated",
		Data:    feedback,
	})
}

// GetFeedbackByID retrieves a single feedback with its replies
func (h *FeedbackHandler) GetFeedbackByID(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, models.MessageResponse{
			Success: false,
			Message: "Invalid feedback ID",
		})
		return
	}

	feedback, err := h.service.GetFeedbackByID(r.Context(), id)
	if err != nil {
		h.logr.Error("failed to fetch feedback", zap.Error(err))
		writeJSON(w, http.StatusNotFound, models.MessageResponse{
			Success: false,
			Message: "Feedback not found",
		})
		return
	}

	// For JSON response, nil pointers will be omitted or shown as null
	// If you want to omit status for replies completely, you can add omitempty
	writeJSON(w, http.StatusOK, models.FeedbackResponse{
		Success: true,
		Data:    feedback,
	})
}
