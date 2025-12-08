package handlers

import (
	"bknd-1/internal/models"
	"bknd-1/internal/services"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

type ServiceAreaHandler struct {
	service *services.ServiceAreaService
	logr    *zap.Logger
}

func NewServiceAreaHandler(svc *services.ServiceAreaService, logr *zap.Logger) *ServiceAreaHandler {
	return &ServiceAreaHandler{service: svc, logr: logr}
}

// GetServiceAreas returns ECG service areas with geographic boundaries
func (h *ServiceAreaHandler) GetServiceAreas(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()

	// Helper to split CSV parameters
	splitCSV := func(s string) []string {
		if s == "" {
			return nil
		}
		parts := strings.Split(s, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		return parts
	}

	// Build filter params
	params := models.ServiceAreaQueryParams{
		Regions:   splitCSV(q.Get("region")),
		Districts: splitCSV(q.Get("district")),
	}

	// Call service
	response, err := h.service.GetServiceAreas(ctx, params)
	if err != nil {
		h.logr.Error("failed to get service areas", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve service areas",
		})
		return
	}

	writeJSON(w, http.StatusOK, response)
}

// GetServiceAreaByID returns a single service area by ID
func (h *ServiceAreaHandler) GetServiceAreaByID(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	idStr := chi.URLParam(r, "id")

	id, err := strconv.Atoi(idStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid id parameter",
		})
		return
	}

	feature, err := h.service.GetServiceAreaByID(ctx, id)
	if err != nil {
		h.logr.Error("failed to get service area", zap.Error(err), zap.Int("id", id))
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": "service area not found",
		})
		return
	}

	writeJSON(w, http.StatusOK, feature)
}

// GetRegions returns a list of unique regions
func (h *ServiceAreaHandler) GetRegions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	regions, err := h.service.GetUniqueRegions(ctx)
	if err != nil {
		h.logr.Error("failed to get regions", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve regions",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"regions": regions,
		"count":   len(regions),
	})
}

// GetDistricts returns a list of unique districts, optionally filtered by region
func (h *ServiceAreaHandler) GetDistricts(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	region := q.Get("region")

	districts, err := h.service.GetUniqueDistricts(ctx, region)
	if err != nil {
		h.logr.Error("failed to get districts", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve districts",
		})
		return
	}

	response := map[string]interface{}{
		"districts": districts,
		"count":     len(districts),
	}

	if region != "" {
		response["region"] = region
	}

	writeJSON(w, http.StatusOK, response)
}
