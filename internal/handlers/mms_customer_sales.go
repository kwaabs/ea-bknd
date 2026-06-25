package handlers

import (
	"bknd-1/internal/models"
	"bknd-1/internal/services"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go.uber.org/zap"
)

type MMSCustomerSalesHandler struct {
	service *services.MMSCustomerSalesService
	logr    *zap.Logger
}

func NewMMSCustomerSalesHandler(svc *services.MMSCustomerSalesService, logr *zap.Logger) *MMSCustomerSalesHandler {
	return &MMSCustomerSalesHandler{service: svc, logr: logr}
}

func (h *MMSCustomerSalesHandler) GetDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()

	page := 1
	if p := q.Get("page"); p != "" {
		if v, err := strconv.Atoi(p); err == nil && v > 0 {
			page = v
		}
	}

	limit := 50
	if l := q.Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 500 {
			limit = v
		}
	}

	dateTimeFrom, err := parseMMSDate(q, "dateFrom", "dateTimeFrom")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateFrom")
		return
	}
	dateTimeTo, err := parseMMSDate(q, "dateTo", "dateTimeTo")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateTo")
		return
	}

	params := models.MMSCustomerSalesFilterParams{
		Region:        splitCSV(q.Get("region")),
		District:      splitCSV(q.Get("district")),
		ContractType:  splitCSV(q.Get("contractType")),
		Tariff:        splitCSV(q.Get("tariff")),
		Manufacturer:  splitCSV(q.Get("manufacturer")),
		Model:         splitCSV(q.Get("model")),
		AccountNumber: splitCSV(q.Get("accountNumber")),
		MeterNumber:   splitCSV(q.Get("meterNumber")),
		Search:        q.Get("search"),
		DateTimeFrom:  dateTimeFrom,
		DateTimeTo:    dateTimeTo,
		Page:          page,
		Limit:         limit,
	}

	result, err := h.service.GetDetail(ctx, params)
	if err != nil {
		h.logr.Error("failed to get mms customer sales detail", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *MMSCustomerSalesHandler) GetAggregate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()

	dateTimeFrom, err := parseMMSDate(q, "dateFrom", "dateTimeFrom")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateFrom")
		return
	}
	dateTimeTo, err := parseMMSDate(q, "dateTo", "dateTimeTo")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateTo")
		return
	}

	groupBy := splitCSV(q.Get("groupBy"))
	if len(groupBy) == 0 {
		groupBy = []string{"region"}
	}

	params := models.MMSCustomerSalesFilterParams{
		Region:        splitCSV(q.Get("region")),
		District:      splitCSV(q.Get("district")),
		ContractType:  splitCSV(q.Get("contractType")),
		Tariff:        splitCSV(q.Get("tariff")),
		Manufacturer:  splitCSV(q.Get("manufacturer")),
		Model:         splitCSV(q.Get("model")),
		AccountNumber: splitCSV(q.Get("accountNumber")),
		MeterNumber:   splitCSV(q.Get("meterNumber")),
		Search:        q.Get("search"),
		DateTimeFrom:  dateTimeFrom,
		DateTimeTo:    dateTimeTo,
	}

	result, err := h.service.GetAggregate(ctx, params, groupBy)
	if err != nil {
		h.logr.Error("failed to get mms customer sales aggregate", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// parseMMSDate reads a YYYY-MM-DD date from the primary param name, falling back
// to an alias (kept for backward compatibility). Empty returns a zero time.
func parseMMSDate(q url.Values, primary, alias string) (time.Time, error) {
	v := q.Get(primary)
	if v == "" {
		v = q.Get(alias)
	}
	if v == "" {
		return time.Time{}, nil
	}
	return time.Parse("2006-01-02", v)
}
