package handlers

import (
	"bknd-1/internal/models"
	"bknd-1/internal/services"
	"encoding/json"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type MeterHandler struct {
	service *services.MeterService
	logr    *zap.Logger
}

func NewMeterHandler(svc *services.MeterService, logr *zap.Logger) *MeterHandler {
	return &MeterHandler{service: svc, logr: logr}
}

func (h *MeterHandler) GetMeterByID(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	meter, err := h.service.GetMeterByID(r.Context(), id)
	if err != nil {
		h.logr.Error("failed to fetch meter", zap.Error(err))
		http.Error(w, "meter not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, meter)
}

func (h *MeterHandler) QueryMeters(w http.ResponseWriter, r *http.Request) {
	results, err := h.service.QueryMeters(r.Context(), r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": results})
}

func (h *MeterHandler) GetMeterStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	// --- Validate dates ---
	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateFrom")
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateTo")
		return
	}

	// --- Split helpers ---
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

	// --- Build filter params ---
	params := models.ReadingFilterParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(q.Get("meterNumber")),
		Regions:               splitCSV(q.Get("region")),
		Districts:             splitCSV(q.Get("district")),
		Stations:              splitCSV(q.Get("station")),
		Locations:             splitCSV(q.Get("location")),
		BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
		MeterTypes:            splitCSV(q.Get("meterType")), // ✅ INCLUDED
	}

	// --- Execute service method ---
	results, err := h.service.GetMeterStatus(ctx, params)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, results)
}

func (h *MeterHandler) GetMeterStatusCounts(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	// --- Validate dates ---
	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateFrom")
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateTo")
		return
	}

	// --- CSV splitter ---
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

	// --- Build filter params ---
	params := models.ReadingFilterParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(q.Get("meterNumber")),
		Regions:               splitCSV(q.Get("region")),
		Districts:             splitCSV(q.Get("district")),
		Stations:              splitCSV(q.Get("station")),
		Locations:             splitCSV(q.Get("location")),
		BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
		MeterTypes:            splitCSV(q.Get("meterType")),
	}

	// --- Execute service ---
	result, err := h.service.GetMeterStatusCounts(ctx, params)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *MeterHandler) GetAggregatedReadings(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	params := models.AggregatedQueryParams{
		DateFrom:         q.Get("date_from"),
		DateTo:           q.Get("date_to"),
		Regions:          splitCSV(q.Get("regions")),
		Districts:        splitCSV(q.Get("districts")),
		Stations:         splitCSV(q.Get("stations")),
		Voltages:         parseCSVFloat(q.Get("voltages")),
		Locations:        splitCSV(q.Get("locations")),
		BoundaryPoints:   splitCSV(q.Get("boundary_metering_point")),
		MeterTypes:       splitCSV(q.Get("meterTypes")),
		GroupBy:          q.Get("groupBy"),
		StackByMeterType: parseBool(q.Get("stackByMeterType")),
	}

	result, err := h.service.GetAggregated(r.Context(), &params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *MeterHandler) GetDailyConsumption(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {

		writeJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {

		writeJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	// ✅ Split comma-separated values manually
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

	params := models.ReadingFilterParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(q.Get("meterNumber")),
		Regions:               splitCSV(q.Get("region")),
		Districts:             splitCSV(q.Get("district")),
		Stations:              splitCSV(q.Get("station")),
		Locations:             splitCSV(q.Get("location")),
		BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
		MeterTypes:            splitCSV(q.Get("meterType")),
	}

	results, err := h.service.GetDailyConsumption(ctx, params)
	if err != nil {

		writeJSON(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, results)
}

func (h *MeterHandler) GetRegionalBoundaryDailyConsumption(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {

		writeJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {

		writeJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	// ✅ Split comma-separated values manually
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

	params := models.ReadingFilterParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(q.Get("meterNumber")),
		Regions:               splitCSV(q.Get("region")),
		Districts:             splitCSV(q.Get("district")),
		Stations:              splitCSV(q.Get("station")),
		Locations:             splitCSV(q.Get("location")),
		BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
		MeterTypes:            splitCSV(q.Get("meterType")),
	}

	results, err := h.service.GetRegionalBoundaryDailyConsumption(ctx, params)
	if err != nil {

		writeJSON(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, results)
}

func (h *MeterHandler) GetDistrictBoundaryDailyConsumption(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {

		writeJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {

		writeJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	// ✅ Split comma-separated values manually
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

	params := models.ReadingFilterParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(q.Get("meterNumber")),
		Regions:               splitCSV(q.Get("region")),
		Districts:             splitCSV(q.Get("district")),
		Stations:              splitCSV(q.Get("station")),
		Locations:             splitCSV(q.Get("location")),
		BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
		MeterTypes:            splitCSV(q.Get("meterType")),
	}

	results, err := h.service.GetDistrictBoundaryDailyConsumption(ctx, params)
	if err != nil {

		writeJSON(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, results)
}

func (h *MeterHandler) GetBSPDailyConsumption(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {

		writeJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {

		writeJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	// ✅ Split comma-separated values manually
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

	params := models.ReadingFilterParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(q.Get("meterNumber")),
		Regions:               splitCSV(q.Get("region")),
		Districts:             splitCSV(q.Get("district")),
		Stations:              splitCSV(q.Get("station")),
		Locations:             splitCSV(q.Get("location")),
		BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
		MeterTypes:            splitCSV(q.Get("meterType")),
	}

	results, err := h.service.GetBSPDailyConsumption(ctx, params)
	if err != nil {

		writeJSON(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, results)
}

func (h *MeterHandler) GetFeederAggregatedConsumption(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateFrom")
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateTo")
		return
	}

	// Helper to split comma-separated params
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

	// Parse grouping options
	groupBy := q.Get("groupBy") // e.g. "day", "month", "year"
	if groupBy == "" {
		groupBy = "day"
	}
	additionalGroups := splitCSV(q.Get("group")) // e.g. "region,station"

	// Parse meter types - default to all types if not specified
	meterTypes := splitCSV(q.Get("meterType"))
	if len(meterTypes) == 0 {
		meterTypes = []string{"BSP", "PSS", "SS"} // Default to all types
	}

	params := models.ReadingFilterParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(q.Get("meterNumber")),
		Regions:               splitCSV(q.Get("region")),
		Districts:             splitCSV(q.Get("district")),
		Stations:              splitCSV(q.Get("station")),
		Locations:             splitCSV(q.Get("location")),
		Voltages:              splitCSV(q.Get("voltage_kv")),
		BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
		MeterTypes:            meterTypes,
	}

	results, err := h.service.GetFeederAggregatedConsumption(ctx, params, groupBy, additionalGroups, meterTypes)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, results)
}

func (h *MeterHandler) GetFeederDailyConsumption(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateFrom")
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateTo")
		return
	}

	// Helper to split comma-separated params
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

	// Parse meter types - default to all types if not specified
	meterTypes := splitCSV(q.Get("meterType"))
	if len(meterTypes) == 0 {
		meterTypes = []string{"BSP", "PSS", "SS"} // Default to all types
	}

	params := models.ReadingFilterParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(q.Get("meterNumber")),
		Regions:               splitCSV(q.Get("region")),
		Districts:             splitCSV(q.Get("district")),
		Stations:              splitCSV(q.Get("station")),
		Locations:             splitCSV(q.Get("location")),
		Voltages:              splitCSV(q.Get("voltage_kv")),
		BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
		MeterTypes:            meterTypes,
	}

	results, err := h.service.GetFeederDailyConsumption(ctx, params, meterTypes)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, results)
}

func (h *MeterHandler) GetBSPAggregatedConsumption(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateFrom")
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateTo")
		return
	}

	// Helper to split comma-separated params
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

	// Parse grouping options
	groupBy := q.Get("groupBy") // e.g. "day", "month", "year"
	if groupBy == "" {
		groupBy = "day"
	}
	additionalGroups := splitCSV(q.Get("group")) // e.g. "region,station"

	params := models.ReadingFilterParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(q.Get("meterNumber")),
		Regions:               splitCSV(q.Get("region")),
		Districts:             splitCSV(q.Get("district")),
		Stations:              splitCSV(q.Get("station")),
		Locations:             splitCSV(q.Get("location")),
		Voltages:              splitCSV(q.Get("voltage_kv")),
		BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
		MeterTypes:            splitCSV(q.Get("meterType")),
	}

	results, err := h.service.GetBSPAggregatedConsumption(ctx, params, groupBy, additionalGroups)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, results)
}

func (h *MeterHandler) GetPSSDailyConsumption(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {

		writeJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {

		writeJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	// ✅ Split comma-separated values manually
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

	params := models.ReadingFilterParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(q.Get("meterNumber")),
		Regions:               splitCSV(q.Get("region")),
		Districts:             splitCSV(q.Get("district")),
		Stations:              splitCSV(q.Get("station")),
		Locations:             splitCSV(q.Get("location")),
		BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
		MeterTypes:            splitCSV(q.Get("meterType")),
	}

	results, err := h.service.GetPSSDailyConsumption(ctx, params)
	if err != nil {

		writeJSON(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, results)
}

func (h *MeterHandler) GetPSSAggregatedConsumption(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateFrom")
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateTo")
		return
	}

	// Helper to split comma-separated params
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

	// Parse grouping options
	groupBy := q.Get("groupBy") // e.g. "day", "month", "year"
	if groupBy == "" {
		groupBy = "day"
	}
	additionalGroups := splitCSV(q.Get("group")) // e.g. "region,station"

	params := models.ReadingFilterParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(q.Get("meterNumber")),
		Regions:               splitCSV(q.Get("region")),
		Districts:             splitCSV(q.Get("district")),
		Stations:              splitCSV(q.Get("station")),
		Locations:             splitCSV(q.Get("location")),
		Voltages:              splitCSV(q.Get("voltage_kv")),
		BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
		MeterTypes:            splitCSV(q.Get("meterType")),
	}

	results, err := h.service.GetPSSAggregatedConsumption(ctx, params, groupBy, additionalGroups)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, results)
}

func (h *MeterHandler) GetSSDailyConsumption(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {

		writeJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {

		writeJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	// ✅ Split comma-separated values manually
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

	params := models.ReadingFilterParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(q.Get("meterNumber")),
		Regions:               splitCSV(q.Get("region")),
		Districts:             splitCSV(q.Get("district")),
		Stations:              splitCSV(q.Get("station")),
		Locations:             splitCSV(q.Get("location")),
		BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
		MeterTypes:            splitCSV(q.Get("meterType")),
	}

	results, err := h.service.GetSSDailyConsumption(ctx, params)
	if err != nil {

		writeJSON(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, results)
}

func (h *MeterHandler) GetSSAggregatedConsumption(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateFrom")
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateTo")
		return
	}

	// Helper to split comma-separated params
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

	// Parse grouping options
	groupBy := q.Get("groupBy") // e.g. "day", "month", "year"
	if groupBy == "" {
		groupBy = "day"
	}
	additionalGroups := splitCSV(q.Get("group")) // e.g. "region,station"

	params := models.ReadingFilterParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(q.Get("meterNumber")),
		Regions:               splitCSV(q.Get("region")),
		Districts:             splitCSV(q.Get("district")),
		Stations:              splitCSV(q.Get("station")),
		Locations:             splitCSV(q.Get("location")),
		Voltages:              splitCSV(q.Get("voltage_kv")),
		BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
		MeterTypes:            splitCSV(q.Get("meterType")),
	}

	results, err := h.service.GetSSAggregatedConsumption(ctx, params, groupBy, additionalGroups)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, results)
}

func (h *MeterHandler) GetAggregatedConsumption(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateFrom")
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateTo")
		return
	}

	// Helper to split comma-separated params
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

	// Parse grouping options
	groupBy := q.Get("groupBy") // e.g. "day", "month", "year"
	if groupBy == "" {
		groupBy = "day"
	}
	additionalGroups := splitCSV(q.Get("group")) // e.g. "region,station"

	params := models.ReadingFilterParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(q.Get("meterNumber")),
		Regions:               splitCSV(q.Get("region")),
		Districts:             splitCSV(q.Get("district")),
		Stations:              splitCSV(q.Get("station")),
		Locations:             splitCSV(q.Get("location")),
		Voltages:              splitCSV(q.Get("voltage_kv")),
		BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
		MeterTypes:            splitCSV(q.Get("meterType")),
	}

	results, err := h.service.GetAggregatedConsumption(ctx, params, groupBy, additionalGroups)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, results)
}

func (h *MeterHandler) GetDTXDailyConsumption(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {

		writeJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {

		writeJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	// ✅ Split comma-separated values manually
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

	params := models.ReadingFilterParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(q.Get("meterNumber")),
		Regions:               splitCSV(q.Get("region")),
		Districts:             splitCSV(q.Get("district")),
		Stations:              splitCSV(q.Get("station")),
		Locations:             splitCSV(q.Get("location")),
		BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
		MeterTypes:            splitCSV(q.Get("meterType")),
	}

	results, err := h.service.GetDTXDailyConsumption(ctx, params)
	if err != nil {

		writeJSON(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, results)
}

func (h *MeterHandler) GetDTXAggregatedConsumption(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateFrom")
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateTo")
		return
	}

	// Helper to split comma-separated params
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

	// Parse grouping options
	groupBy := q.Get("groupBy") // e.g. "day", "month", "year"
	if groupBy == "" {
		groupBy = "day"
	}
	additionalGroups := splitCSV(q.Get("group")) // e.g. "region,station"

	params := models.ReadingFilterParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(q.Get("meterNumber")),
		Regions:               splitCSV(q.Get("region")),
		Districts:             splitCSV(q.Get("district")),
		Stations:              splitCSV(q.Get("station")),
		Locations:             splitCSV(q.Get("location")),
		Voltages:              splitCSV(q.Get("voltage_kv")),
		BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
		MeterTypes:            splitCSV(q.Get("meterType")),
	}

	results, err := h.service.GetDTXAggregatedConsumption(ctx, params, groupBy, additionalGroups)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, results)
}

func (h *MeterHandler) GetRegionalBoundaryAggregatedConsumption(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateFrom")
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateTo")
		return
	}

	// Helper to split comma-separated params
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

	// Parse grouping options
	groupBy := q.Get("groupBy") // e.g. "day", "month", "year"
	if groupBy == "" {
		groupBy = "day"
	}
	additionalGroups := splitCSV(q.Get("group")) // e.g. "region,station"

	params := models.ReadingFilterParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(q.Get("meterNumber")),
		Regions:               splitCSV(q.Get("region")),
		Districts:             splitCSV(q.Get("district")),
		Stations:              splitCSV(q.Get("station")),
		Locations:             splitCSV(q.Get("location")),
		Voltages:              splitCSV(q.Get("voltage_kv")),
		BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
		MeterTypes:            splitCSV(q.Get("meterType")),
	}

	results, err := h.service.GetRegionalBoundaryAggregatedConsumption(ctx, params, groupBy, additionalGroups)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, results)
}

func (h *MeterHandler) GetDistrictBoundaryAggregatedConsumption(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateFrom")
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, "invalid dateTo")
		return
	}

	// Helper to split comma-separated params
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

	// Parse grouping options
	groupBy := q.Get("groupBy") // e.g. "day", "month", "year"
	if groupBy == "" {
		groupBy = "day"
	}
	additionalGroups := splitCSV(q.Get("group")) // e.g. "region,station"

	params := models.ReadingFilterParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(q.Get("meterNumber")),
		Regions:               splitCSV(q.Get("region")),
		Districts:             splitCSV(q.Get("district")),
		Stations:              splitCSV(q.Get("station")),
		Locations:             splitCSV(q.Get("location")),
		Voltages:              splitCSV(q.Get("voltage_kv")),
		BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
		MeterTypes:            splitCSV(q.Get("meterType")),
	}

	results, err := h.service.GetDistrictBoundaryAggregatedConsumption(ctx, params, groupBy, additionalGroups)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, results)
}

// --- helper functions ---

func splitCSV(input string) []string {
	if input == "" {
		return nil
	}
	parts := strings.Split(input, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func parseCSVFloat(input string) []float64 {
	if input == "" {
		return nil
	}
	parts := strings.Split(input, ",")
	var result []float64
	for _, p := range parts {
		if f, err := strconv.ParseFloat(strings.TrimSpace(p), 64); err == nil {
			result = append(result, f)
		}
	}
	return result
}

func parseBool(input string) bool {
	input = strings.ToLower(strings.TrimSpace(input))
	return input == "1" || input == "true"
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if data == nil {
		return
	}

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(true)
	_ = enc.Encode(data)
}
