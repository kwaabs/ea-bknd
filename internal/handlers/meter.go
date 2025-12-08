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

// GetMeterStatusSummary returns aggregated status counts and metrics for summary cards
func (h *MeterHandler) GetMeterStatusSummary(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	// Parse and validate dates
	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid dateFrom parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid dateTo parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	// Validate date range
	if dateTo.Before(dateFrom) {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "dateTo must be after dateFrom",
		})
		return
	}

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
		Voltages:              splitCSV(q.Get("voltage_kv")),
	}

	// Call service
	summary, err := h.service.GetMeterStatusSummary(ctx, params)
	if err != nil {
		h.logr.Error("failed to get meter status summary", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve status summary",
		})
		return
	}

	writeJSON(w, http.StatusOK, summary)
}

// GetMeterStatusTimeline returns daily online/offline counts for timeline charts
func (h *MeterHandler) GetMeterStatusTimeline(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	// Parse and validate dates
	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid dateFrom parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid dateTo parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	// Validate date range
	if dateTo.Before(dateFrom) {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "dateTo must be after dateFrom",
		})
		return
	}

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
		Voltages:              splitCSV(q.Get("voltage_kv")),
	}

	// Call service
	timeline, err := h.service.GetMeterStatusTimeline(ctx, params)
	if err != nil {
		h.logr.Error("failed to get meter status timeline", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve status timeline",
		})
		return
	}

	writeJSON(w, http.StatusOK, timeline)
}

// GetMeterStatusDetails returns paginated meter status details with sorting and filtering
func (h *MeterHandler) GetMeterStatusDetails(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	// Parse and validate dates
	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid dateFrom parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid dateTo parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	// Validate date range
	if dateTo.Before(dateFrom) {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "dateTo must be after dateFrom",
		})
		return
	}

	// Parse pagination parameters
	page := 1
	if pageStr := q.Get("page"); pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
			page = p
		}
	}

	limit := 50
	if limitStr := q.Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 200 {
			limit = l
		}
	}

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

	// Parse sorting parameters
	sortBy := q.Get("sortBy")
	sortOrder := strings.ToLower(q.Get("sortOrder")) // ✅ Also make sortOrder case-insensitive
	if sortOrder == "" {
		sortOrder = "desc"
	}

	// Validate sortBy
	validSortFields := map[string]bool{
		"meter_number": true,
		"uptime":       true,
		"consumption":  true,
		"":             true, // default
	}
	if !validSortFields[sortBy] {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid sortBy parameter, must be one of: meter_number, uptime, consumption",
		})
		return
	}

	// Validate sortOrder
	if sortOrder != "asc" && sortOrder != "desc" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid sortOrder parameter, must be 'asc' or 'desc'",
		})
		return
	}

	// ✅ Parse status filter (case-insensitive)
	status := strings.ToUpper(strings.TrimSpace(q.Get("status")))
	if status != "" && status != "ONLINE" && status != "OFFLINE" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid status parameter, must be 'ONLINE' or 'OFFLINE' (case-insensitive)",
		})
		return
	}

	// Build filter params
	params := models.StatusDetailQueryParams{
		ReadingFilterParams: models.ReadingFilterParams{
			DateFrom:              dateFrom,
			DateTo:                dateTo,
			MeterNumber:           splitCSV(q.Get("meterNumber")),
			Regions:               splitCSV(q.Get("region")),
			Districts:             splitCSV(q.Get("district")),
			Stations:              splitCSV(q.Get("station")),
			Locations:             splitCSV(q.Get("location")),
			BoundaryMeteringPoint: splitCSV(q.Get("boundaryMeteringPoint")),
			MeterTypes:            splitCSV(q.Get("meterType")),
			Voltages:              splitCSV(q.Get("voltage_kv")),
		},
		Page:      page,
		Limit:     limit,
		Search:    q.Get("search"),
		Status:    status, // ✅ Now uppercase
		SortBy:    sortBy,
		SortOrder: sortOrder,
	}

	// Call service
	details, err := h.service.GetMeterStatusDetails(ctx, params)
	if err != nil {
		h.logr.Error("failed to get meter status details", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve status details",
		})
		return
	}

	writeJSON(w, http.StatusOK, details)
}

// GetConsumptionByRegion returns consumption aggregated by region over time
func (h *MeterHandler) GetConsumptionByRegion(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	// Parse and validate dates
	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid dateFrom parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid dateTo parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	// Validate date range
	if dateTo.Before(dateFrom) {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "dateTo must be after dateFrom",
		})
		return
	}

	// Parse and validate groupBy parameter
	groupBy := q.Get("groupBy")
	if groupBy == "" {
		groupBy = "day"
	}

	validGroupBy := map[string]bool{
		"day":   true,
		"week":  true,
		"month": true,
		"year":  true,
	}
	if !validGroupBy[groupBy] {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid groupBy parameter, must be one of: day, week, month, year",
		})
		return
	}

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
		Voltages:              splitCSV(q.Get("voltage_kv")),
	}

	// Call service
	consumption, err := h.service.GetConsumptionByRegion(ctx, params, groupBy)
	if err != nil {
		h.logr.Error("failed to get consumption by region", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve consumption data",
		})
		return
	}

	writeJSON(w, http.StatusOK, consumption)
}

// GetMeterHealthMetrics returns health breakdown and metrics
func (h *MeterHandler) GetMeterHealthMetrics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	// Parse and validate dates
	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid dateFrom parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid dateTo parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	// Validate date range
	if dateTo.Before(dateFrom) {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "dateTo must be after dateFrom",
		})
		return
	}

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
		Voltages:              splitCSV(q.Get("voltage_kv")),
	}

	// Call service
	health, err := h.service.GetMeterHealthMetrics(ctx, params)
	if err != nil {
		h.logr.Error("failed to get meter health metrics", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve health metrics",
		})
		return
	}

	writeJSON(w, http.StatusOK, health)
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
