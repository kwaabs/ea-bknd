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

// GetMetersWithServiceArea returns meters with spatial service area assignment
func (h *MeterHandler) GetMetersWithServiceArea(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()

	// Parse pagination
	page, _ := strconv.Atoi(q.Get("page"))
	if page < 1 {
		page = 1
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 50
	}

	// Helper to split CSV
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

	// Parse hasCoordinates filter
	var hasCoordinates *bool
	if coordStr := q.Get("hasCoordinates"); coordStr != "" {
		val := strings.ToLower(coordStr) == "true" || coordStr == "1"
		hasCoordinates = &val
	}

	params := models.MeterSpatialJoinParams{
		Page:              page,
		Limit:             limit,
		MeterTypes:        splitCSV(q.Get("meterType")),
		Regions:           splitCSV(q.Get("region")),
		Districts:         splitCSV(q.Get("district")),
		ServiceAreaRegion: splitCSV(q.Get("serviceAreaRegion")),
		HasCoordinates:    hasCoordinates,
		Search:            q.Get("search"),
		SortBy:            q.Get("sortBy"),
		SortOrder:         q.Get("sortOrder"),
	}

	result, err := h.service.GetMetersWithServiceArea(ctx, params)
	if err != nil {
		h.logr.Error("failed to get meters with service area", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve meters",
		})
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// GetMeterSpatialMismatch returns meters with region/district mismatches
func (h *MeterHandler) GetMeterSpatialMismatch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()

	page, _ := strconv.Atoi(q.Get("page"))
	if page < 1 {
		page = 1
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 50
	}

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

	params := models.MeterSpatialJoinParams{
		Page:       page,
		Limit:      limit,
		MeterTypes: splitCSV(q.Get("meterType")),
		Search:     q.Get("search"),
	}

	result, err := h.service.GetMeterSpatialMismatch(ctx, params)
	if err != nil {
		h.logr.Error("failed to get meter spatial mismatches", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve mismatches",
		})
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// GetMeterSpatialStats returns spatial assignment statistics
func (h *MeterHandler) GetMeterSpatialStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	stats, err := h.service.GetMeterSpatialStats(ctx)
	if err != nil {
		h.logr.Error("failed to get meter spatial stats", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve statistics",
		})
		return
	}

	writeJSON(w, http.StatusOK, stats)
}

// GetMeterSpatialCounts returns aggregated meter counts by service area
func (h *MeterHandler) GetMeterSpatialCounts(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()

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

	params := models.MeterSpatialCountParams{
		GroupBy:    q.Get("groupBy"), // region, district, meter_type, region_meter_type, district_meter_type
		MeterTypes: splitCSV(q.Get("meterType")),
		Regions:    splitCSV(q.Get("region")),
		Districts:  splitCSV(q.Get("district")),
	}

	// Default to region if not specified
	if params.GroupBy == "" {
		params.GroupBy = "region"
	}

	result, err := h.service.GetMeterSpatialCounts(ctx, params)
	if err != nil {
		h.logr.Error("failed to get meter spatial counts", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve counts",
		})
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// GetMeterSpatialCountsByRegion returns counts grouped by region
func (h *MeterHandler) GetMeterSpatialCountsByRegion(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()

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

	result, err := h.service.GetMeterSpatialCountsByRegion(ctx, splitCSV(q.Get("meterType")))
	if err != nil {
		h.logr.Error("failed to get counts by region", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve counts",
		})
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// GetMeterSpatialCountsByDistrict returns counts grouped by district
func (h *MeterHandler) GetMeterSpatialCountsByDistrict(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()

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

	result, err := h.service.GetMeterSpatialCountsByDistrict(
		ctx,
		q.Get("region"),
		splitCSV(q.Get("meterType")),
	)
	if err != nil {
		h.logr.Error("failed to get counts by district", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve counts",
		})
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// GetMeterSpatialCountsByType returns counts grouped by meter type
func (h *MeterHandler) GetMeterSpatialCountsByType(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	result, err := h.service.GetMeterSpatialCountsByType(ctx)
	if err != nil {
		h.logr.Error("failed to get counts by type", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve counts",
		})
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// GetTopBottomConsumers handles GET /api/meters/top-bottom-consumers
func (h *MeterHandler) GetTopBottomConsumers(w http.ResponseWriter, r *http.Request) {
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
	result, err := h.service.GetTopBottomConsumers(ctx, params)
	if err != nil {
		h.logr.Error("failed to get top/bottom consumers", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve top/bottom consumers",
		})
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// GetMeterHealthSummary handles GET /api/v1/meters/health/summary
func (h *MeterHandler) GetMeterHealthSummary(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	// Parse and validate dates
	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "invalid dateFrom parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "invalid dateTo parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	// Validate date range
	if dateTo.Before(dateFrom) {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "dateTo must be after dateFrom",
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
	summary, err := h.service.GetMeterHealthSummary(ctx, params)
	if err != nil {
		h.logr.Error("failed to get meter health summary", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "failed to retrieve meter health summary",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"data":    summary,
	})
}

// GetMeterHealthDetails handles GET /api/v1/meters/health/details
func (h *MeterHandler) GetMeterHealthDetails(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	// Parse and validate dates
	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "invalid dateFrom parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "invalid dateTo parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	// Validate date range
	if dateTo.Before(dateFrom) {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "dateTo must be after dateFrom",
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

	// Parse and validate health category
	healthCategory := strings.ToLower(strings.TrimSpace(q.Get("healthCategory")))
	validCategories := map[string]bool{
		"excellent": true,
		"good":      true,
		"poor":      true,
		"critical":  true,
		"online":    true,
		"offline":   true,
		"":          true, // Allow empty for all
	}
	if !validCategories[healthCategory] {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "invalid healthCategory parameter, must be one of: excellent, good, poor, critical, online, offline",
		})
		return
	}

	// Parse sorting parameters
	sortBy := q.Get("sortBy")
	sortOrder := strings.ToLower(q.Get("sortOrder"))
	if sortOrder == "" {
		sortOrder = "desc"
	}

	// Validate sortBy
	validSortFields := map[string]bool{
		"meter_number": true,
		"uptime":       true,
		"meter_type":   true,
		"last_seen":    true,
		"consumption":  true,
		"":             true, // default
	}
	if !validSortFields[sortBy] {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "invalid sortBy parameter, must be one of: meter_number, uptime, meter_type, last_seen, consumption",
		})
		return
	}

	// Validate sortOrder
	if sortOrder != "asc" && sortOrder != "desc" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "invalid sortOrder parameter, must be 'asc' or 'desc'",
		})
		return
	}

	// Build filter params
	params := models.MeterHealthDetailParams{
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
		Page:           page,
		Limit:          limit,
		Search:         q.Get("search"),
		HealthCategory: healthCategory,
		SortBy:         sortBy,
		SortOrder:      sortOrder,
	}

	// Call service
	details, err := h.service.GetMeterHealthDetails(ctx, params)
	if err != nil {
		h.logr.Error("failed to get meter health details", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "failed to retrieve meter health details",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"data":    details,
	})
}

// GetRegions returns a list of unique regions from meters table
func (h *MeterHandler) GetRegions(w http.ResponseWriter, r *http.Request) {
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

	// Optional meter type filter
	meterTypes := splitCSV(q.Get("meterType"))

	regions, err := h.service.GetUniqueRegions(ctx, meterTypes)
	if err != nil {
		h.logr.Error("failed to get regions", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve regions",
		})
		return
	}

	response := map[string]interface{}{
		"regions": regions,
		"count":   len(regions),
	}

	if len(meterTypes) > 0 {
		response["filters"] = map[string]interface{}{
			"meterTypes": meterTypes,
		}
	}

	writeJSON(w, http.StatusOK, response)
}

// GetDistricts returns a list of unique districts from meters table
func (h *MeterHandler) GetDistricts(w http.ResponseWriter, r *http.Request) {
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

	region := q.Get("region")
	meterTypes := splitCSV(q.Get("meterType"))

	districts, err := h.service.GetUniqueDistricts(ctx, region, meterTypes)
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

	filters := map[string]interface{}{}
	if region != "" {
		filters["region"] = region
	}
	if len(meterTypes) > 0 {
		filters["meterTypes"] = meterTypes
	}
	if len(filters) > 0 {
		response["filters"] = filters
	}

	writeJSON(w, http.StatusOK, response)
}

// GetStations returns a list of unique stations from meters table
func (h *MeterHandler) GetStations(w http.ResponseWriter, r *http.Request) {
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

	region := q.Get("region")
	district := q.Get("district")
	meterTypes := splitCSV(q.Get("meterType"))

	stations, err := h.service.GetUniqueStations(ctx, region, district, meterTypes)
	if err != nil {
		h.logr.Error("failed to get stations", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve stations",
		})
		return
	}

	response := map[string]interface{}{
		"stations": stations,
		"count":    len(stations),
	}

	filters := map[string]interface{}{}
	if region != "" {
		filters["region"] = region
	}
	if district != "" {
		filters["district"] = district
	}
	if len(meterTypes) > 0 {
		filters["meterTypes"] = meterTypes
	}
	if len(filters) > 0 {
		response["filters"] = filters
	}

	writeJSON(w, http.StatusOK, response)
}

// GetLocations returns a list of unique locations from meters table
func (h *MeterHandler) GetLocations(w http.ResponseWriter, r *http.Request) {
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

	region := q.Get("region")
	district := q.Get("district")
	meterTypes := splitCSV(q.Get("meterType"))

	locations, err := h.service.GetUniqueLocations(ctx, region, district, meterTypes)
	if err != nil {
		h.logr.Error("failed to get locations", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve locations",
		})
		return
	}

	response := map[string]interface{}{
		"locations": locations,
		"count":     len(locations),
	}

	filters := map[string]interface{}{}
	if region != "" {
		filters["region"] = region
	}
	if district != "" {
		filters["district"] = district
	}
	if len(meterTypes) > 0 {
		filters["meterTypes"] = meterTypes
	}
	if len(filters) > 0 {
		response["filters"] = filters
	}

	writeJSON(w, http.StatusOK, response)
}

// GetBoundaryPoints returns a list of unique boundary metering points
func (h *MeterHandler) GetBoundaryPoints(w http.ResponseWriter, r *http.Request) {
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

	region := q.Get("region")
	district := q.Get("district")
	meterTypes := splitCSV(q.Get("meterType"))

	boundaryPoints, err := h.service.GetUniqueBoundaryPoints(ctx, region, district, meterTypes)
	if err != nil {
		h.logr.Error("failed to get boundary points", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve boundary points",
		})
		return
	}

	response := map[string]interface{}{
		"boundaryPoints": boundaryPoints,
		"count":          len(boundaryPoints),
	}

	filters := map[string]interface{}{}
	if region != "" {
		filters["region"] = region
	}
	if district != "" {
		filters["district"] = district
	}
	if len(meterTypes) > 0 {
		filters["meterTypes"] = meterTypes
	}
	if len(filters) > 0 {
		response["filters"] = filters
	}

	writeJSON(w, http.StatusOK, response)
}

// GetVoltages returns a list of unique voltage levels
func (h *MeterHandler) GetVoltages(w http.ResponseWriter, r *http.Request) {
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

	region := q.Get("region")
	district := q.Get("district")
	meterTypes := splitCSV(q.Get("meterType"))

	voltages, err := h.service.GetUniqueVoltages(ctx, region, district, meterTypes)
	if err != nil {
		h.logr.Error("failed to get voltages", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve voltages",
		})
		return
	}

	response := map[string]interface{}{
		"voltages": voltages,
		"count":    len(voltages),
	}

	filters := map[string]interface{}{}
	if region != "" {
		filters["region"] = region
	}
	if district != "" {
		filters["district"] = district
	}
	if len(meterTypes) > 0 {
		filters["meterTypes"] = meterTypes
	}
	if len(filters) > 0 {
		response["filters"] = filters
	}

	writeJSON(w, http.StatusOK, response)
}

// GetRegionalMapConsumption handles GET /api/v1/meters/consumption/regional-map
func (h *MeterHandler) GetRegionalMapConsumption(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	// Parse and validate dates
	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "invalid dateFrom parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "invalid dateTo parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	// Validate date range
	if dateTo.Before(dateFrom) {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "dateTo must be after dateFrom",
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
	params := models.RegionalMapParams{
		DateFrom:  dateFrom,
		DateTo:    dateTo,
		MeterType: splitCSV(q.Get("meterType")),
		Region:    q.Get("region"),
		District:  q.Get("district"),
		Location:  q.Get("location"),
	}

	// Call service
	result, err := h.service.GetRegionalMapConsumption(ctx, params)
	if err != nil {
		h.logr.Error("failed to get regional map consumption", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "failed to retrieve regional map data",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"data":    result,
	})
}

// GetDistrictGeometries handles GET /api/v1/meters/geometries/districts
func (h *MeterHandler) GetDistrictGeometries(w http.ResponseWriter, r *http.Request) {
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

	regions := splitCSV(q.Get("region"))
	districts := splitCSV(q.Get("district"))

	// Call service
	geometries, err := h.service.GetDistrictGeometries(ctx, regions, districts)
	if err != nil {
		h.logr.Error("failed to get district geometries", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "failed to retrieve district geometries",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"version": geometries.Version,
		"data":    geometries,
	})
}

// GetDistrictTimeseriesConsumption handles GET /api/v1/meters/consumption/districts-timeseries
func (h *MeterHandler) GetDistrictTimeseriesConsumption(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	// Parse and validate dates
	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "invalid dateFrom parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "invalid dateTo parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	// Validate date range
	if dateTo.Before(dateFrom) {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "dateTo must be after dateFrom",
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
	params := models.DistrictConsumptionParams{
		DateFrom:  dateFrom,
		DateTo:    dateTo,
		MeterType: splitCSV(q.Get("meterType")),
		Region:    splitCSV(q.Get("region")),
		District:  splitCSV(q.Get("district")),
	}

	// Call service
	timeseries, err := h.service.GetDistrictTimeseriesConsumption(ctx, params)
	if err != nil {
		h.logr.Error("failed to get district timeseries consumption", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "failed to retrieve district timeseries",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"data":    timeseries,
	})
}

// GetRegionalEnergyBalance handles GET /api/energy-balance/regional
// Updated handler with ALL filters parsed (like GetMeterStatus, GetDailyConsumption)

// GetRegionalEnergyBalance handles GET /api/energy-balance/regional
// Supports both singular and plural parameter names (e.g., region OR regions)
func (h *MeterHandler) GetRegionalEnergyBalance(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	// Parse and validate dates
	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "invalid dateFrom parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "invalid dateTo parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	// Validate date range
	if dateTo.Before(dateFrom) {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "dateTo must be after dateFrom",
		})
		return
	}

	// Helper to get parameter value, trying both singular and plural forms
	getParam := func(singular, plural string) string {
		// Try singular first
		if val := q.Get(singular); val != "" {
			return val
		}
		// Try plural
		if val := q.Get(plural); val != "" {
			return val
		}
		return ""
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

	// Helper to parse voltages
	parseVoltages := func(s string) []float64 {
		if s == "" {
			return nil
		}
		parts := strings.Split(s, ",")
		var voltages []float64
		for _, p := range parts {
			if v, err := strconv.ParseFloat(strings.TrimSpace(p), 64); err == nil {
				voltages = append(voltages, v)
			}
		}
		return voltages
	}

	// Parse ALL filter parameters (supporting both singular and plural)
	params := models.EnergyBalanceParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(getParam("meterNumber", "meterNumbers")),
		Regions:               splitCSV(getParam("region", "regions")),
		Districts:             splitCSV(getParam("district", "districts")),
		Stations:              splitCSV(getParam("station", "stations")),
		Locations:             splitCSV(getParam("location", "locations")),
		BoundaryMeteringPoint: splitCSV(getParam("boundaryMeteringPoint", "boundaryMeteringPoints")),
		MeterTypes:            splitCSV(getParam("meterType", "meterTypes")),
		Voltages:              parseVoltages(getParam("voltage", "voltages")),
	}

	// Call service
	response, err := h.service.GetRegionalEnergyBalance(ctx, params)
	if err != nil {
		h.logr.Error("failed to get regional energy balance", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "failed to calculate energy balance",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"data":    response.Data,
		"summary": response.Summary,
	})
}

// GetRegionalEnergyBalanceSummary handles GET /api/energy-balance/regional/summary
// Supports both singular and plural parameter names
func (h *MeterHandler) GetRegionalEnergyBalanceSummary(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	layout := "2006-01-02"

	// Parse and validate dates
	dateFrom, err := time.Parse(layout, q.Get("dateFrom"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "invalid dateFrom parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	dateTo, err := time.Parse(layout, q.Get("dateTo"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "invalid dateTo parameter, expected format: YYYY-MM-DD",
		})
		return
	}

	// Validate date range
	if dateTo.Before(dateFrom) {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "dateTo must be after dateFrom",
		})
		return
	}

	// Helper to get parameter value, trying both singular and plural forms
	getParam := func(singular, plural string) string {
		// Try singular first
		if val := q.Get(singular); val != "" {
			return val
		}
		// Try plural
		if val := q.Get(plural); val != "" {
			return val
		}
		return ""
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

	// Helper to parse voltages
	parseVoltages := func(s string) []float64 {
		if s == "" {
			return nil
		}
		parts := strings.Split(s, ",")
		var voltages []float64
		for _, p := range parts {
			if v, err := strconv.ParseFloat(strings.TrimSpace(p), 64); err == nil {
				voltages = append(voltages, v)
			}
		}
		return voltages
	}

	// Parse ALL filter parameters (supporting both singular and plural)
	params := models.EnergyBalanceParams{
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		MeterNumber:           splitCSV(getParam("meterNumber", "meterNumbers")),
		Regions:               splitCSV(getParam("region", "regions")),
		Districts:             splitCSV(getParam("district", "districts")),
		Stations:              splitCSV(getParam("station", "stations")),
		Locations:             splitCSV(getParam("location", "locations")),
		BoundaryMeteringPoint: splitCSV(getParam("boundaryMeteringPoint", "boundaryMeteringPoints")),
		MeterTypes:            splitCSV(getParam("meterType", "meterTypes")),
		Voltages:              parseVoltages(getParam("voltage", "voltages")),
	}

	// Call service
	summaries, err := h.service.GetRegionalEnergyBalanceSummary(ctx, params)
	if err != nil {
		h.logr.Error("failed to get regional energy balance summary", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "failed to calculate summary",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"data":    summaries,
	})
}

// GetRegionGeometries handles GET /api/regions/geometries
func (h *MeterHandler) GetRegionGeometries(w http.ResponseWriter, r *http.Request) {
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

	// Parse region filter (optional)
	regions := splitCSV(q.Get("regions"))
	// Also support singular form
	if len(regions) == 0 {
		regions = splitCSV(q.Get("region"))
	}

	// Call service
	response, err := h.service.GetRegionGeometries(ctx, regions)
	if err != nil {
		h.logr.Error("failed to get region geometries", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   "failed to retrieve region geometries",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"data":    response,
	})
}


// // Add these handlers to your MeterHandler struct in handlers/meter_handler.go

// // GetRegionMetadata returns comprehensive metadata for a specific region
// // GET /api/v1/regions/{region}/metadata
// func (h *MeterHandler) GetRegionMetadata(w http.ResponseWriter, r *http.Request) {
// 	ctx := r.Context()

// 	// Get region from URL parameter
// 	region := chi.URLParam(r, "region")

// 	if region == "" {
// 		writeJSON(w, http.StatusBadRequest, map[string]string{
// 			"error": "region parameter is required",
// 		})
// 		return
// 	}

// 	metadata, err := h.service.GetRegionMetadata(ctx, region)
// 	if err != nil {
// 		h.logr.Error("failed to get region metadata", zap.Error(err))
// 		writeJSON(w, http.StatusInternalServerError, map[string]string{
// 			"error": "failed to retrieve region metadata",
// 		})
// 		return
// 	}

// 	writeJSON(w, http.StatusOK, map[string]interface{}{
// 		"success": true,
// 		"data":    metadata,
// 	})
// }

// // GetAllRegionsMetadata returns metadata for all regions
// // GET /api/v1/regions/metadata
// func (h *MeterHandler) GetAllRegionsMetadata(w http.ResponseWriter, r *http.Request) {
// 	ctx := r.Context()

// 	metadata, err := h.service.GetAllRegionsMetadata(ctx)
// 	if err != nil {
// 		h.logr.Error("failed to get all regions metadata", zap.Error(err))
// 		writeJSON(w, http.StatusInternalServerError, map[string]string{
// 			"error": "failed to retrieve regions metadata",
// 		})
// 		return
// 	}

// 	writeJSON(w, http.StatusOK, map[string]interface{}{
// 		"success": true,
// 		"data": map[string]interface{}{
// 			"regions": metadata,
// 			"total":   len(metadata),
// 		},
// 	})
// }

// // GetRegionDistrictMetadata returns metadata for a specific district in a region
// // GET /api/v1/regions/{region}/districts/{district}/metadata
// func (h *MeterHandler) GetRegionDistrictMetadata(w http.ResponseWriter, r *http.Request) {
// 	ctx := r.Context()

// 	// Get URL parameters
// 	region := chi.URLParam(r, "region")
// 	district := chi.URLParam(r, "district")

// 	if region == "" || district == "" {
// 		writeJSON(w, http.StatusBadRequest, map[string]string{
// 			"error": "both region and district parameters are required",
// 		})
// 		return
// 	}

// 	metadata, err := h.service.GetRegionDistrictMetadata(ctx, region, district)
// 	if err != nil {
// 		h.logr.Error("failed to get district metadata", zap.Error(err))
// 		writeJSON(w, http.StatusInternalServerError, map[string]string{
// 			"error": "failed to retrieve district metadata",
// 		})
// 		return
// 	}

// 	writeJSON(w, http.StatusOK, map[string]interface{}{
// 		"success": true,
// 		"data":    metadata,
// 	})
// }

// Example route registration in your router setup:
// r.Get("/regions/metadata", meterHandler.GetAllRegionsMetadata)
// r.Get("/regions/{region}/metadata", meterHandler.GetRegionMetadata)
// r.Get("/regions/{region}/districts/{district}/metadata", meterHandler.GetRegionDistrictMetadata)


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
