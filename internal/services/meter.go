package services

import (
	"bknd-1/internal/models"
	"context"
	"fmt"
	"github.com/uptrace/bun"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type MeterService struct {
	db *bun.DB
}

func NewMeterService(db *bun.DB) *MeterService {
	return &MeterService{db: db}
}

type MeterQueryParams struct {
	Page       int
	Limit      int
	Regions    []string
	MeterTypes []string
	Locations  []string
	Search     string
	SortBy     string
	SortOrder  string
	Columns    []string
}

func parseMeterQuery(r *http.Request) MeterQueryParams {
	q := r.URL.Query()

	page, _ := strconv.Atoi(q.Get("page"))
	if page < 1 {
		page = 1
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 50
	}

	trimSplit := func(val string) []string {
		if val == "" {
			return nil
		}
		parts := strings.Split(val, ",")
		out := []string{}
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				out = append(out, p)
			}
		}
		return out
	}

	return MeterQueryParams{
		Page:       page,
		Limit:      limit,
		Regions:    trimSplit(q.Get("regions")),
		MeterTypes: trimSplit(q.Get("meterTypes")),
		Locations:  trimSplit(q.Get("locations")),
		Search:     q.Get("search"),
		SortBy:     q.Get("sortBy"),
		SortOrder:  q.Get("sortOrder"),
		Columns:    trimSplit(q.Get("columns")),
	}
}

type MeterQueryResult struct {
	Data []models.Meter `json:"data"`
	Meta any            `json:"meta"`
}

type DailyConsumptionResult struct {
	ConsumptionDate time.Time `bun:"consumption_date" json:"consumption_date"`
	MeterNumber     string    `bun:"meter_number" json:"meter_number"`
	DayStartReading float64   `bun:"day_start_reading" json:"day_start_reading"`
	DayEndReading   float64   `bun:"day_end_reading" json:"day_end_reading"`
	ConsumedEnergy  float64   `bun:"consumed_energy" json:"consumed_energy"`
	SystemName      string    `bun:"system_name" json:"system_name"`
}

func (s *MeterService) QueryMeters(ctx context.Context, r *http.Request) (*MeterQueryResult, error) {
	params := parseMeterQuery(r)

	q := s.db.NewSelect().Model((*models.Meter)(nil))

	if len(params.Regions) > 0 {
		q.Where("region IN (?)", bun.In(params.Regions))
	}
	if len(params.MeterTypes) > 0 {
		q.Where("meter_type IN (?)", bun.In(params.MeterTypes))
	}
	if len(params.Locations) > 0 {
		q.Where("location IN (?)", bun.In(params.Locations))
	}
	if params.Search != "" {
		search := "%" + params.Search + "%"
		q.Where("meter_number ILIKE ? OR station ILIKE ? OR feeder_panel_name ILIKE ?", search, search, search)
	}

	// Sorting
	if params.SortBy != "" {
		order := "ASC"
		if strings.ToLower(params.SortOrder) == "desc" {
			order = "DESC"
		}
		q.Order(params.SortBy + " " + order)
	}

	// Count total before pagination
	total, err := q.Count(ctx)
	if err != nil {
		return nil, err
	}

	// Apply pagination
	q.Offset((params.Page - 1) * params.Limit).Limit(params.Limit)

	var meters []models.Meter
	if err := q.Scan(ctx, &meters); err != nil {
		return nil, err
	}

	meta := map[string]any{
		"page":  params.Page,
		"limit": params.Limit,
		"total": total,
		"pages": (total + params.Limit - 1) / params.Limit, // ceil
	}

	// Add applied filters dynamically
	filters := map[string]any{}
	if len(params.Regions) > 0 {
		filters["regions"] = params.Regions
	}
	if len(params.MeterTypes) > 0 {
		filters["meterTypes"] = params.MeterTypes
	}
	if len(params.Locations) > 0 {
		filters["locations"] = params.Locations
	}
	if params.Search != "" {
		filters["search"] = params.Search
	}
	if params.SortBy != "" {
		filters["sortBy"] = params.SortBy
		filters["sortOrder"] = params.SortOrder
	}
	if len(filters) > 0 {
		meta["filters"] = filters
	}

	return &MeterQueryResult{
		Data: meters,
		Meta: meta,
	}, nil
}

// GetByID returns a single meter by ID
func (s *MeterService) GetMeterByID(ctx context.Context, id string) (*models.Meter, error) {
	meter := new(models.Meter)
	err := s.db.NewSelect().Model(meter).Where("id = ?", id).Scan(ctx)
	return meter, err
}

func (s *MeterService) GetAggregated(ctx context.Context, params *models.AggregatedQueryParams) (*models.AggregatedResult, error) {
	// 1️⃣ Build filters
	filters := []string{"1=1"}
	args := []interface{}{}

	// convert all filter lists to lower-case
	for i := range params.Regions {
		params.Regions[i] = strings.ToLower(params.Regions[i])
	}
	for i := range params.Districts {
		params.Districts[i] = strings.ToLower(params.Districts[i])
	}
	for i := range params.Stations {
		params.Stations[i] = strings.ToLower(params.Stations[i])
	}
	for i := range params.Locations {
		params.Locations[i] = strings.ToLower(params.Locations[i])
	}
	for i := range params.BoundaryPoints {
		params.BoundaryPoints[i] = strings.ToLower(params.BoundaryPoints[i])
	}
	for i := range params.MeterTypes {
		params.MeterTypes[i] = strings.ToLower(params.MeterTypes[i])
	}

	if params.DateFrom != "" {
		filters = append(filters, "r.reading_date >= ?")
		args = append(args, params.DateFrom)
	}
	if params.DateTo != "" {
		filters = append(filters, "r.reading_date <= ?")
		args = append(args, params.DateTo)
	}
	if len(params.Regions) > 0 {
		filters = append(filters, "lower(m.region) IN (?)")
		args = append(args, bun.In(params.Regions))
	}
	if len(params.Districts) > 0 {
		filters = append(filters, "lower(m.district) IN (?)")
		args = append(args, bun.In(params.Districts))
	}
	if len(params.Stations) > 0 {
		filters = append(filters, "lower(m.station) IN (?)")
		args = append(args, bun.In(params.Stations))
	}
	if len(params.Locations) > 0 {
		filters = append(filters, "lower(m.location) IN (?)")
		args = append(args, bun.In(params.Locations))
	}
	if len(params.BoundaryPoints) > 0 {
		filters = append(filters, "lower(m.boundary_metering_point) IN (?)")
		args = append(args, bun.In(params.BoundaryPoints))
	}
	if len(params.MeterTypes) > 0 {
		filters = append(filters, "lower(m.meter_type) IN (?)")
		args = append(args, bun.In(params.MeterTypes))
	}
	if params.StackByMeterType == false {
		params.StackByMeterType = false // optional, same as zero value
	}

	whereClause := strings.Join(filters, " AND ")

	// 2️⃣ Query aggregated totals
	var agg models.AggregatedReading
	err := s.db.NewRaw(`
        WITH filtered_meters AS (SELECT * FROM app.meters m),
        meter_readings AS (
            SELECT r.*, d.system_name, m.meter_type
            FROM app.meter_readings_daily r
            JOIN filtered_meters m ON r.meter_number = m.meter_number
            LEFT JOIN app.data_item_mapping d ON r.data_item_id = d.data_item_id
            WHERE `+whereClause+`
        )
        SELECT
            COUNT(DISTINCT meter_number) AS meter_count,
            SUM(record_count) AS reading_count,
            SUM(CASE WHEN system_name='import_kwh' THEN total_val ELSE 0 END) AS total_import_kwh,
            SUM(CASE WHEN system_name='export_kwh' THEN total_val ELSE 0 END) AS total_export_kwh,
            SUM(CASE WHEN system_name='import_kvah' THEN total_val ELSE 0 END) AS total_import_kvah,
            SUM(CASE WHEN system_name='export_kvah' THEN total_val ELSE 0 END) AS total_export_kvah,
            SUM(CASE WHEN system_name='import_kvar' THEN total_val ELSE 0 END) AS total_import_kvar,
            SUM(CASE WHEN system_name='export_kvar' THEN total_val ELSE 0 END) AS total_export_kvar
        FROM meter_readings
    `, args...).Scan(ctx, &agg)
	if err != nil {
		return nil, err
	}

	// 3️⃣ Query time series grouped by day
	type row struct {
		Date       time.Time `bun:"reading_date"`
		MeterType  string    `bun:"meter_type"`
		SystemName string    `bun:"system_name"`
		TotalVal   float64   `bun:"total_val"`
	}

	var rows []row
	err = s.db.NewRaw(`
        SELECT r.reading_date, m.meter_type, d.system_name, SUM(r.total_val) AS total_val
        FROM app.meter_readings_daily r
        JOIN app.meters m ON r.meter_number = m.meter_number
        LEFT JOIN app.data_item_mapping d ON r.data_item_id = d.data_item_id
        WHERE `+whereClause+`
        GROUP BY r.reading_date, m.meter_type, d.system_name
        ORDER BY r.reading_date
    `, args...).Scan(ctx, &rows)
	if err != nil {
		return nil, err
	}

	// 4️⃣ Build time series response
	timeSeriesMap := map[string]*models.TimeSeriesReading{}
	for _, r := range rows {
		key := r.Date.Format("2006-01-02")
		ts, ok := timeSeriesMap[key]
		if !ok {
			ts = &models.TimeSeriesReading{
				Date:  r.Date,
				Extra: make(map[string]float64),
			}
			timeSeriesMap[key] = ts
		}

		if params.StackByMeterType && r.MeterType != "" && r.SystemName != "" {
			ts.Extra[r.MeterType+"_"+r.SystemName] = r.TotalVal
		} else {
			switch r.SystemName {
			case "import_kwh":
				ts.TotalImportKWh += r.TotalVal
			case "export_kwh":
				ts.TotalExportKWh += r.TotalVal
			case "import_kvah":
				ts.TotalImportKVah += r.TotalVal
			case "export_kvah":
				ts.TotalExportKVah += r.TotalVal
			case "import_kvar":
				ts.TotalImportKVar += r.TotalVal
			case "export_kvar":
				ts.TotalExportKVar += r.TotalVal
			}
		}
	}

	var tsList []models.TimeSeriesReading
	for _, v := range timeSeriesMap {
		tsList = append(tsList, *v)
	}
	sort.Slice(tsList, func(i, j int) bool {
		return tsList[i].Date.Before(tsList[j].Date)
	})

	// 5️⃣ Query byMeterType totals
	var byType []models.ByMeterTypeReading
	err = s.db.NewRaw(`
        SELECT m.meter_type,
               SUM(CASE WHEN d.system_name='import_kwh' THEN r.total_val ELSE 0 END) AS total_import_kwh,
               SUM(CASE WHEN d.system_name='export_kwh' THEN r.total_val ELSE 0 END) AS total_export_kwh,
               SUM(CASE WHEN d.system_name='import_kvah' THEN r.total_val ELSE 0 END) AS total_import_kvah,
               SUM(CASE WHEN d.system_name='export_kvah' THEN r.total_val ELSE 0 END) AS total_export_kvah,
               SUM(CASE WHEN d.system_name='import_kvar' THEN r.total_val ELSE 0 END) AS total_import_kvar,
               SUM(CASE WHEN d.system_name='export_kvar' THEN r.total_val ELSE 0 END) AS total_export_kvar,
               SUM(r.record_count) AS reading_count
        FROM app.meter_readings_daily r
        JOIN app.meters m ON r.meter_number = m.meter_number
        LEFT JOIN app.data_item_mapping d ON r.data_item_id = d.data_item_id
        WHERE `+whereClause+`
        GROUP BY m.meter_type
    `, args...).Scan(ctx, &byType)
	if err != nil {
		return nil, err
	}

	// 6️⃣ Collect meter types
	var meterTypes []string
	for _, t := range byType {
		meterTypes = append(meterTypes, t.MeterType)
	}

	return &models.AggregatedResult{
		Aggregated:  agg,
		TimeSeries:  tsList,
		ByMeterType: byType,
		MeterTypes:  meterTypes,
	}, nil
}

type Filter struct {
	Query string
	Args  []interface{}
}

func (s *MeterService) GetDailyConsumption(
	ctx context.Context,
	params models.ReadingFilterParams,
) ([]models.DailyConsumptionResults, error) {
	var results []models.DailyConsumptionResults

	filters := buildReadingFilters(params)

	q := s.db.NewSelect().
		ColumnExpr("mcd.consumption_date AT TIME ZONE 'UTC' AS consumption_date").
		Column("mcd.meter_number").
		Column("mtr.region").
		Column("mtr.district").
		Column("mtr.station").
		Column("mtr.meter_type").
		Column("mtr.location").
		Column("mtr.voltage_kv").
		Column("mcd.day_start_reading").
		Column("mcd.day_end_reading").
		ColumnExpr("round((sum(mcd.consumption))::numeric, 4) as consumed_energy").
		Column("dim.system_name").
		TableExpr("app.meter_consumption_daily AS mcd").
		Join("LEFT JOIN app.meters AS mtr ON mcd.meter_number = mtr.meter_number").
		Join("JOIN app.data_item_mapping AS dim ON mcd.data_item_id = dim.data_item_id")

	for _, f := range filters {
		q = q.Where(f.Query, f.Args...)
	}

	q = q.
		Group("mcd.consumption_date").
		Group("mcd.meter_number").
		Group("mtr.region").
		Group("mtr.district").
		Group("mtr.meter_type").
		Group("mtr.station").
		Group("mtr.voltage_kv").
		Group("mtr.location").
		Group("dim.system_name").
		Group("mcd.day_start_reading").
		Group("mcd.day_end_reading")

	if err := q.Scan(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *MeterService) GetRegionalBoundaryDailyConsumption(
	ctx context.Context,
	params models.ReadingFilterParams,
) ([]models.DailyConsumptionResults, error) {
	var results []models.DailyConsumptionResults

	filters := buildReadingFilters(params)

	q := s.db.NewSelect().
		ColumnExpr("mcd.consumption_date AT TIME ZONE 'UTC' AS consumption_date").
		Column("mcd.meter_number").
		Column("mtr.region").
		Column("mtr.district").
		Column("mtr.station").
		Column("mtr.meter_type").
		Column("mtr.voltage_kv").
		Column("mtr.boundary_metering_point").
		Column("mtr.location").
		Column("mcd.day_start_reading").
		Column("mcd.day_end_reading").
		ColumnExpr("round((sum(mcd.consumption))::numeric, 4) AS consumed_energy").
		Column("dim.system_name").
		TableExpr("app.meter_consumption_daily AS mcd").
		Join("LEFT JOIN app.meters AS mtr ON mcd.meter_number = mtr.meter_number").
		Join("JOIN app.data_item_mapping AS dim ON mcd.data_item_id = dim.data_item_id").
		Where("mtr.meter_type = ?", "REGIONAL_BOUNDARY") // ✅ strict base filter

	// ✅ Apply dynamic filters (except meter_type)
	for _, f := range filters {
		// prevent user from overriding meter_type
		if strings.Contains(strings.ToLower(f.Query), "meter_type") {
			continue
		}
		q = q.Where(f.Query, f.Args...)
	}

	q = q.
		Group("mcd.consumption_date").
		Group("mcd.meter_number").
		Group("mtr.region").
		Group("mtr.district").
		Group("mtr.meter_type").
		Group("mtr.station").
		Group("mtr.voltage_kv").
		Group("mtr.boundary_metering_point").
		Group("mtr.location").
		Group("dim.system_name").
		Group("mcd.day_start_reading").
		Group("mcd.day_end_reading")

	if err := q.Scan(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *MeterService) GetDistrictBoundaryDailyConsumption(
	ctx context.Context,
	params models.ReadingFilterParams,
) ([]models.DailyConsumptionResults, error) {
	var results []models.DailyConsumptionResults

	filters := buildReadingFilters(params)

	q := s.db.NewSelect().
		ColumnExpr("mcd.consumption_date AT TIME ZONE 'UTC' AS consumption_date").
		Column("mcd.meter_number").
		Column("mtr.region").
		Column("mtr.district").
		Column("mtr.boundary_metering_point").
		Column("mtr.location").
		Column("mtr.station").
		Column("mtr.meter_type").
		Column("mtr.voltage_kv").
		Column("mcd.day_start_reading").
		Column("mcd.day_end_reading").
		ColumnExpr("round((sum(mcd.consumption))::numeric, 4) AS consumed_energy").
		Column("dim.system_name").
		TableExpr("app.meter_consumption_daily AS mcd").
		Join("LEFT JOIN app.meters AS mtr ON mcd.meter_number = mtr.meter_number").
		Join("JOIN app.data_item_mapping AS dim ON mcd.data_item_id = dim.data_item_id").
		Where("mtr.meter_type = ?", "DISTRICT_BOUNDARY") // ✅ strict base filter

	// ✅ Apply dynamic filters (except meter_type)
	for _, f := range filters {
		// prevent user from overriding meter_type
		if strings.Contains(strings.ToLower(f.Query), "meter_type") {
			continue
		}
		q = q.Where(f.Query, f.Args...)
	}

	q = q.
		Group("mcd.consumption_date").
		Group("mcd.meter_number").
		Group("mtr.region").
		Group("mtr.district").
		Group("mtr.meter_type").
		Group("mtr.station").
		Group("mtr.voltage_kv").
		Group("mtr.boundary_metering_point").
		Group("mtr.location").
		Group("dim.system_name").
		Group("mcd.day_start_reading").
		Group("mcd.day_end_reading")

	if err := q.Scan(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *MeterService) GetBSPDailyConsumption(
	ctx context.Context,
	params models.ReadingFilterParams,
) ([]models.DailyConsumptionResults, error) {
	var results []models.DailyConsumptionResults

	filters := buildReadingFilters(params)

	q := s.db.NewSelect().
		ColumnExpr("mcd.consumption_date AT TIME ZONE 'UTC' AS consumption_date").
		Column("mcd.meter_number").
		Column("mtr.region").
		Column("mtr.district").
		Column("mtr.station").
		Column("mtr.meter_type").
		Column("mtr.feeder_panel_name").
		ColumnExpr("mtr.ic_og AS ic_og").
		Column("mtr.voltage_kv").
		Column("mcd.day_start_reading").
		Column("mcd.day_end_reading").
		ColumnExpr("round((sum(mcd.consumption))::numeric, 4) AS consumed_energy").
		Column("dim.system_name").
		TableExpr("app.meter_consumption_daily AS mcd").
		Join("LEFT JOIN app.meters AS mtr ON mcd.meter_number = mtr.meter_number").
		Join("JOIN app.data_item_mapping AS dim ON mcd.data_item_id = dim.data_item_id").
		Where("mtr.meter_type = ?", "BSP") // ✅ strict base filter

	// ✅ Apply dynamic filters (except meter_type)
	for _, f := range filters {
		// prevent user from overriding meter_type
		if strings.Contains(strings.ToLower(f.Query), "meter_type") {
			continue
		}
		q = q.Where(f.Query, f.Args...)
	}

	q = q.
		Group("mcd.consumption_date").
		Group("mcd.meter_number").
		Group("mtr.region").
		Group("mtr.district").
		Group("mtr.meter_type").
		Group("mtr.station").
		Group("mtr.feeder_panel_name").
		Group("mtr.ic_og").
		Group("mtr.voltage_kv").
		Group("dim.system_name").
		Group("mcd.day_start_reading").
		Group("mcd.day_end_reading")

	if err := q.Scan(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *MeterService) GetDTXDailyConsumption(
	ctx context.Context,
	params models.ReadingFilterParams,
) ([]models.DailyConsumptionResults, error) {
	var results []models.DailyConsumptionResults

	filters := buildReadingFilters(params)

	q := s.db.NewSelect().
		ColumnExpr("mcd.consumption_date AT TIME ZONE 'UTC' AS consumption_date").
		Column("mcd.meter_number").
		Column("mtr.region").
		Column("mtr.district").
		Column("mtr.station").
		Column("mtr.meter_type").
		Column("mtr.feeder_panel_name").
		Column("mtr.voltage_kv").
		Column("mcd.day_start_reading").
		Column("mcd.day_end_reading").
		ColumnExpr("round((sum(mcd.consumption))::numeric, 4) AS consumed_energy").
		Column("dim.system_name").
		TableExpr("app.meter_consumption_daily AS mcd").
		Join("LEFT JOIN app.meters AS mtr ON mcd.meter_number = mtr.meter_number").
		Join("JOIN app.data_item_mapping AS dim ON mcd.data_item_id = dim.data_item_id").
		Where("mtr.meter_type = ?", "DTX") // ✅ strict base filter

	// ✅ Apply dynamic filters (except meter_type)
	for _, f := range filters {
		// prevent user from overriding meter_type
		if strings.Contains(strings.ToLower(f.Query), "meter_type") {
			continue
		}
		q = q.Where(f.Query, f.Args...)
	}

	q = q.
		Group("mcd.consumption_date").
		Group("mcd.meter_number").
		Group("mtr.region").
		Group("mtr.district").
		Group("mtr.meter_type").
		Group("mtr.station").
		Group("mtr.feeder_panel_name").
		Group("mtr.voltage_kv").
		Group("dim.system_name").
		Group("mcd.day_start_reading").
		Group("mcd.day_end_reading")

	if err := q.Scan(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *MeterService) GetDTXAggregatedConsumption(
	ctx context.Context,
	params models.ReadingFilterParams,
	groupBy string,
	additionalGroups []string,
) ([]models.AggregatedConsumptionResult, error) {

	var results []models.AggregatedConsumptionResult
	filters := buildReadingFilters(params)

	q := s.db.NewSelect().
		TableExpr("app.meter_consumption_daily AS mcd").
		Join("LEFT JOIN app.meters AS mtr ON mcd.meter_number = mtr.meter_number").
		Join("JOIN app.data_item_mapping AS dim ON mcd.data_item_id = dim.data_item_id").
		ColumnExpr("dim.system_name AS system_name").
		ColumnExpr("mtr.station AS station").
		ColumnExpr("mtr.region AS region").
		ColumnExpr("mtr.district AS district").
		ColumnExpr("mtr.feeder_panel_name AS feeder_panel_name").
		ColumnExpr("ROUND(SUM(mcd.consumption)::numeric, 4) AS total_consumption").
		ColumnExpr("COUNT(DISTINCT mcd.meter_number) AS active_meters").
		Where("mtr.meter_type = ?", "DTX")

	// --- Subquery: total count of all DTX meters ---
	subQTotal := s.db.NewSelect().
		TableExpr("app.meters AS mtr2").
		ColumnExpr("COUNT(DISTINCT mtr2.meter_number)").
		Where("mtr2.meter_type = ?", "DTX")

	// Apply filters for total meters (global level)
	if len(params.Regions) > 0 {
		subQTotal = subQTotal.Where("mtr2.region IN (?)", bun.In(params.Regions))
	}
	if len(params.Districts) > 0 {
		subQTotal = subQTotal.Where("mtr2.district IN (?)", bun.In(params.Districts))
	}
	if len(params.Stations) > 0 {
		subQTotal = subQTotal.Where("mtr2.station IN (?)", bun.In(params.Stations))
	}
	if len(params.Locations) > 0 {
		subQTotal = subQTotal.Where("mtr2.location IN (?)", bun.In(params.Locations))
	}

	q = q.ColumnExpr("(?) AS total_meter_count", subQTotal)

	// --- Subquery: total meters by region ---
	subQRegion := s.db.NewSelect().
		TableExpr("app.meters AS mtr3").
		ColumnExpr("COUNT(DISTINCT mtr3.meter_number)").
		Where("mtr3.meter_type = ?", "DTX").
		Where("mtr3.region = mtr.region") // correlate by region

	q = q.ColumnExpr("(?) AS total_meters_by_region", subQRegion)

	// --- Subquery: total meters by district ---
	subQDistrict := s.db.NewSelect().
		TableExpr("app.meters AS mtr4").
		ColumnExpr("COUNT(DISTINCT mtr4.meter_number)").
		Where("mtr4.meter_type = ?", "DTX").
		Where("mtr4.district = mtr.district") // correlate by district

	q = q.ColumnExpr("(?) AS total_meters_by_district", subQDistrict)

	// --- Time grouping ---
	groupCols := []bun.Safe{
		bun.Safe("dim.system_name"),
		bun.Safe("mtr.station"),
		bun.Safe("mtr.region"),
		bun.Safe("mtr.district"),
		bun.Safe("mtr.feeder_panel_name"),
	}

	switch groupBy {
	case "day":
		q = q.ColumnExpr("DATE(mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE(mcd.consumption_date)"))
	case "month":
		q = q.ColumnExpr("DATE_TRUNC('month', mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE_TRUNC('month', mcd.consumption_date)"))
	case "year":
		q = q.ColumnExpr("DATE_TRUNC('year', mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE_TRUNC('year', mcd.consumption_date)"))
	default:
		q = q.ColumnExpr("DATE(mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE(mcd.consumption_date)"))
	}

	// --- Additional grouping ---
	for _, g := range additionalGroups {
		col := fmt.Sprintf("mtr.%s", g)
		if g != "meter_type" {
			col = fmt.Sprintf("LOWER(mtr.%s)", g)
		}
		q = q.ColumnExpr(fmt.Sprintf("%s AS %s", col, g))
		groupCols = append(groupCols, bun.Safe(col))
	}

	// --- Apply filters (except meter_type override) ---
	for _, f := range filters {
		if strings.Contains(strings.ToLower(f.Query), "meter_type") {
			continue
		}
		q = q.Where(f.Query, f.Args...)
	}

	// --- Group by all relevant columns ---
	for _, g := range groupCols {
		q = q.GroupExpr(string(g))
	}

	if err := q.Scan(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *MeterService) GetAggregatedConsumption(
	ctx context.Context,
	params models.ReadingFilterParams,
	groupBy string,
	additionalGroups []string,
) ([]models.AggregatedConsumptionResult, error) {

	var results []models.AggregatedConsumptionResult
	filters := buildReadingFilters(params)

	q := s.db.NewSelect().
		TableExpr("app.meter_consumption_daily AS mcd").
		Join("LEFT JOIN app.meters AS mtr ON mcd.meter_number = mtr.meter_number").
		Join("JOIN app.data_item_mapping AS dim ON mcd.data_item_id = dim.data_item_id").
		ColumnExpr("dim.system_name AS system_name").
		ColumnExpr("mtr.station AS station").
		ColumnExpr("mtr.meter_type AS meter_type").
		ColumnExpr("mtr.region AS region").
		ColumnExpr("mtr.boundary_metering_point AS boundary_metering_point").
		ColumnExpr("mtr.feeder_panel_name AS feeder_panel_name").
		ColumnExpr("ROUND(SUM(mcd.consumption)::numeric, 4) AS total_consumption").
		ColumnExpr("COUNT(DISTINCT mcd.meter_number) AS active_meters")

	// --- Dynamic subquery for total meters ---
	subQuery := strings.Builder{}
	subQuery.WriteString(`
		(
			SELECT COUNT(DISTINCT mtr2.meter_number)
			FROM app.meters AS mtr2
			WHERE TRUE
	`)

	// Apply same filters to subquery
	if len(params.Regions) > 0 {
		subQuery.WriteString(" AND mtr2.region IN (?)")
	}
	if len(params.Districts) > 0 {
		subQuery.WriteString(" AND mtr2.district IN (?)")
	}
	if len(params.Stations) > 0 {
		subQuery.WriteString(" AND mtr2.station IN (?)")
	}
	if len(params.Locations) > 0 {
		subQuery.WriteString(" AND mtr2.location IN (?)")
	}
	if len(params.MeterTypes) > 0 {
		subQuery.WriteString(" AND mtr2.meter_type IN (?)")
	}

	subQuery.WriteString(") AS total_meter_count")

	// Collect bind parameters
	var subArgs []interface{}
	if len(params.Regions) > 0 {
		subArgs = append(subArgs, bun.In(params.Regions))
	}
	if len(params.Districts) > 0 {
		subArgs = append(subArgs, bun.In(params.Districts))
	}
	if len(params.Stations) > 0 {
		subArgs = append(subArgs, bun.In(params.Stations))
	}
	if len(params.Locations) > 0 {
		subArgs = append(subArgs, bun.In(params.Locations))
	}
	if len(params.MeterTypes) > 0 {
		subArgs = append(subArgs, bun.In(params.MeterTypes))
	}

	q = q.ColumnExpr(subQuery.String(), subArgs...)

	// --- Time grouping ---

	groupCols := []bun.Safe{
		bun.Safe("dim.system_name"),
		bun.Safe("mtr.region"),
		bun.Safe("mtr.boundary_metering_point"),
		bun.Safe("mtr.meter_type"),
		bun.Safe("mtr.station"),
		bun.Safe("mtr.feeder_panel_name"),
	}
	switch groupBy {
	case "day":
		q = q.ColumnExpr("DATE(mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE(mcd.consumption_date)"))
	case "month":
		q = q.ColumnExpr("DATE_TRUNC('month', mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE_TRUNC('month', mcd.consumption_date)"))
	case "year":
		q = q.ColumnExpr("DATE_TRUNC('year', mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE_TRUNC('year', mcd.consumption_date)"))
	default:
		q = q.ColumnExpr("DATE(mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE(mcd.consumption_date)"))
	}

	// --- Additional grouping (region, station, etc.) ---
	for _, g := range additionalGroups {
		col := fmt.Sprintf("mtr.%s", g)
		if g != "meter_type" {
			col = fmt.Sprintf("LOWER(mtr.%s)", g)
		}
		q = q.ColumnExpr(fmt.Sprintf("%s AS %s", col, g))
		groupCols = append(groupCols, bun.Safe(col))
	}

	// Apply filters to main query
	for _, f := range filters {
		q = q.Where(f.Query, f.Args...)
	}

	// Group by all relevant columns
	for _, g := range groupCols {
		q = q.GroupExpr(string(g))
	}

	// Run query
	if err := q.Scan(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *MeterService) GetBSPAggregatedConsumption(
	ctx context.Context,
	params models.ReadingFilterParams,
	groupBy string,
	additionalGroups []string,
) ([]models.AggregatedConsumptionResult, error) {

	var results []models.AggregatedConsumptionResult
	filters := buildReadingFilters(params)

	q := s.db.NewSelect().
		TableExpr("app.meter_consumption_daily AS mcd").
		Join("LEFT JOIN app.meters AS mtr ON mcd.meter_number = mtr.meter_number").
		Join("JOIN app.data_item_mapping AS dim ON mcd.data_item_id = dim.data_item_id").
		ColumnExpr("dim.system_name AS system_name").
		ColumnExpr("mtr.station AS station").
		ColumnExpr("mtr.feeder_panel_name AS feeder_panel_name").
		ColumnExpr("mtr.ic_og AS ic_og").
		ColumnExpr("ROUND(SUM(mcd.consumption)::numeric, 4) AS total_consumption").
		ColumnExpr("COUNT(DISTINCT mcd.meter_number) AS active_meters").
		Where("mtr.meter_type = ?", "BSP")

	// --- Subquery 1: total_meter_count (filtered) ---
	subQFiltered := s.db.NewSelect().
		TableExpr("app.meters AS mtr2").
		Join("JOIN app.meter_consumption_daily AS mcd2 ON mcd2.meter_number = mtr2.meter_number").
		ColumnExpr("COUNT(DISTINCT mtr2.meter_number)").
		Where("mtr2.meter_type = ?", "BSP")

	for _, f := range filters {
		qry := strings.ReplaceAll(f.Query, "mtr.", "mtr2.")
		qry = strings.ReplaceAll(qry, "mcd.", "mcd2.")
		if strings.Contains(strings.ToLower(qry), "meter_type") {
			continue
		}
		subQFiltered = subQFiltered.Where(qry, f.Args...)
	}

	q = q.ColumnExpr("(?) AS total_meter_count", subQFiltered)

	// --- Subquery 2: all_meters_count (unfiltered BSP) ---
	subQAll := s.db.NewSelect().
		TableExpr("app.meters AS mtr3").
		ColumnExpr("COUNT(DISTINCT mtr3.meter_number)").
		Where("mtr3.meter_type = ?", "BSP")

	q = q.ColumnExpr("(?) AS all_meters_count", subQAll)

	// --- Time grouping ---
	groupCols := []bun.Safe{
		bun.Safe("dim.system_name"),
		bun.Safe("mtr.station"),
		bun.Safe("mtr.feeder_panel_name"),
		bun.Safe("mtr.ic_og"),
	}

	switch groupBy {
	case "day":
		q = q.ColumnExpr("DATE(mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE(mcd.consumption_date)"))
	case "month":
		q = q.ColumnExpr("DATE_TRUNC('month', mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE_TRUNC('month', mcd.consumption_date)"))
	case "year":
		q = q.ColumnExpr("DATE_TRUNC('year', mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE_TRUNC('year', mcd.consumption_date)"))
	default:
		q = q.ColumnExpr("DATE(mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE(mcd.consumption_date)"))
	}

	// --- Additional grouping ---
	for _, g := range additionalGroups {
		col := fmt.Sprintf("mtr.%s", g)
		if g != "meter_type" {
			col = fmt.Sprintf("LOWER(mtr.%s)", g)
		}
		q = q.ColumnExpr(fmt.Sprintf("%s AS %s", col, g))
		groupCols = append(groupCols, bun.Safe(col))
	}

	// --- Apply filters (skip meter_type override) ---
	for _, f := range filters {
		if strings.Contains(strings.ToLower(f.Query), "meter_type") {
			continue
		}
		q = q.Where(f.Query, f.Args...)
	}

	// --- Group by all relevant columns ---
	for _, g := range groupCols {
		q = q.GroupExpr(string(g))
	}

	if err := q.Scan(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *MeterService) GetFeederAggregatedConsumption(
	ctx context.Context,
	params models.ReadingFilterParams,
	groupBy string,
	additionalGroups []string,
	meterTypes []string, // New parameter to specify meter types
) ([]models.AggregatedConsumptionResult, error) {

	var results []models.AggregatedConsumptionResult
	filters := buildReadingFilters(params)

	// Validate and set default meter types if none provided
	if len(meterTypes) == 0 {
		meterTypes = []string{"BSP", "PSS", "SS"} // Default to all types
	}

	q := s.db.NewSelect().
		TableExpr("app.meter_consumption_daily AS mcd").
		Join("LEFT JOIN app.meters AS mtr ON mcd.meter_number = mtr.meter_number").
		Join("JOIN app.data_item_mapping AS dim ON mcd.data_item_id = dim.data_item_id").
		ColumnExpr("dim.system_name AS system_name").
		ColumnExpr("mtr.station AS station").
		ColumnExpr("mtr.feeder_panel_name AS feeder_panel_name").
		ColumnExpr("mtr.ic_og AS ic_og").
		ColumnExpr("mtr.meter_type AS meter_type"). // Include meter_type in results
		ColumnExpr("ROUND(SUM(mcd.consumption)::numeric, 4) AS total_consumption").
		ColumnExpr("COUNT(DISTINCT mcd.meter_number) AS active_meters").
		Where("mtr.meter_type IN (?)", bun.In(meterTypes)) // Use IN clause for multiple types

	// --- Subquery 1: total_meter_count (filtered) ---
	subQFiltered := s.db.NewSelect().
		TableExpr("app.meters AS mtr2").
		Join("JOIN app.meter_consumption_daily AS mcd2 ON mcd2.meter_number = mtr2.meter_number").
		ColumnExpr("COUNT(DISTINCT mtr2.meter_number)").
		Where("mtr2.meter_type IN (?)", bun.In(meterTypes))

	for _, f := range filters {
		qry := strings.ReplaceAll(f.Query, "mtr.", "mtr2.")
		qry = strings.ReplaceAll(qry, "mcd.", "mcd2.")
		if strings.Contains(strings.ToLower(qry), "meter_type") {
			continue
		}
		subQFiltered = subQFiltered.Where(qry, f.Args...)
	}

	q = q.ColumnExpr("(?) AS total_meter_count", subQFiltered)

	// --- Subquery 2: all_meters_count (unfiltered by specified types) ---
	subQAll := s.db.NewSelect().
		TableExpr("app.meters AS mtr3").
		ColumnExpr("COUNT(DISTINCT mtr3.meter_number)").
		Where("mtr3.meter_type IN (?)", bun.In(meterTypes))

	q = q.ColumnExpr("(?) AS all_meters_count", subQAll)

	// --- Time grouping ---
	groupCols := []bun.Safe{
		bun.Safe("dim.system_name"),
		bun.Safe("mtr.station"),
		bun.Safe("mtr.feeder_panel_name"),
		bun.Safe("mtr.ic_og"),
		bun.Safe("mtr.meter_type"), // Include meter_type in grouping
	}

	switch groupBy {
	case "day":
		q = q.ColumnExpr("DATE(mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE(mcd.consumption_date)"))
	case "month":
		q = q.ColumnExpr("DATE_TRUNC('month', mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE_TRUNC('month', mcd.consumption_date)"))
	case "year":
		q = q.ColumnExpr("DATE_TRUNC('year', mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE_TRUNC('year', mcd.consumption_date)"))
	default:
		q = q.ColumnExpr("DATE(mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE(mcd.consumption_date)"))
	}

	// --- Additional grouping ---
	for _, g := range additionalGroups {
		col := fmt.Sprintf("mtr.%s", g)
		if g != "meter_type" {
			col = fmt.Sprintf("LOWER(mtr.%s)", g)
		}
		q = q.ColumnExpr(fmt.Sprintf("%s AS %s", col, g))
		groupCols = append(groupCols, bun.Safe(col))
	}

	// --- Apply filters (skip meter_type override) ---
	for _, f := range filters {
		if strings.Contains(strings.ToLower(f.Query), "meter_type") {
			continue
		}
		q = q.Where(f.Query, f.Args...)
	}

	// --- Group by all relevant columns ---
	for _, g := range groupCols {
		q = q.GroupExpr(string(g))
	}

	// Optional: Order by meter_type for consistent results
	q = q.OrderExpr("mtr.meter_type", "group_period")

	if err := q.Scan(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *MeterService) GetFeederDailyConsumption(
	ctx context.Context,
	params models.ReadingFilterParams,
	meterTypes []string, // New parameter to specify meter types
) ([]models.DailyConsumptionResults, error) {
	var results []models.DailyConsumptionResults

	filters := buildReadingFilters(params)

	// Validate and set default meter types if none provided
	if len(meterTypes) == 0 {
		meterTypes = []string{"BSP", "PSS", "SS"} // Default to all types
	}

	q := s.db.NewSelect().
		ColumnExpr("mcd.consumption_date AT TIME ZONE 'UTC' AS consumption_date").
		Column("mcd.meter_number").
		Column("mtr.region").
		Column("mtr.district").
		Column("mtr.station").
		Column("mtr.meter_type").
		Column("mtr.feeder_panel_name").
		Column("mtr.ic_og").
		Column("mtr.voltage_kv").
		Column("mcd.day_start_reading").
		Column("mcd.day_end_reading").
		ColumnExpr("round((sum(mcd.consumption))::numeric, 4) AS consumed_energy").
		Column("dim.system_name").
		TableExpr("app.meter_consumption_daily AS mcd").
		Join("LEFT JOIN app.meters AS mtr ON mcd.meter_number = mtr.meter_number").
		Join("JOIN app.data_item_mapping AS dim ON mcd.data_item_id = dim.data_item_id").
		Where("mtr.meter_type IN (?)", bun.In(meterTypes)) // ✅ Use IN clause for multiple types

	// ✅ Apply dynamic filters (except meter_type)
	for _, f := range filters {
		// prevent user from overriding meter_type
		if strings.Contains(strings.ToLower(f.Query), "meter_type") {
			continue
		}
		q = q.Where(f.Query, f.Args...)
	}

	q = q.
		Group("mcd.consumption_date").
		Group("mcd.meter_number").
		Group("mtr.region").
		Group("mtr.district").
		Group("mtr.meter_type"). // Keep meter_type in grouping
		Group("mtr.station").
		Group("mtr.feeder_panel_name").
		Group("mtr.ic_og").
		Group("mtr.voltage_kv").
		Group("dim.system_name").
		Group("mcd.day_start_reading").
		Group("mcd.day_end_reading").
		Order("mtr.meter_type", "mcd.consumption_date") // Optional: Add ordering for consistency

	if err := q.Scan(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *MeterService) GetPSSDailyConsumption(
	ctx context.Context,
	params models.ReadingFilterParams,
) ([]models.DailyConsumptionResults, error) {
	var results []models.DailyConsumptionResults

	filters := buildReadingFilters(params)

	q := s.db.NewSelect().
		ColumnExpr("mcd.consumption_date AT TIME ZONE 'UTC' AS consumption_date").
		Column("mcd.meter_number").
		Column("mtr.region").
		Column("mtr.district").
		Column("mtr.station").
		Column("mtr.meter_type").
		Column("mtr.feeder_panel_name").
		Column("mtr.ic_og").
		Column("mtr.voltage_kv").
		Column("mcd.day_start_reading").
		Column("mcd.day_end_reading").
		ColumnExpr("round((sum(mcd.consumption))::numeric, 4) AS consumed_energy").
		Column("dim.system_name").
		TableExpr("app.meter_consumption_daily AS mcd").
		Join("LEFT JOIN app.meters AS mtr ON mcd.meter_number = mtr.meter_number").
		Join("JOIN app.data_item_mapping AS dim ON mcd.data_item_id = dim.data_item_id").
		Where("mtr.meter_type = ?", "PSS") // ✅ strict base filter

	// ✅ Apply dynamic filters (except meter_type)
	for _, f := range filters {
		// prevent user from overriding meter_type
		if strings.Contains(strings.ToLower(f.Query), "meter_type") {
			continue
		}
		q = q.Where(f.Query, f.Args...)
	}

	q = q.
		Group("mcd.consumption_date").
		Group("mcd.meter_number").
		Group("mtr.region").
		Group("mtr.district").
		Group("mtr.meter_type").
		Group("mtr.station").
		Group("mtr.feeder_panel_name").
		Group("mtr.ic_og").
		Group("mtr.voltage_kv").
		Group("dim.system_name").
		Group("mcd.day_start_reading").
		Group("mcd.day_end_reading")

	if err := q.Scan(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *MeterService) GetPSSAggregatedConsumption(
	ctx context.Context,
	params models.ReadingFilterParams,
	groupBy string,
	additionalGroups []string,
) ([]models.AggregatedConsumptionResult, error) {

	var results []models.AggregatedConsumptionResult
	filters := buildReadingFilters(params)

	q := s.db.NewSelect().
		TableExpr("app.meter_consumption_daily AS mcd").
		Join("LEFT JOIN app.meters AS mtr ON mcd.meter_number = mtr.meter_number").
		Join("JOIN app.data_item_mapping AS dim ON mcd.data_item_id = dim.data_item_id").
		ColumnExpr("dim.system_name AS system_name").
		ColumnExpr("mtr.station AS station").
		ColumnExpr("mtr.feeder_panel_name AS feeder_panel_name").
		ColumnExpr("mtr.ic_og AS ic_og").
		ColumnExpr("ROUND(SUM(mcd.consumption)::numeric, 4) AS total_consumption").
		ColumnExpr("COUNT(DISTINCT mcd.meter_number) AS active_meters").
		Where("mtr.meter_type = ?", "PSS")

	// --- Subquery 1: total_meter_count (filtered) ---
	subQFiltered := s.db.NewSelect().
		TableExpr("app.meters AS mtr2").
		Join("JOIN app.meter_consumption_daily AS mcd2 ON mcd2.meter_number = mtr2.meter_number").
		ColumnExpr("COUNT(DISTINCT mtr2.meter_number)").
		Where("mtr2.meter_type = ?", "PSS")

	for _, f := range filters {
		qry := strings.ReplaceAll(f.Query, "mtr.", "mtr2.")
		qry = strings.ReplaceAll(qry, "mcd.", "mcd2.")
		if strings.Contains(strings.ToLower(qry), "meter_type") {
			continue
		}
		subQFiltered = subQFiltered.Where(qry, f.Args...)
	}

	q = q.ColumnExpr("(?) AS total_meter_count", subQFiltered)

	// --- Subquery 2: all_meters_count (unfiltered PSS) ---
	subQAll := s.db.NewSelect().
		TableExpr("app.meters AS mtr3").
		ColumnExpr("COUNT(DISTINCT mtr3.meter_number)").
		Where("mtr3.meter_type = ?", "PSS")

	q = q.ColumnExpr("(?) AS all_meters_count", subQAll)

	// --- Time grouping ---
	groupCols := []bun.Safe{
		bun.Safe("dim.system_name"),
		bun.Safe("mtr.station"),
		bun.Safe("mtr.feeder_panel_name"),
		bun.Safe("mtr.ic_og"),
	}

	switch groupBy {
	case "day":
		q = q.ColumnExpr("DATE(mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE(mcd.consumption_date)"))
	case "month":
		q = q.ColumnExpr("DATE_TRUNC('month', mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE_TRUNC('month', mcd.consumption_date)"))
	case "year":
		q = q.ColumnExpr("DATE_TRUNC('year', mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE_TRUNC('year', mcd.consumption_date)"))
	default:
		q = q.ColumnExpr("DATE(mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE(mcd.consumption_date)"))
	}

	// --- Additional grouping ---
	for _, g := range additionalGroups {
		col := fmt.Sprintf("mtr.%s", g)
		if g != "meter_type" {
			col = fmt.Sprintf("LOWER(mtr.%s)", g)
		}
		q = q.ColumnExpr(fmt.Sprintf("%s AS %s", col, g))
		groupCols = append(groupCols, bun.Safe(col))
	}

	// --- Apply filters (skip meter_type override) ---
	for _, f := range filters {
		if strings.Contains(strings.ToLower(f.Query), "meter_type") {
			continue
		}
		q = q.Where(f.Query, f.Args...)
	}

	// --- Group by all relevant columns ---
	for _, g := range groupCols {
		q = q.GroupExpr(string(g))
	}

	if err := q.Scan(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *MeterService) GetSSDailyConsumption(
	ctx context.Context,
	params models.ReadingFilterParams,
) ([]models.DailyConsumptionResults, error) {
	var results []models.DailyConsumptionResults

	filters := buildReadingFilters(params)

	q := s.db.NewSelect().
		ColumnExpr("mcd.consumption_date AT TIME ZONE 'UTC' AS consumption_date").
		Column("mcd.meter_number").
		Column("mtr.region").
		Column("mtr.district").
		Column("mtr.station").
		Column("mtr.meter_type").
		Column("mtr.feeder_panel_name").
		Column("mtr.voltage_kv").
		Column("mcd.day_start_reading").
		Column("mcd.day_end_reading").
		ColumnExpr("round((sum(mcd.consumption))::numeric, 4) AS consumed_energy").
		Column("dim.system_name").
		TableExpr("app.meter_consumption_daily AS mcd").
		Join("LEFT JOIN app.meters AS mtr ON mcd.meter_number = mtr.meter_number").
		Join("JOIN app.data_item_mapping AS dim ON mcd.data_item_id = dim.data_item_id").
		Where("mtr.meter_type = ?", "SS") // ✅ strict base filter

	// ✅ Apply dynamic filters (except meter_type)
	for _, f := range filters {
		// prevent user from overriding meter_type
		if strings.Contains(strings.ToLower(f.Query), "meter_type") {
			continue
		}
		q = q.Where(f.Query, f.Args...)
	}

	q = q.
		Group("mcd.consumption_date").
		Group("mcd.meter_number").
		Group("mtr.region").
		Group("mtr.district").
		Group("mtr.meter_type").
		Group("mtr.station").
		Group("mtr.feeder_panel_name").
		Group("mtr.voltage_kv").
		Group("dim.system_name").
		Group("mcd.day_start_reading").
		Group("mcd.day_end_reading")

	if err := q.Scan(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *MeterService) GetSSAggregatedConsumption(
	ctx context.Context,
	params models.ReadingFilterParams,
	groupBy string,
	additionalGroups []string,
) ([]models.AggregatedConsumptionResult, error) {

	var results []models.AggregatedConsumptionResult
	filters := buildReadingFilters(params)

	q := s.db.NewSelect().
		TableExpr("app.meter_consumption_daily AS mcd").
		Join("LEFT JOIN app.meters AS mtr ON mcd.meter_number = mtr.meter_number").
		Join("JOIN app.data_item_mapping AS dim ON mcd.data_item_id = dim.data_item_id").
		ColumnExpr("dim.system_name AS system_name").
		ColumnExpr("mtr.station AS station").
		ColumnExpr("mtr.feeder_panel_name AS feeder_panel_name").
		ColumnExpr("mtr.ic_og AS ic_og").
		ColumnExpr("ROUND(SUM(mcd.consumption)::numeric, 4) AS total_consumption").
		ColumnExpr("COUNT(DISTINCT mcd.meter_number) AS active_meters").
		Where("mtr.meter_type = ?", "SS")

	// --- Subquery 1: total_meter_count (filtered) ---
	subQFiltered := s.db.NewSelect().
		TableExpr("app.meters AS mtr2").
		Join("JOIN app.meter_consumption_daily AS mcd2 ON mcd2.meter_number = mtr2.meter_number").
		ColumnExpr("COUNT(DISTINCT mtr2.meter_number)").
		Where("mtr2.meter_type = ?", "SS")

	for _, f := range filters {
		qry := strings.ReplaceAll(f.Query, "mtr.", "mtr2.")
		qry = strings.ReplaceAll(qry, "mcd.", "mcd2.")
		if strings.Contains(strings.ToLower(qry), "meter_type") {
			continue
		}
		subQFiltered = subQFiltered.Where(qry, f.Args...)
	}

	q = q.ColumnExpr("(?) AS total_meter_count", subQFiltered)

	// --- Subquery 2: all_meters_count (unfiltered SS) ---
	subQAll := s.db.NewSelect().
		TableExpr("app.meters AS mtr3").
		ColumnExpr("COUNT(DISTINCT mtr3.meter_number)").
		Where("mtr3.meter_type = ?", "SS")

	q = q.ColumnExpr("(?) AS all_meters_count", subQAll)

	// --- Time grouping ---
	groupCols := []bun.Safe{
		bun.Safe("dim.system_name"),
		bun.Safe("mtr.station"),
		bun.Safe("mtr.feeder_panel_name"),
		bun.Safe("mtr.ic_og"),
	}

	switch groupBy {
	case "day":
		q = q.ColumnExpr("DATE(mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE(mcd.consumption_date)"))
	case "month":
		q = q.ColumnExpr("DATE_TRUNC('month', mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE_TRUNC('month', mcd.consumption_date)"))
	case "year":
		q = q.ColumnExpr("DATE_TRUNC('year', mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE_TRUNC('year', mcd.consumption_date)"))
	default:
		q = q.ColumnExpr("DATE(mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE(mcd.consumption_date)"))
	}

	// --- Additional grouping ---
	for _, g := range additionalGroups {
		col := fmt.Sprintf("mtr.%s", g)
		if g != "meter_type" {
			col = fmt.Sprintf("LOWER(mtr.%s)", g)
		}
		q = q.ColumnExpr(fmt.Sprintf("%s AS %s", col, g))
		groupCols = append(groupCols, bun.Safe(col))
	}

	// --- Apply filters (skip meter_type override) ---
	for _, f := range filters {
		if strings.Contains(strings.ToLower(f.Query), "meter_type") {
			continue
		}
		q = q.Where(f.Query, f.Args...)
	}

	// --- Group by all relevant columns ---
	for _, g := range groupCols {
		q = q.GroupExpr(string(g))
	}

	if err := q.Scan(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *MeterService) GetRegionalBoundaryAggregatedConsumption(
	ctx context.Context,
	params models.ReadingFilterParams,
	groupBy string,
	additionalGroups []string,
) ([]models.AggregatedConsumptionResult, error) {

	var results []models.AggregatedConsumptionResult
	filters := buildReadingFilters(params)

	q := s.db.NewSelect().
		TableExpr("app.meter_consumption_daily AS mcd").
		Join("LEFT JOIN app.meters AS mtr ON mcd.meter_number = mtr.meter_number").
		Join("JOIN app.data_item_mapping AS dim ON mcd.data_item_id = dim.data_item_id").
		ColumnExpr("dim.system_name AS system_name").
		ColumnExpr("mtr.boundary_metering_point AS boundary_metering_point").
		ColumnExpr("mtr.location AS location").
		ColumnExpr("ROUND(SUM(mcd.consumption)::numeric, 4) AS total_consumption").
		ColumnExpr("COUNT(DISTINCT mcd.meter_number) AS active_meters").
		Where("mtr.meter_type = ?", "REGIONAL_BOUNDARY")

	// --- Subquery for total meter count ---
	subQ := s.db.NewSelect().
		TableExpr("app.meters AS mtr2").
		ColumnExpr("COUNT(DISTINCT mtr2.meter_number)").
		Where("mtr2.meter_type = ?", "REGIONAL_BOUNDARY")

	if len(params.Regions) > 0 {
		subQ = subQ.Where("mtr2.region IN (?)", bun.In(params.Regions))
	}
	if len(params.Districts) > 0 {
		subQ = subQ.Where("mtr2.district IN (?)", bun.In(params.Districts))
	}
	if len(params.Stations) > 0 {
		subQ = subQ.Where("mtr2.station IN (?)", bun.In(params.Stations))
	}
	if len(params.Locations) > 0 {
		subQ = subQ.Where("mtr2.location IN (?)", bun.In(params.Locations))
	}

	q = q.ColumnExpr("(?) AS total_meter_count", subQ)

	// --- Time grouping ---
	groupCols := []bun.Safe{
		bun.Safe("dim.system_name"),
		bun.Safe("mtr.boundary_metering_point"), // ✅ Add here!
		bun.Safe("mtr.location"),                // ✅ Add here!
	}

	switch groupBy {
	case "day":
		q = q.ColumnExpr("DATE(mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE(mcd.consumption_date)"))
	case "month":
		q = q.ColumnExpr("DATE_TRUNC('month', mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE_TRUNC('month', mcd.consumption_date)"))
	case "year":
		q = q.ColumnExpr("DATE_TRUNC('year', mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE_TRUNC('year', mcd.consumption_date)"))
	default:
		q = q.ColumnExpr("DATE(mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE(mcd.consumption_date)"))
	}

	// --- Additional grouping ---
	for _, g := range additionalGroups {
		col := fmt.Sprintf("mtr.%s", g)
		if g != "meter_type" {
			col = fmt.Sprintf("LOWER(mtr.%s)", g)
		}
		q = q.ColumnExpr(fmt.Sprintf("%s AS %s", col, g))
		groupCols = append(groupCols, bun.Safe(col))
	}

	// --- Apply filters (skip meter_type override) ---
	for _, f := range filters {
		if strings.Contains(strings.ToLower(f.Query), "meter_type") {
			continue
		}
		q = q.Where(f.Query, f.Args...)
	}

	// --- Group by all relevant columns ---
	for _, g := range groupCols {
		q = q.GroupExpr(string(g))
	}

	if err := q.Scan(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *MeterService) GetDistrictBoundaryAggregatedConsumption(
	ctx context.Context,
	params models.ReadingFilterParams,
	groupBy string,
	additionalGroups []string,
) ([]models.AggregatedConsumptionResult, error) {

	var results []models.AggregatedConsumptionResult
	filters := buildReadingFilters(params)

	q := s.db.NewSelect().
		TableExpr("app.meter_consumption_daily AS mcd").
		Join("LEFT JOIN app.meters AS mtr ON mcd.meter_number = mtr.meter_number").
		Join("JOIN app.data_item_mapping AS dim ON mcd.data_item_id = dim.data_item_id").
		ColumnExpr("dim.system_name AS system_name").
		ColumnExpr("mtr.boundary_metering_point AS boundary_metering_point").
		ColumnExpr("mtr.location AS location").
		ColumnExpr("ROUND(SUM(mcd.consumption)::numeric, 4) AS total_consumption").
		ColumnExpr("COUNT(DISTINCT mcd.meter_number) AS active_meters").
		Where("mtr.meter_type = ?", "DISTRICT_BOUNDARY")

	// --- Subquery for total meter count ---
	subQ := s.db.NewSelect().
		TableExpr("app.meters AS mtr2").
		ColumnExpr("COUNT(DISTINCT mtr2.meter_number)").
		Where("mtr2.meter_type = ?", "DISTRICT_BOUNDARY")

	if len(params.Regions) > 0 {
		subQ = subQ.Where("mtr2.region IN (?)", bun.In(params.Regions))
	}
	if len(params.Districts) > 0 {
		subQ = subQ.Where("mtr2.district IN (?)", bun.In(params.Districts))
	}
	if len(params.Stations) > 0 {
		subQ = subQ.Where("mtr2.station IN (?)", bun.In(params.Stations))
	}
	if len(params.Locations) > 0 {
		subQ = subQ.Where("mtr2.location IN (?)", bun.In(params.Locations))
	}

	q = q.ColumnExpr("(?) AS total_meter_count", subQ)

	// --- Time grouping ---
	groupCols := []bun.Safe{
		bun.Safe("dim.system_name"),
		bun.Safe("mtr.boundary_metering_point"), // ✅ Add here!
		bun.Safe("mtr.location"),                // ✅ Add here!
	}

	switch groupBy {
	case "day":
		q = q.ColumnExpr("DATE(mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE(mcd.consumption_date)"))
	case "month":
		q = q.ColumnExpr("DATE_TRUNC('month', mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE_TRUNC('month', mcd.consumption_date)"))
	case "year":
		q = q.ColumnExpr("DATE_TRUNC('year', mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE_TRUNC('year', mcd.consumption_date)"))
	default:
		q = q.ColumnExpr("DATE(mcd.consumption_date) AS group_period")
		groupCols = append(groupCols, bun.Safe("DATE(mcd.consumption_date)"))
	}

	// --- Additional grouping ---
	for _, g := range additionalGroups {
		col := fmt.Sprintf("mtr.%s", g)
		if g != "meter_type" {
			col = fmt.Sprintf("LOWER(mtr.%s)", g)
		}
		q = q.ColumnExpr(fmt.Sprintf("%s AS %s", col, g))
		groupCols = append(groupCols, bun.Safe(col))
	}

	// --- Apply filters (skip meter_type override) ---
	for _, f := range filters {
		if strings.Contains(strings.ToLower(f.Query), "meter_type") {
			continue
		}
		q = q.Where(f.Query, f.Args...)
	}

	// --- Group by all relevant columns ---
	for _, g := range groupCols {
		q = q.GroupExpr(string(g))
	}

	if err := q.Scan(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

//func (s *MeterService) GetMeterStatus(
//	ctx context.Context,
//	params models.ReadingFilterParams,
//) ([]models.MeterStatusResult, error) {
//
//	results := []models.MeterStatusResult{}
//
//	filters := buildReadingFilters(params)
//
//	q := s.db.NewSelect().
//		TableExpr("app.meters AS mtr").
//		Column("mcd.consumption_date").
//		Column("mtr.meter_number").
//		Column("mtr.meter_type").
//		Column("mtr.region").
//		Column("mtr.district").
//		Column("mtr.boundary_metering_point").
//		Column("mtr.location").
//		Column("mtr.station").
//		Column("mtr.feeder_panel_name").
//		Column("mtr.location").
//		ColumnExpr(`
//            CASE
//                WHEN mcd.meter_number IS NULL THEN 'OFFLINE - No Record'
//                WHEN mcd.data_item_id = 'NO_DATA' THEN 'OFFLINE - No Data'
//                ELSE 'ONLINE'
//            END AS status`).
//		Column("mcd.consumption").
//		Column("mcd.reading_count").
//		Column("mcd.day_start_time").
//		Column("mcd.day_end_time").
//		Join(`LEFT JOIN app.meter_consumption_daily AS mcd
//              ON mtr.meter_number = mcd.meter_number
//              AND mcd.consumption_date BETWEEN ? AND ?`,
//			params.DateFrom, params.DateTo)
//
//	// 🔥 Apply dynamic meter filters (region, meter_type, station, etc.)
//	for _, f := range filters {
//		// skip date filter because we applied date range in the LEFT JOIN
//		if strings.Contains(strings.ToLower(f.Query), "consumption_date") {
//			continue
//		}
//		q = q.Where(f.Query, f.Args...)
//	}
//
//	// Sorting exactly as your SQL
//	q = q.OrderExpr(`
//        CASE
//            WHEN mcd.meter_number IS NULL THEN 1
//            WHEN mcd.data_item_id = 'NO_DATA' THEN 2
//            ELSE 3
//        END ASC,
//        mtr.meter_number ASC
//    `)
//
//	if err := q.Scan(ctx, &results); err != nil {
//		return nil, err
//	}
//
//	return results, nil
//}
//
//func (s *MeterService) GetMeterStatusCounts(
//	ctx context.Context,
//	params models.ReadingFilterParams,
//) (map[string]int, error) {
//
//	counts := map[string]int{
//		"total":             0,
//		"online":            0,
//		"offline_no_record": 0,
//		"offline_no_data":   0,
//	}
//
//	// Build dynamic filters for meters
//	filters := buildReadingFilters(params)
//
//	// Base query
//	q := s.db.NewSelect().
//		TableExpr("app.meters AS mtr").
//		ColumnExpr(`
//			COUNT(DISTINCT CASE WHEN mcd.meter_number IS NULL THEN mtr.id END) AS offline_no_record
//		`).
//		ColumnExpr(`
//			COUNT(DISTINCT CASE WHEN mcd.data_item_id = 'NO_DATA' THEN mtr.id END) AS offline_no_data
//		`).
//		ColumnExpr(`
//			COUNT(DISTINCT CASE WHEN mcd.meter_number IS NOT NULL AND mcd.data_item_id != 'NO_DATA' THEN mtr.id END) AS online
//		`).
//		Join(`
//			LEFT JOIN (
//				SELECT meter_number,
//					   MAX(data_item_id) AS data_item_id,
//					   MAX(consumption_date) AS last_consumption_date
//				FROM app.meter_consumption_daily
//				WHERE consumption_date BETWEEN ? AND ?
//				GROUP BY meter_number
//			) AS mcd
//			ON mtr.meter_number = mcd.meter_number
//		`, params.DateFrom, params.DateTo)
//
//	// Apply filters on meters table
//	for _, f := range filters {
//		// If the filter is for mcd.consumption_date, replace with last_consumption_date
//		if strings.Contains(strings.ToLower(f.Query), "consumption_date") {
//			f.Query = strings.ReplaceAll(f.Query, "mcd.consumption_date", "mcd.last_consumption_date")
//		}
//		q = q.Where(f.Query, f.Args...)
//	}
//
//	// Optional: dynamic filters on mcd fields (like last_consumption_date)
//	// Example: if params specify a date filter outside the join range
//	// q = q.Where("mcd.last_consumption_date BETWEEN ? AND ?", params.SomeFrom, params.SomeTo)
//
//	// Result struct
//	var row struct {
//		Online          int `bun:"online"`
//		OfflineNoRecord int `bun:"offline_no_record"`
//		OfflineNoData   int `bun:"offline_no_data"`
//	}
//
//	if err := q.Scan(ctx, &row); err != nil {
//		return nil, err
//	}
//
//	counts["online"] = row.Online
//	counts["offline_no_record"] = row.OfflineNoRecord
//	counts["offline_no_data"] = row.OfflineNoData
//	counts["total"] = row.Online + row.OfflineNoRecord + row.OfflineNoData
//
//	return counts, nil
//}

func (s *MeterService) GetMeterStatus(ctx context.Context, params models.ReadingFilterParams) ([]models.MeterStatusResult, error) {
	var results []models.MeterStatusResult

	q := s.db.NewSelect().
		TableExpr("app.meters AS mtr").
		// Select meter master data
		Column("mtr.meter_number").
		Column("mtr.meter_type").
		Column("mtr.region").
		Column("mtr.district").
		Column("mtr.boundary_metering_point").
		Column("mtr.station").
		Column("mtr.feeder_panel_name").
		Column("mtr.location").

		// Daily consumption fields
		Column("mcd.consumption_date").
		Column("mcd.consumption").
		Column("mcd.reading_count").
		Column("mcd.day_start_time").
		Column("mcd.day_end_time").

		// Computed status
		ColumnExpr(`
            CASE
                WHEN mcd.meter_number IS NULL THEN 'OFFLINE - No Record'
                WHEN mcd.data_item_id = 'NO_DATA' THEN 'OFFLINE - No Data'
                ELSE 'ONLINE'
            END AS status`).

		// Join by meter_number + date range
		Join(`
            LEFT JOIN app.meter_consumption_daily AS mcd
            ON mtr.meter_number = mcd.meter_number
            AND mcd.consumption_date BETWEEN ? AND ?
        `, params.DateFrom, params.DateTo)

	// Optional filtering
	if len(params.MeterNumber) > 0 {
		q = q.Where("mtr.meter_number IN (?)", bun.In(params.MeterNumber))
	}

	if len(params.Regions) > 0 {
		q = q.Where("mtr.region IN (?)", bun.In(params.Regions))
	}

	if len(params.Districts) > 0 {
		q = q.Where("mtr.district IN (?)", bun.In(params.Districts))
	}

	// Execute
	if err := q.Scan(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *MeterService) GetMeterStatusCounts(
	ctx context.Context,
	params models.ReadingFilterParams,
) (map[string]int, error) {

	counts := map[string]int{
		"total":             0,
		"online":            0,
		"offline_no_record": 0,
		"offline_no_data":   0,
	}

	filters := buildReadingFilters(params)

	// 🔥 Fixed query using BOOL_OR approach
	q := s.db.NewSelect().
		TableExpr("app.meters AS mtr").
		ColumnExpr(`
          COUNT(DISTINCT CASE WHEN mcd.meter_number IS NULL THEN mtr.id END) AS offline_no_record
       `).
		ColumnExpr(`
          COUNT(DISTINCT CASE WHEN mcd.has_actual_data = false THEN mtr.id END) AS offline_no_data
       `).
		ColumnExpr(`
          COUNT(DISTINCT CASE WHEN mcd.has_actual_data = true THEN mtr.id END) AS online
       `).
		Join(`
          LEFT JOIN (
             SELECT 
                meter_number,
                MAX(consumption_date) AS last_consumption_date,
                BOOL_OR(data_item_id != 'NO_DATA' AND data_item_id = '00100000') AS has_actual_data
             FROM app.meter_consumption_daily
             WHERE consumption_date BETWEEN ? AND ?
             GROUP BY meter_number
          ) AS mcd
          ON mtr.meter_number = mcd.meter_number
       `, params.DateFrom, params.DateTo)

	// Apply filters on meters table
	for _, f := range filters {
		// Skip date filters since they're already in the subquery
		if strings.Contains(strings.ToLower(f.Query), "consumption_date") {
			continue
		}
		q = q.Where(f.Query, f.Args...)
	}

	var row struct {
		Online          int `bun:"online"`
		OfflineNoRecord int `bun:"offline_no_record"`
		OfflineNoData   int `bun:"offline_no_data"`
	}

	if err := q.Scan(ctx, &row); err != nil {
		return nil, err
	}

	counts["online"] = row.Online
	counts["offline_no_record"] = row.OfflineNoRecord
	counts["offline_no_data"] = row.OfflineNoData
	counts["total"] = row.Online + row.OfflineNoRecord + row.OfflineNoData

	return counts, nil
}

///HELPER

func buildReadingFilters(params models.ReadingFilterParams) []Filter {
	filters := []Filter{}

	// Date range (required)
	filters = append(filters, Filter{
		Query: "mcd.consumption_date BETWEEN ? AND ?",
		Args:  []interface{}{params.DateFrom, params.DateTo},
	})

	// Meter numbers (now slice)
	if len(params.MeterNumber) > 0 {
		filters = append(filters, Filter{
			Query: "mcd.meter_number IN (?)",
			Args:  []interface{}{bun.In(params.MeterNumber)},
		})
	}

	// Region (lowercase)
	if len(params.Regions) > 0 {
		filters = append(filters, Filter{
			Query: "lower(mtr.region) IN (?)",
			Args:  []interface{}{bun.In(stringsToLower(params.Regions))},
		})
	}

	// District (lowercase)
	if len(params.Districts) > 0 {
		filters = append(filters, Filter{
			Query: "lower(mtr.district) IN (?)",
			Args:  []interface{}{bun.In(stringsToLower(params.Districts))},
		})
	}

	// Station (lowercase)
	if len(params.Stations) > 0 {
		filters = append(filters, Filter{
			Query: "lower(mtr.station) IN (?)",
			Args:  []interface{}{bun.In(stringsToLower(params.Stations))},
		})
	}

	// Location (lowercase)
	if len(params.Locations) > 0 {
		filters = append(filters, Filter{
			Query: "lower(mtr.location) IN (?)",
			Args:  []interface{}{bun.In(stringsToLower(params.Locations))},
		})
	}

	// Boundary metering point (lowercase)
	if len(params.BoundaryMeteringPoint) > 0 {
		filters = append(filters, Filter{
			Query: "lower(mtr.boundary_metering_point) IN (?)",
			Args:  []interface{}{bun.In(stringsToLower(params.BoundaryMeteringPoint))},
		})
	}

	// Meter type (uppercase)
	if len(params.MeterTypes) > 0 {
		filters = append(filters, Filter{
			Query: "mtr.meter_type IN (?)",
			Args:  []interface{}{bun.In(stringsToUpper(params.MeterTypes))},
		})
	}

	// Voltage (numeric)
	if len(params.Voltages) > 0 {
		filters = append(filters, Filter{
			Query: "mtr.voltage_kv IN (?)",
			Args:  []interface{}{bun.In(params.Voltages)},
		})
	}

	return filters
}

func stringsToLower(arr []string) []string {
	out := make([]string, len(arr))
	for i, v := range arr {
		out[i] = strings.ToLower(v)
	}
	return out
}

func stringsToUpper(arr []string) []string {
	out := make([]string, len(arr))
	for i, v := range arr {
		out[i] = strings.ToUpper(v)
	}
	return out
}
