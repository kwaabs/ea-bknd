package models

import (
	"encoding/json"
	"time"

	"github.com/uptrace/bun"
)

type Meter struct {
	bun.BaseModel `bun:"table:app.meters,alias:mtr"`

	ID                    string     `bun:",pk,type:uuid,default:uuid_generate_v4()" json:"id"`
	MeterNumber           string     `json:"meter_number"`
	MeterType             string     `json:"meter_type"`
	SPN                   *string    `json:"spn"`
	MeterBrand            *string    `json:"meter_brand"`
	Location              *string    `json:"location"`
	DigitalAddress        *string    `json:"digital_address"`
	Status                *string    `json:"status"`
	MeteringPoint         *string    `json:"metering_point"`
	BoundaryMeteringPoint *string    `json:"boundary_metering_point"`
	Incomer               *string    `json:"incomer"`
	Region                *string    `json:"region"`
	District              *string    `json:"district"`
	Station               *string    `json:"station"`
	CreatedAt             *time.Time `json:"created_at"`
	UpdatedAt             *time.Time `json:"updated_at"`
	MultiplyFactor        *float64   `json:"multiply_factor"`
	CTRatioPrimary        *float64   `json:"ct_ratio_primary"`
	CTRatioSecondary      *float64   `json:"ct_ratio_secondary"`
	VTRatioPrimary        *float64   `json:"vt_ratio_primary"`
	VTRatioSecondary      *float64   `json:"vt_ratio_secondary"`
	Latitude              *float64   `json:"latitude"`
	Longitude             *float64   `json:"longitude"`
	VoltageKV             *float64   `json:"voltage_kv"`
	FeederPanelName       *string    `json:"feeder_panel_name"`
}

type MeterReadingDaily struct {
	bun.BaseModel `bun:"table:meter_readings_daily,alias:mrd"`

	MeterNumber string            `bun:"meter_number,pk" json:"meter_number"`
	DataItemID  string            `bun:"data_item_id,pk" json:"data_item_id"`
	ReadingDate time.Time         `bun:"reading_date,pk" json:"reading_date"`
	TotalVal    float64           `bun:"total_val" json:"total_val"`
	RecordCount int               `bun:"record_count" json:"record_count"`
	Records     []json.RawMessage `bun:"records" json:"records"` // jsonb
	LastUpdate  time.Time         `bun:"last_update" json:"last_update"`
}

type AggregatedReading struct {
	TotalImportKWh  float64 `bun:"total_import_kwh" json:"total_import_kwh"`
	TotalExportKWh  float64 `bun:"total_export_kwh" json:"total_export_kwh"`
	TotalImportKVah float64 `bun:"total_import_kvah" json:"total_import_kvah"`
	TotalExportKVah float64 `bun:"total_export_kvah" json:"total_export_kvah"`
	TotalImportKVar float64 `bun:"total_import_kvar" json:"total_import_kvar"`
	TotalExportKVar float64 `bun:"total_export_kvar" json:"total_export_kvar"`
	MeterCount      int     `bun:"meter_count" json:"meter_count"`
	ReadingCount    int     `bun:"reading_count" json:"reading_count"`
}

type TimeSeriesReading struct {
	Date            time.Time `bun:"date" json:"date"`
	TotalImportKWh  float64   `bun:"total_import_kwh" json:"total_import_kwh"`
	TotalExportKWh  float64   `bun:"total_export_kwh" json:"total_export_kwh"`
	TotalImportKVah float64   `bun:"total_import_kvah" json:"total_import_kvah"`
	TotalExportKVah float64   `bun:"total_export_kvah" json:"total_export_kvah"`
	TotalImportKVar float64   `bun:"total_import_kvar" json:"total_import_kvar"`
	TotalExportKVar float64   `bun:"total_export_kvar" json:"total_export_kvar"`
	// optional: per meter type fields if stackByMeterType=true

	// dynamic stacked values: e.g., PSS_import_kwh, BSP_export_kvah
	Extra map[string]float64 `json:"-"`
}

// MarshalJSON allows Extra fields to be serialized dynamically
func (t TimeSeriesReading) MarshalJSON() ([]byte, error) {
	type Alias TimeSeriesReading
	aux := make(map[string]interface{})

	// copy static fields
	aux["date"] = t.Date
	aux["total_import_kwh"] = t.TotalImportKWh
	aux["total_export_kwh"] = t.TotalExportKWh
	aux["total_import_kvah"] = t.TotalImportKVah
	aux["total_export_kvah"] = t.TotalExportKVah
	aux["total_import_kvar"] = t.TotalImportKVar
	aux["total_export_kvar"] = t.TotalExportKVar

	// add dynamic stacked fields
	for k, v := range t.Extra {
		aux[k] = v
	}

	return json.Marshal(aux)
}

type ByMeterTypeReading struct {
	MeterType       string  `bun:"meter_type" json:"meter_type"`
	TotalImportKWh  float64 `bun:"total_import_kwh" json:"total_import_kwh"`
	TotalExportKWh  float64 `bun:"total_export_kwh" json:"total_export_kwh"`
	TotalImportKVah float64 `bun:"total_import_kvah" json:"total_import_kvah"`
	TotalExportKVah float64 `bun:"total_export_kvah" json:"total_export_kvah"`
	TotalImportKVar float64 `bun:"total_import_kvar" json:"total_import_kvar"`
	TotalExportKVar float64 `bun:"total_export_kvar" json:"total_export_kvar"`
	ReadingCount    int     `bun:"reading_count" json:"reading_count"`
}

type AggregatedResult struct {
	Aggregated  AggregatedReading    `json:"aggregated"`
	TimeSeries  []TimeSeriesReading  `json:"timeSeries"`
	ByMeterType []ByMeterTypeReading `json:"byMeterType,omitempty"`
	MeterTypes  []string             `json:"meterTypes,omitempty"`
}

type AggregatedQueryParams struct {
	DateFrom         string
	DateTo           string
	Regions          []string
	Districts        []string
	Stations         []string
	Voltages         []float64
	Locations        []string
	BoundaryPoints   []string
	MeterTypes       []string
	GroupBy          string // "day"
	StackByMeterType bool
}

type DailyConsumptionResults struct {
	ConsumptionDate       time.Time `bun:"consumption_date" json:"consumption_date"`
	MeterNumber           string    `bun:"meter_number" json:"meter_number"`
	DayStartReading       float64   `bun:"day_start_reading" json:"day_start_reading"`
	DayEndReading         float64   `bun:"day_end_reading" json:"day_end_reading"`
	ConsumedEnergy        float64   `bun:"consumed_energy" json:"consumed_energy"`
	SystemName            string    `bun:"system_name" json:"system_name"`
	Region                string    `bun:"region" json:"region,omitempty"`
	District              string    `bun:"district" json:"district,omitempty"`
	Station               string    `bun:"station" json:"station,omitempty"`
	Location              string    `bun:"location" json:"location,omitempty"`
	FeederPanelName       string    `bun:"feeder_panel_name" json:"feeder_panel_name,omitempty"`
	IC_OG                 string    `bun:"ic_og" json:"ic_og,omitempty"`
	VoltageKv             string    `bun:"voltage_kv" json:"voltage_kv,omitempty"`
	MeterType             string    `bun:"meter_type" json:"meter_type,omitempty"`
	BoundaryMeteringPoint string    `bun:"boundary_metering_point" json:"boundary_metering_point,omitempty"`
}

type ReadingFilterParams struct {
	MeterNumber           []string
	Regions               []string
	Districts             []string
	Stations              []string
	Locations             []string
	Voltages              []string
	BoundaryMeteringPoint []string
	MeterTypes            []string
	DateFrom              time.Time
	DateTo                time.Time
}

type AggregatedConsumptionResult struct {
	SystemName            *string `bun:"system_name" json:"system_name,omitempty"`
	BoundaryMeteringPoint *string `bun:"boundary_metering_point" json:"boundary_metering_point,omitempty"`
	Region                *string `bun:"region" json:"region,omitempty"`
	District              *string `bun:"district" json:"district,omitempty"`
	Station               *string `bun:"station" json:"station,omitempty"`
	FeederPanelName       *string `bun:"feeder_panel_name" json:"feeder_panel_name,omitempty"`
	Location              string  `bun:"location" json:"location,omitempty"`
	IC_OG                 string  `bun:"ic_og" json:"ic_og,omitempty"`

	ActiveMeters          int `bun:"active_meters" json:"active_meters,omitempty"`
	TotalMeterCount       int `bun:"total_meter_count" json:"total_meter_count,omitempty"`
	TotalMetersByRegion   int `bun:"total_meters_by_region" json:"total_meters_by_region,omitempty"`
	TotalMetersByDistrict int `bun:"total_meters_by_district" json:"total_meters_by_district,omitempty"`
	AllMetersCount        int `bun:"all_meters_count" json:"all_meters_count,omitempty"`

	MeterType        *string   `bun:"meter_type" json:"meter_type,omitempty"`
	GroupPeriod      time.Time `bun:"group_period" json:"group_period"`
	TotalConsumption float64   `bun:"total_consumption" json:"total_consumption"`
}
