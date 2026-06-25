package services

import (
	"bknd-1/internal/models"
	"context"
	"strings"

	"github.com/uptrace/bun"
)

type MMSCustomerSalesService struct {
	db *bun.DB
}

func NewMMSCustomerSalesService(db *bun.DB) *MMSCustomerSalesService {
	return &MMSCustomerSalesService{db: db}
}

func (s *MMSCustomerSalesService) buildFilters(q *bun.SelectQuery, params models.MMSCustomerSalesFilterParams) *bun.SelectQuery {
	if len(params.Region) > 0 {
		q = q.Where("lower(region) IN (?)", bun.In(stringsToLower(params.Region)))
	}
	if len(params.District) > 0 {
		q = q.Where("lower(district) IN (?)", bun.In(stringsToLower(params.District)))
	}
	if len(params.ContractType) > 0 {
		q = q.Where("lower(contract_type) IN (?)", bun.In(stringsToLower(params.ContractType)))
	}
	if len(params.Tariff) > 0 {
		q = q.Where("lower(tariff) IN (?)", bun.In(stringsToLower(params.Tariff)))
	}
	if len(params.Manufacturer) > 0 {
		q = q.Where("lower(manufacturer) IN (?)", bun.In(stringsToLower(params.Manufacturer)))
	}
	if len(params.Model) > 0 {
		q = q.Where("lower(model) IN (?)", bun.In(stringsToLower(params.Model)))
	}
	if len(params.AccountNumber) > 0 {
		q = q.Where("account_number IN (?)", bun.In(params.AccountNumber))
	}
	if len(params.MeterNumber) > 0 {
		q = q.Where("meter_number IN (?)", bun.In(params.MeterNumber))
	}
	if params.Search != "" {
		search := "%" + strings.ToLower(strings.TrimSpace(params.Search)) + "%"
		q = q.Where(
			"(lower(customer_name) LIKE ? OR lower(account_number::text) LIKE ? OR lower(meter_number::text) LIKE ? OR lower(meter_serial_number::text) LIKE ?)",
			search, search, search, search,
		)
	}
	if !params.DateTimeFrom.IsZero() {
		q = q.Where("date_time >= ?", params.DateTimeFrom)
	}
	if !params.DateTimeTo.IsZero() {
		q = q.Where("date_time <= ?", params.DateTimeTo)
	}
	return q
}

func (s *MMSCustomerSalesService) GetDetail(
	ctx context.Context,
	params models.MMSCustomerSalesFilterParams,
) (*models.MMSCustomerSalesDetailResult, error) {

	if params.Page < 1 {
		params.Page = 1
	}
	if params.Limit < 1 {
		params.Limit = 50
	}
	if params.Limit > 500 {
		params.Limit = 500 // prevent abuse
	}

	offset := (params.Page - 1) * params.Limit

	var total int
	countQ := s.db.NewSelect().TableExpr("app.mms_customer_sales")
	countQ = s.buildFilters(countQ, params)

	if err := countQ.ColumnExpr("COUNT(*)").Scan(ctx, &total); err != nil {
		return nil, err
	}

	var data []models.MMSCustomerSales
	dataQ := s.db.NewSelect().TableExpr("app.mms_customer_sales")
	dataQ = s.buildFilters(dataQ, params)

	if err := dataQ.
		ColumnExpr("*").
		ColumnExpr("'MMS Sales' AS data_src").
		OrderExpr("region, district, customer_name, account_number"). // stable sort
		Limit(params.Limit).
		Offset(offset).
		Scan(ctx, &data); err != nil {
		return nil, err
	}

	totalPages := (total + params.Limit - 1) / params.Limit

	return &models.MMSCustomerSalesDetailResult{
		Data:       data,
		Total:      total,
		Page:       params.Page,
		Limit:      params.Limit,
		TotalPages: totalPages,
	}, nil
}

func (s *MMSCustomerSalesService) GetAggregate(
	ctx context.Context,
	params models.MMSCustomerSalesFilterParams,
	groupBy []string,
) (*models.MMSCustomerSalesAggregateResult, error) {

	validGroupBy := map[string]bool{
		"region":        true,
		"district":      true,
		"contract_type": true,
		"tariff":        true,
		"manufacturer":  true,
		"model":         true,
	}

	q := s.db.NewSelect().TableExpr("app.mms_customer_sales")
	q = s.buildFilters(q, params)

	q = q.
		ColumnExpr("'MMS Sales' AS data_src").
		ColumnExpr("COUNT(*) AS customer_count").
		ColumnExpr("COALESCE(ROUND(SUM(sts_credit_balance_remaining)::numeric, 2), 0) AS sum_credit_balance_remaining").
		ColumnExpr("COALESCE(ROUND(SUM(sts_last_month_credit_read)::numeric, 2), 0) AS sum_last_month_credit_read").
		ColumnExpr("COALESCE(ROUND(SUM(sts_last_month_kwh_read)::numeric, 2), 0) AS sum_last_month_kwh_read")

	var validGroups []string
	for _, g := range groupBy {
		g = strings.ToLower(strings.TrimSpace(g))
		if !validGroupBy[g] {
			continue
		}
		validGroups = append(validGroups, g)
		q = q.ColumnExpr(g).GroupExpr(g)
	}

	if len(validGroups) > 0 {
		q = q.OrderExpr(strings.Join(validGroups, ", "))
	}

	var data []models.MMSCustomerSalesAggregateRow
	if err := q.Scan(ctx, &data); err != nil {
		return nil, err
	}

	return &models.MMSCustomerSalesAggregateResult{
		Data:  data,
		Total: len(data), // number of groups
	}, nil
}
