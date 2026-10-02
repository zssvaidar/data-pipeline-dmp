// Package activate builds the high-value customer segment with Athena and
// "activates" it: writes it to the lake and notifies downstream consumers.
package activate

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

//go:embed segment_athena.sql
var segmentSQL string

const Segment = "high_value_customers"

// Querier runs a parameterized SQL query and returns the data rows (no header).
type Querier interface {
	Query(ctx context.Context, sql string, params []string) ([][]string, error)
}

type Sink interface {
	Put(ctx context.Context, key string, body []byte) error
}

// Notifier tells downstream systems (ad platform, CRM, ...) a segment is ready.
type Notifier interface {
	Notify(ctx context.Context, message []byte) error
}

type Customer struct {
	CustomerID string  `json:"customer_id"`
	TotalSpend float64 `json:"total_spend"`
	OrderCount int     `json:"order_count"`
}

type Activator struct {
	Querier    Querier
	Sink       Sink
	Notifier   Notifier // optional
	Database   string
	Table      string
	Bucket     string
	WindowDays int
	MinSpend   float64
}

type Result struct {
	Key       string
	Customers []Customer
}

func (a *Activator) Run(ctx context.Context, asOf time.Time) (Result, error) {
	sql, params := a.query(asOf)
	rows, err := a.Querier.Query(ctx, sql, params)
	if err != nil {
		return Result{}, fmt.Errorf("segment query: %w", err)
	}
	customers, err := parseRows(rows)
	if err != nil {
		return Result{}, err
	}

	// One JSON customer per line, the same layout as the local pipeline.
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, c := range customers {
		if err := enc.Encode(c); err != nil {
			return Result{}, err
		}
	}
	date := asOf.UTC().Format(time.DateOnly)
	key := fmt.Sprintf("activation/%s/dt=%s/segment.json", Segment, date)
	if err := a.Sink.Put(ctx, key, body.Bytes()); err != nil {
		return Result{}, fmt.Errorf("write segment: %w", err)
	}

	if a.Notifier != nil {
		// Notify with a pointer, not the payload: SNS messages are capped at 256 KB.
		msg, _ := json.Marshal(map[string]any{
			"segment":   Segment,
			"as_of":     date,
			"customers": len(customers),
			"location":  fmt.Sprintf("s3://%s/%s", a.Bucket, key),
		})
		if err := a.Notifier.Notify(ctx, msg); err != nil {
			return Result{}, fmt.Errorf("notify: %w", err)
		}
	}
	return Result{Key: key, Customers: customers}, nil
}

// query returns the Athena SQL and its execution parameters. Athena
// substitutes parameters as SQL literals, so strings carry their quotes.
func (a *Activator) query(asOf time.Time) (string, []string) {
	end := asOf.UTC()
	start := end.AddDate(0, 0, -(a.WindowDays - 1))
	sql := fmt.Sprintf(segmentSQL, quoteIdent(a.Database), quoteIdent(a.Table))
	return sql, []string{
		quoteLiteral(start.Format(time.DateOnly)),
		quoteLiteral(end.Format(time.DateOnly)),
		strconv.FormatFloat(a.MinSpend, 'f', -1, 64),
	}
}

func parseRows(rows [][]string) ([]Customer, error) {
	customers := make([]Customer, 0, len(rows))
	for i, r := range rows {
		if len(r) != 3 {
			return nil, fmt.Errorf("row %d: got %d columns, want 3", i, len(r))
		}
		spend, err := strconv.ParseFloat(r[1], 64)
		if err != nil {
			return nil, fmt.Errorf("row %d total_spend: %w", i, err)
		}
		count, err := strconv.Atoi(r[2])
		if err != nil {
			return nil, fmt.Errorf("row %d order_count: %w", i, err)
		}
		customers = append(customers, Customer{CustomerID: r[0], TotalSpend: spend, OrderCount: count})
	}
	return customers, nil
}

func quoteIdent(s string) string {
	return `"` + string(bytes.ReplaceAll([]byte(s), []byte(`"`), []byte(`""`))) + `"`
}

func quoteLiteral(s string) string {
	return "'" + string(bytes.ReplaceAll([]byte(s), []byte("'"), []byte("''"))) + "'"
}
