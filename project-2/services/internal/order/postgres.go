package order

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore writes orders to the Postgres orders table.
type PostgresStore struct {
	Pool *pgxpool.Pool
}

func (s *PostgresStore) CreateOrder(ctx context.Context, o Order) error {
	_, err := s.Pool.Exec(ctx,
		`INSERT INTO orders (order_id, customer_id, amount, created_at) VALUES ($1, $2, $3, $4)`,
		o.OrderID, o.CustomerID, o.Amount, o.CreatedAt)
	return err
}
