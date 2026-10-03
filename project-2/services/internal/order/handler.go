// Package order implements the POST /orders endpoint: a transactional write
// followed by a fire-and-forget behavioral event for the data lake.
package order

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// Order is the transactional record (Postgres "orders" table).
type Order struct {
	OrderID    string
	CustomerID string
	Amount     float64
	CreatedAt  time.Time
}

// Event is the behavioral event sent to the stream (Kafka/Redpanda) and
// landed in the raw zone.
type Event struct {
	EventID    string  `json:"event_id"`
	EventType  string  `json:"event_type"`
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
	TS         string  `json:"ts"`
}

// Store persists orders. A failure here fails the request.
type Store interface {
	CreateOrder(ctx context.Context, o Order) error
}

// Publisher emits events. It must not block the caller and has no error
// return: event emission never fails the customer-facing write.
type Publisher interface {
	Publish(ctx context.Context, key string, value []byte)
}

type createRequest struct {
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
}

type Handler struct {
	Store     Store
	Publisher Publisher
	Now       func() time.Time
	Log       *slog.Logger
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	var req createRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if err := validate(req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	now := h.now()
	o := Order{
		OrderID:    uuid.NewString(),
		CustomerID: req.CustomerID,
		Amount:     req.Amount,
		CreatedAt:  now,
	}

	// Transactional write.
	if err := h.Store.CreateOrder(r.Context(), o); err != nil {
		h.log().Error("create order", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not create order"})
		return
	}

	// Behavioral event for the data lake: fire-and-forget relative to the response.
	payload, _ := json.Marshal(Event{
		EventID:    uuid.NewString(),
		EventType:  "order_placed",
		OrderID:    o.OrderID,
		CustomerID: o.CustomerID,
		Amount:     o.Amount,
		TS:         now.Format(time.RFC3339),
	})
	h.Publisher.Publish(context.WithoutCancel(r.Context()), o.CustomerID, payload)

	writeJSON(w, http.StatusCreated, map[string]string{"order_id": o.OrderID, "status": "created"})
}

func validate(req createRequest) error {
	if req.CustomerID == "" {
		return errors.New("customer_id is required")
	}
	if req.Amount <= 0 {
		return errors.New("amount must be positive")
	}
	return nil
}

func (h *Handler) now() time.Time {
	if h.Now != nil {
		return h.Now().UTC()
	}
	return time.Now().UTC()
}

func (h *Handler) log() *slog.Logger {
	if h.Log != nil {
		return h.Log
	}
	return slog.Default()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
