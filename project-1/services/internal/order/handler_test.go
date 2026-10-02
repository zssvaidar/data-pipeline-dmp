package order

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeStore struct {
	orders []Order
	err    error
}

func (s *fakeStore) CreateOrder(_ context.Context, o Order) error {
	if s.err != nil {
		return s.err
	}
	s.orders = append(s.orders, o)
	return nil
}

type published struct {
	key   string
	value []byte
}

type fakePublisher struct{ msgs []published }

func (p *fakePublisher) Publish(_ context.Context, key string, value []byte) {
	p.msgs = append(p.msgs, published{key, value})
}

var fixedNow = time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)

func newHandler(store *fakeStore, pub *fakePublisher) *Handler {
	return &Handler{Store: store, Publisher: pub, Now: func() time.Time { return fixedNow }}
}

func do(h http.Handler, method, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, "/orders", strings.NewReader(body)))
	return rec
}

func TestCreateOrder(t *testing.T) {
	store, pub := &fakeStore{}, &fakePublisher{}
	rec := do(newHandler(store, pub), http.MethodPost, `{"customer_id":"c-1","amount":12.5}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(store.orders) != 1 {
		t.Fatalf("stored %d orders, want 1", len(store.orders))
	}
	o := store.orders[0]
	if resp["order_id"] != o.OrderID || o.CustomerID != "c-1" || o.Amount != 12.5 || !o.CreatedAt.Equal(fixedNow) {
		t.Fatalf("unexpected order %+v / response %v", o, resp)
	}

	if len(pub.msgs) != 1 {
		t.Fatalf("published %d events, want 1", len(pub.msgs))
	}
	if pub.msgs[0].key != "c-1" {
		t.Errorf("partition key = %q, want customer id", pub.msgs[0].key)
	}
	var ev Event
	if err := json.Unmarshal(pub.msgs[0].value, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.EventType != "order_placed" || ev.OrderID != o.OrderID || ev.EventID == "" || ev.TS != "2026-10-02T12:00:00Z" {
		t.Errorf("unexpected event %+v", ev)
	}
}

func TestCreateOrderRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"malformed":        `{"customer_id":`,
		"missing customer": `{"amount":5}`,
		"zero amount":      `{"customer_id":"c-1","amount":0}`,
		"negative amount":  `{"customer_id":"c-1","amount":-3}`,
		"unknown field":    `{"customer_id":"c-1","amount":3,"extra":true}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			store, pub := &fakeStore{}, &fakePublisher{}
			rec := do(newHandler(store, pub), http.MethodPost, body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
			if len(store.orders) != 0 || len(pub.msgs) != 0 {
				t.Fatal("bad input must not write or publish")
			}
		})
	}
}

func TestStoreFailureDoesNotPublish(t *testing.T) {
	store, pub := &fakeStore{err: errors.New("db down")}, &fakePublisher{}
	rec := do(newHandler(store, pub), http.MethodPost, `{"customer_id":"c-1","amount":1}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if len(pub.msgs) != 0 {
		t.Fatal("an event was published for an order that was never written")
	}
}

func TestMethodNotAllowed(t *testing.T) {
	rec := do(newHandler(&fakeStore{}, &fakePublisher{}), http.MethodGet, "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}
