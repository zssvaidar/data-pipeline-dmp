package activate

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

type fakeQuerier struct {
	sql    string
	params []string
	rows   [][]string
	err    error
}

func (q *fakeQuerier) Query(_ context.Context, sql string, params []string) ([][]string, error) {
	q.sql, q.params = sql, params
	return q.rows, q.err
}

type memSink map[string]string

func (s memSink) Put(_ context.Context, key string, body []byte) error {
	s[key] = string(body)
	return nil
}

type fakeNotifier struct{ msgs []string }

func (n *fakeNotifier) Notify(_ context.Context, m []byte) error {
	n.msgs = append(n.msgs, string(m))
	return nil
}

var asOf = time.Date(2026, 10, 2, 3, 0, 0, 0, time.UTC)

func newActivator(q Querier, s Sink, n Notifier) *Activator {
	return &Activator{Querier: q, Sink: s, Notifier: n, Database: "curated_db", Table: "order_events",
		Bucket: "lake", WindowDays: 30, MinSpend: 10000}
}

func TestRun(t *testing.T) {
	q := &fakeQuerier{rows: [][]string{{"whale-3", "34427.93", "10"}, {"whale-1", "29829.41", "8"}}}
	sink, notifier := memSink{}, &fakeNotifier{}

	res, err := newActivator(q, sink, notifier).Run(context.Background(), asOf)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(q.sql, `FROM "curated_db"."order_events"`) {
		t.Errorf("table not substituted:\n%s", q.sql)
	}
	if want := []string{"'2026-09-03'", "'2026-10-02'", "10000"}; !reflect.DeepEqual(q.params, want) {
		t.Errorf("params = %v, want %v (30-day window inclusive of as_of)", q.params, want)
	}

	wantKey := "activation/high_value_customers/dt=2026-10-02/segment.json"
	if res.Key != wantKey {
		t.Errorf("key = %q", res.Key)
	}
	wantBody := `{"customer_id":"whale-3","total_spend":34427.93,"order_count":10}` + "\n" +
		`{"customer_id":"whale-1","total_spend":29829.41,"order_count":8}` + "\n"
	if sink[wantKey] != wantBody {
		t.Errorf("body = %q", sink[wantKey])
	}

	if len(notifier.msgs) != 1 {
		t.Fatalf("notified %d times", len(notifier.msgs))
	}
	var msg map[string]any
	_ = json.Unmarshal([]byte(notifier.msgs[0]), &msg)
	if msg["location"] != "s3://lake/"+wantKey || msg["customers"] != float64(2) {
		t.Errorf("unexpected notification %v", msg)
	}
}

func TestRunEmptySegmentStillWritesAndNotifies(t *testing.T) {
	sink, notifier := memSink{}, &fakeNotifier{}
	if _, err := newActivator(&fakeQuerier{}, sink, notifier).Run(context.Background(), asOf); err != nil {
		t.Fatal(err)
	}
	if len(sink) != 1 || len(notifier.msgs) != 1 {
		t.Fatal("an empty segment must still be published so consumers see today's run")
	}
}

func TestRunErrors(t *testing.T) {
	cases := map[string]*fakeQuerier{
		"query fails": {err: errors.New("boom")},
		"bad number":  {rows: [][]string{{"c", "abc", "1"}}},
		"bad shape":   {rows: [][]string{{"c", "1"}}},
	}
	for name, q := range cases {
		t.Run(name, func(t *testing.T) {
			sink, notifier := memSink{}, &fakeNotifier{}
			if _, err := newActivator(q, sink, notifier).Run(context.Background(), asOf); err == nil {
				t.Fatal("expected error")
			}
			if len(sink) != 0 || len(notifier.msgs) != 0 {
				t.Fatal("nothing may be published when the query fails")
			}
		})
	}
}

func TestQuoting(t *testing.T) {
	if got := quoteIdent(`a"b`); got != `"a""b"` {
		t.Errorf("quoteIdent = %s", got)
	}
	if got := quoteLiteral(`it's`); got != `'it''s'` {
		t.Errorf("quoteLiteral = %s", got)
	}
}
