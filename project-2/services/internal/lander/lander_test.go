package lander

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type memSink struct {
	objects map[string]string
	err     error
}

func (s *memSink) Put(_ context.Context, key string, body []byte) error {
	if s.err != nil {
		return s.err
	}
	if s.objects == nil {
		s.objects = map[string]string{}
	}
	s.objects[key] = string(body)
	return nil
}

var t0 = time.Date(2026, 10, 2, 9, 30, 0, 0, time.UTC)

func TestFlushOnAge(t *testing.T) {
	sink := &memSink{}
	l := &Lander{Sink: sink, Prefix: "raw/behavior-events", MaxBytes: 1 << 20, MaxAge: time.Minute}

	if l.ShouldFlush(t0) {
		t.Fatal("empty buffer should not flush")
	}
	if err := l.Add([]byte("{\n  \"a\": 1\n}"), t0); err != nil {
		t.Fatal(err)
	}
	if err := l.Add([]byte(`{"a":2}`), t0.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if l.ShouldFlush(t0.Add(59 * time.Second)) {
		t.Fatal("flushed before MaxAge")
	}
	if !l.ShouldFlush(t0.Add(time.Minute)) {
		t.Fatal("did not flush at MaxAge")
	}

	key, err := l.Flush(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(key, "raw/behavior-events/year=2026/month=10/day=02/behavior-events-20261002T093000Z-") {
		t.Errorf("unexpected key %q", key)
	}
	if got, want := sink.objects[key], "{\"a\":1}\n{\"a\":2}\n"; got != want {
		t.Errorf("body = %q, want %q (one compact JSON record per line)", got, want)
	}
	if l.Len() != 0 {
		t.Error("buffer not cleared after flush")
	}
}

func TestFlushOnSize(t *testing.T) {
	l := &Lander{Sink: &memSink{}, Prefix: "p", MaxBytes: 16, MaxAge: time.Hour}
	_ = l.Add([]byte(`{"k":"0123456789"}`), t0)
	if !l.ShouldFlush(t0) {
		t.Fatal("did not flush at MaxBytes")
	}
}

func TestFailedFlushKeepsRecords(t *testing.T) {
	sink := &memSink{err: errors.New("s3 unavailable")}
	l := &Lander{Sink: sink, Prefix: "p", MaxBytes: 1 << 20, MaxAge: time.Minute}
	_ = l.Add([]byte(`{"a":1}`), t0)

	if _, err := l.Flush(context.Background()); err == nil {
		t.Fatal("expected error")
	}
	if l.Len() != 1 {
		t.Fatal("records dropped after failed flush")
	}

	sink.err = nil
	key, err := l.Flush(context.Background())
	if err != nil || sink.objects[key] != "{\"a\":1}\n" {
		t.Fatalf("retry failed: key=%q err=%v objects=%v", key, err, sink.objects)
	}
}

func TestRejectsInvalidJSON(t *testing.T) {
	l := &Lander{Sink: &memSink{}, Prefix: "p", MaxBytes: 1 << 20, MaxAge: time.Minute}
	if err := l.Add([]byte("not json"), t0); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	if l.Len() != 0 {
		t.Fatal("invalid record was buffered")
	}
}
