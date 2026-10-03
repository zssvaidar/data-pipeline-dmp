// Package lander buffers stream records and writes them to the raw zone as newline-delimited JSON
// objects, partitioned by arrival time, flushing on size or age.
package lander

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Sink stores one object (MinIO).
type Sink interface {
	Put(ctx context.Context, key string, body []byte) error
}

type Lander struct {
	Sink Sink
	// Prefix is the raw-zone path, e.g. "raw/behavior-events".
	Prefix   string
	MaxBytes int
	MaxAge   time.Duration

	buf    bytes.Buffer
	count  int
	opened time.Time
}

// Add appends one record to the buffer. Records that are not valid JSON are
// rejected so a single bad producer cannot corrupt a whole raw file.
func (l *Lander) Add(record []byte, now time.Time) error {
	var line bytes.Buffer
	if err := json.Compact(&line, record); err != nil {
		return fmt.Errorf("record is not valid JSON: %w", err)
	}
	if l.count == 0 {
		l.opened = now
	}
	l.buf.Write(line.Bytes())
	l.buf.WriteByte('\n')
	l.count++
	return nil
}

func (l *Lander) Len() int { return l.count }

// ShouldFlush reports whether the buffer hit its size or age limit.
func (l *Lander) ShouldFlush(now time.Time) bool {
	if l.count == 0 {
		return false
	}
	return l.buf.Len() >= l.MaxBytes || now.Sub(l.opened) >= l.MaxAge
}

// Deadline is when the current buffer must be flushed by age.
func (l *Lander) Deadline() (time.Time, bool) {
	if l.count == 0 {
		return time.Time{}, false
	}
	return l.opened.Add(l.MaxAge), true
}

// Flush writes the buffer as one object and clears it. On error the buffer
// is kept so the caller can retry without losing records.
func (l *Lander) Flush(ctx context.Context) (string, error) {
	if l.count == 0 {
		return "", nil
	}
	key := ObjectKey(l.Prefix, l.opened, uuid.NewString())
	if err := l.Sink.Put(ctx, key, l.buf.Bytes()); err != nil {
		return "", err
	}
	l.buf.Reset()
	l.count = 0
	return key, nil
}

// ObjectKey builds a Hive-style partitioned key:
// <prefix>/year=YYYY/month=MM/day=DD/behavior-events-<time>-<id>.json
func ObjectKey(prefix string, t time.Time, id string) string {
	t = t.UTC()
	return fmt.Sprintf("%s/year=%04d/month=%02d/day=%02d/behavior-events-%s-%s.json",
		prefix, t.Year(), t.Month(), t.Day(), t.Format("20060102T150405Z"), id)
}
