// Command lander consumes the behavior-events topic and lands it in the raw
// zone: the local stand-in for Kinesis Data Firehose -> S3.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/zssvaidar/data-pipeline-dmp/services/internal/lander"
	"github.com/zssvaidar/data-pipeline-dmp/services/internal/objstore"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("lander exited", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := objstore.New(ctx, os.Getenv("S3_ENDPOINT"), env("S3_BUCKET", "data-lake"))
	if err != nil {
		return err
	}
	if err := retry(ctx, log, "ensure bucket", func() error { return store.EnsureBucket(ctx) }); err != nil {
		return err
	}

	maxAge, err := time.ParseDuration(env("FLUSH_INTERVAL", "60s"))
	if err != nil {
		return err
	}
	maxBytes, err := strconv.Atoi(env("FLUSH_BYTES", strconv.Itoa(8<<20)))
	if err != nil {
		return err
	}
	l := &lander.Lander{Sink: store, Prefix: env("RAW_PREFIX", "raw/behavior-events"), MaxBytes: maxBytes, MaxAge: maxAge}

	topic := env("EVENTS_TOPIC", "behavior-events")
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(env("KAFKA_BROKERS", "localhost:19092"), ",")...),
		kgo.ConsumerGroup(env("CONSUMER_GROUP", "firehose-s3")),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Offsets are committed only after the records are durably in S3,
		// giving at-least-once delivery into the raw zone.
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	log.Info("landing", "topic", topic, "bucket", store.Bucket, "prefix", l.Prefix, "flush_interval", maxAge, "flush_bytes", maxBytes)

	flush := func(ctx context.Context) error {
		n := l.Len()
		key, err := l.Flush(ctx)
		if err != nil {
			return err
		}
		if err := client.CommitUncommittedOffsets(ctx); err != nil {
			// The object is already written; a lost commit only means these
			// records may be landed again (duplicates, not data loss).
			log.Warn("commit offsets", "err", err)
		}
		log.Info("landed batch", "key", key, "records", n)
		return nil
	}

	for {
		pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if deadline, ok := l.Deadline(); ok {
			cancel()
			pollCtx, cancel = context.WithDeadline(ctx, deadline)
		}
		fetches := client.PollFetches(pollCtx)
		cancel()

		if ctx.Err() != nil {
			// Shutting down: land whatever is buffered.
			finalCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if l.Len() > 0 {
				return flush(finalCtx)
			}
			return nil
		}

		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				log.Error("fetch", "topic", t, "partition", p, "err", err)
			}
		})
		now := time.Now()
		fetches.EachRecord(func(r *kgo.Record) {
			if err := l.Add(r.Value, now); err != nil {
				log.Warn("skipping record", "partition", r.Partition, "offset", r.Offset, "err", err)
			}
		})

		if l.ShouldFlush(time.Now()) {
			if err := flush(ctx); err != nil {
				// Keep the buffer and offsets; retry on the next loop.
				log.Error("flush", "err", err)
				sleep(ctx, 2*time.Second)
			}
		}
	}
}

func retry(ctx context.Context, log *slog.Logger, what string, fn func() error) error {
	var err error
	for attempt := 1; attempt <= 30; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		log.Warn(what+" failed, retrying", "attempt", attempt, "err", err)
		if !sleep(ctx, 2*time.Second) {
			return ctx.Err()
		}
	}
	return err
}

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
