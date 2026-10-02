// Package events publishes behavioral events to Kafka/Redpanda, the local
// stand-in for the Kinesis "behavior-events" stream.
package events

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaPublisher struct {
	Client *kgo.Client
	Topic  string
	Log    *slog.Logger
}

// Publish produces asynchronously and only logs failures, so the caller's
// request is never blocked or failed by the event stream.
func (p *KafkaPublisher) Publish(ctx context.Context, key string, value []byte) {
	rec := &kgo.Record{Topic: p.Topic, Key: []byte(key), Value: value}
	p.Client.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		if err != nil {
			p.Log.Error("publish event", "topic", p.Topic, "err", err)
		}
	})
}
