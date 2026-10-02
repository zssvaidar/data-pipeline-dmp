package events

import (
	"context"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

type KinesisPublisher struct {
	Client  *kinesis.Client
	Stream  string
	Timeout time.Duration
	Log     *slog.Logger
}

// Publish sends synchronously but with a short timeout and only logs
// failures. A Lambda is frozen once it returns, so a background goroutine
// would not reliably deliver; bounding the call keeps the API fast instead.
//
// Each record ends in a newline because Firehose concatenates records
// as-is, and the raw files must be one JSON object per line.
func (p *KinesisPublisher) Publish(ctx context.Context, key string, value []byte) {
	timeout := p.Timeout
	if timeout == 0 {
		timeout = 500 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := p.Client.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   aws.String(p.Stream),
		PartitionKey: aws.String(key),
		Data:         append(value[:len(value):len(value)], '\n'),
	})
	if err != nil {
		p.Log.Error("publish event", "stream", p.Stream, "err", err)
	}
}
