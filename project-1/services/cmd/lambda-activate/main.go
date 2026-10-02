// Command lambda-activate runs after each successful Glue ETL run (an
// EventBridge "Glue Job State Change" rule): it builds the segment on Athena,
// writes it to the lake and announces it on SNS.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/zssvaidar/data-pipeline-dmp/services/internal/activate"
	"github.com/zssvaidar/data-pipeline-dmp/services/internal/objstore"
)

type snsNotifier struct {
	client *sns.Client
	topic  string
}

func (n *snsNotifier) Notify(ctx context.Context, msg []byte) error {
	_, err := n.client.Publish(ctx, &sns.PublishInput{TopicArn: aws.String(n.topic), Message: aws.String(string(msg))})
	return err
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Error("load AWS config", "err", err)
		os.Exit(1)
	}

	windowDays, _ := strconv.Atoi(env("WINDOW_DAYS", "30"))
	minSpend, _ := strconv.ParseFloat(env("MIN_SPEND", "10000"), 64)
	bucket := os.Getenv("LAKE_BUCKET")

	a := &activate.Activator{
		Querier:    &activate.Athena{Client: athena.NewFromConfig(cfg), WorkGroup: os.Getenv("ATHENA_WORKGROUP")},
		Sink:       &objstore.S3{Client: s3.NewFromConfig(cfg), Bucket: bucket},
		Database:   env("CURATED_DATABASE", "curated_db"),
		Table:      env("CURATED_TABLE", "order_events"),
		Bucket:     bucket,
		WindowDays: windowDays,
		MinSpend:   minSpend,
	}
	if topic := os.Getenv("SEGMENT_TOPIC_ARN"); topic != "" {
		a.Notifier = &snsNotifier{client: sns.NewFromConfig(cfg), topic: topic}
	}

	lambda.Start(func(ctx context.Context, ev events.CloudWatchEvent) error {
		var detail struct {
			JobName  string `json:"jobName"`
			JobRunID string `json:"jobRunId"`
			State    string `json:"state"`
		}
		_ = json.Unmarshal(ev.Detail, &detail)
		log.Info("triggered", "source", ev.Source, "job", detail.JobName, "run", detail.JobRunID, "state", detail.State)

		res, err := a.Run(ctx, time.Now())
		if err != nil {
			return err
		}
		log.Info("segment activated", "key", res.Key, "customers", len(res.Customers))
		return nil
	})
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
