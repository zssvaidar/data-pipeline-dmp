// Command lambda-api is the AWS deployment of the order API:
// API Gateway (HTTP API) -> Lambda -> DynamoDB + Kinesis.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	"github.com/zssvaidar/data-pipeline-dmp/services/internal/events"
	"github.com/zssvaidar/data-pipeline-dmp/services/internal/lambdahttp"
	"github.com/zssvaidar/data-pipeline-dmp/services/internal/order"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Clients are created once per execution environment and reused across invocations.
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Error("load AWS config", "err", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.Handle("/orders", &order.Handler{
		Store:     &order.DynamoStore{Client: dynamodb.NewFromConfig(cfg), Table: os.Getenv("ORDERS_TABLE")},
		Publisher: &events.KinesisPublisher{Client: kinesis.NewFromConfig(cfg), Stream: os.Getenv("EVENTS_STREAM"), Log: log},
		Log:       log,
	})
	lambda.Start(lambdahttp.Handler(mux))
}
