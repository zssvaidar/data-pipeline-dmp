package order

import (
	"context"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DynamoStore writes orders to the DynamoDB Orders table.
type DynamoStore struct {
	Client *dynamodb.Client
	Table  string
}

func (s *DynamoStore) CreateOrder(ctx context.Context, o Order) error {
	_, err := s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.Table),
		Item: map[string]types.AttributeValue{
			"order_id":    &types.AttributeValueMemberS{Value: o.OrderID},
			"customer_id": &types.AttributeValueMemberS{Value: o.CustomerID},
			"amount":      &types.AttributeValueMemberN{Value: strconv.FormatFloat(o.Amount, 'f', 2, 64)},
			"created_at":  &types.AttributeValueMemberS{Value: o.CreatedAt.UTC().Format(time.RFC3339)},
		},
		// Never overwrite an existing order on a (vanishingly unlikely) id collision.
		ConditionExpression: aws.String("attribute_not_exists(order_id)"),
	})
	return err
}
