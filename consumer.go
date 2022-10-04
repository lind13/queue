package queue

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go/aws"
)

type Consumer struct {
	client Client
}

func (c *Consumer) GetMessages(ctx context.Context, url string, batchSize int32) ([]types.Message, error) {
	i := sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(url),
		MaxNumberOfMessages:   batchSize,
		MessageAttributeNames: []string{"."},
	}
	o, err := c.client.ReceiveMessage(ctx, &i)
	if err != nil {
		return nil, err
	}

	return o.Messages, nil
}

func (c *Consumer) DeleteMessage(ctx context.Context, url string, receiptHandle *string) error {
	i := sqs.DeleteMessageInput{
		QueueUrl:      aws.String(url),
		ReceiptHandle: receiptHandle,
	}
	_, err := c.client.DeleteMessage(ctx, &i)
	if err != nil {
		return err
	}
	return nil
}

func (c *Consumer) DeleteBatch(ctx context.Context, url string, entries []types.DeleteMessageBatchRequestEntry) (*sqs.DeleteMessageBatchOutput, error) {
	i := sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(url),
		Entries:  entries,
	}

	return c.client.DeleteMessageBatch(ctx, &i)
}

func NewConsumer(c Client) *Consumer {
	return &Consumer{
		client: c,
	}
}
