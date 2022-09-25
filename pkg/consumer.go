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

func (c *Consumer) GetMessages(ctx context.Context, url string) ([]types.Message, error) {
	i := sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(url),
		MaxNumberOfMessages: 10,
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

func New(c Client) *Consumer {
	return &Consumer{
		client: c,
	}
}
