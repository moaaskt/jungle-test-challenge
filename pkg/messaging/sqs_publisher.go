package messaging

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/service"
)

type sqsPublisher struct {
	client   *sqs.Client
	queueURL string
}

// NewSQSPublisher cria um publicador de eventos da outbox despachando para a fila SQS FIFO de saída.
func NewSQSPublisher(client *sqs.Client, queueURL string) service.EventPublisher {
	return &sqsPublisher{
		client:   client,
		queueURL: queueURL,
	}
}

// Publish envia o evento com envelope estável, particionando por aggregateId e deduplicando por eventId.
func (p *sqsPublisher) Publish(ctx context.Context, event *domain.OutboxEvent) error {
	msgGroupID := event.AggregateID
	if msgGroupID == "" {
		msgGroupID = "default-group"
	}
	dedupID := event.ID.String()

	input := &sqs.SendMessageInput{
		QueueUrl:               aws.String(p.queueURL),
		MessageBody:            aws.String(string(event.Payload)),
		MessageGroupId:         aws.String(msgGroupID),
		MessageDeduplicationId: aws.String(dedupID),
	}

	_, err := p.client.SendMessage(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to send outbox event to SQS queue %s: %w", p.queueURL, err)
	}

	return nil
}
