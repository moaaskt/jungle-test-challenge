package messaging

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/moaaskt/jungle-test-challenge/pkg/config"
)

// QueuesConfig armazena as URLs resolvidas das filas SQS provisionadas.
type QueuesConfig struct {
	WagerRequestsQueueURL string
	WagerEventsQueueURL   string
	DLQQueueURL           string
}

// QueueProvisioner é responsável por garantir que as filas FIFO e DLQ existam e estejam configuradas.
type QueueProvisioner interface {
	EnsureQueues(ctx context.Context) (*QueuesConfig, error)
}

type sqsQueueProvisioner struct {
	client *sqs.Client
	cfg    *config.Config
}

func NewQueueProvisioner(client *sqs.Client, cfg *config.Config) QueueProvisioner {
	return &sqsQueueProvisioner{
		client: client,
		cfg:    cfg,
	}
}

func (p *sqsQueueProvisioner) EnsureQueues(ctx context.Context) (*QueuesConfig, error) {
	// 1. Criar ou obter a DLQ FIFO
	dlqName := p.cfg.SQSDLQQueue
	dlqOut, err := p.client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(dlqName),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "false",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create/ensure DLQ %s: %w", dlqName, err)
	}
	dlqURL := *dlqOut.QueueUrl

	// Obter o ARN da DLQ para configurar o redrive
	attrOut, err := p.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(dlqURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameQueueArn,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get DLQ attributes: %w", err)
	}
	dlqArn := attrOut.Attributes[string(types.QueueAttributeNameQueueArn)]

	// 2. Criar ou obter a fila principal com RedrivePolicy para a DLQ (maxReceiveCount = 5)
	redrivePolicy := fmt.Sprintf(`{"deadLetterTargetArn":"%s","maxReceiveCount":"5"}`, dlqArn)
	reqQueueName := p.cfg.SQSWagerRequestsQueue
	reqOut, err := p.client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(reqQueueName),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "false",
			"RedrivePolicy":             redrivePolicy,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create/ensure requests queue %s: %w", reqQueueName, err)
	}
	reqURL := *reqOut.QueueUrl

	// 3. Criar ou obter a fila de eventos de saída da Outbox
	eventsQueueName := p.cfg.SQSWagerEventsQueue
	eventsOut, err := p.client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(eventsQueueName),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "false",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create/ensure events queue %s: %w", eventsQueueName, err)
	}
	eventsURL := *eventsOut.QueueUrl

	return &QueuesConfig{
		WagerRequestsQueueURL: reqURL,
		WagerEventsQueueURL:   eventsURL,
		DLQQueueURL:           dlqURL,
	}, nil
}
