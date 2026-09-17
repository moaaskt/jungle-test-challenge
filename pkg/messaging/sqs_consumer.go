package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/money"
	"github.com/moaaskt/jungle-test-challenge/pkg/service"
)

// WagerMessageEnvelope representa o envelope padrão das mensagens recebidas pelo SQS (Seção 10 da spec).
type WagerMessageEnvelope struct {
	MessageID   string           `json:"messageId"`
	Type        string           `json:"type"`
	OccurredAt  string           `json:"occurredAt"`
	Data        WagerMessageData `json:"data"`
}

// WagerMessageData contém o payload de WagerTransactionRequested.
type WagerMessageData struct {
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	IdempotencyKey                 string      `json:"idempotencyKey"`
	PlayerID                       string      `json:"playerId"`
	WalletID                       string      `json:"walletId"`
	RoundID                        string      `json:"roundId"`
	GameID                         string      `json:"gameId"`
	Kind                           string      `json:"kind"` // BET, WIN, LOSS, REFUND, ROLLBACK
	Money                          moneyPayload `json:"money"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId,omitempty"`
}

type moneyPayload struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// SQSConsumerConfig contém as configurações do consumidor SQS.
type SQSConsumerConfig struct {
	QueueURL            string
	ConsumerName        string
	MaxNumberOfMessages int32
	WaitTimeSeconds     int32
	VisibilityTimeout   int32
}

// DefaultSQSConsumerConfig retorna as configurações padrão para o consumidor de apostas.
func DefaultSQSConsumerConfig(queueURL string) SQSConsumerConfig {
	return SQSConsumerConfig{
		QueueURL:            queueURL,
		ConsumerName:        "wager-transactions-consumer",
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     5,
		VisibilityTimeout:   30,
	}
}

type inFlightItem struct {
	msg      *types.Message
	released bool
}

// SQSConsumer consome mensagens da fila SQS FIFO com proteção de Inbox e transacionalidade atômica.
type SQSConsumer struct {
	client       *sqs.Client
	wagerService service.WagerService
	cfg          SQSConsumerConfig
	logger       *slog.Logger

	inFlightMu   sync.Mutex
	inFlightMsgs map[string]*inFlightItem // chave: ReceiptHandle

	ctx     context.Context
	cancel  context.CancelFunc
	stopCh  chan struct{}
	doneCh  chan struct{}
	wg      sync.WaitGroup
	started atomic.Bool
	stopped atomic.Bool
}

// NewSQSConsumer instancia um novo SQSConsumer.
func NewSQSConsumer(
	client *sqs.Client,
	wagerService service.WagerService,
	cfg SQSConsumerConfig,
	logger *slog.Logger,
) *SQSConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	return &SQSConsumer{
		client:       client,
		wagerService: wagerService,
		cfg:          cfg,
		logger:       logger,
		inFlightMsgs: make(map[string]*inFlightItem),
		ctx:          consumerCtx,
		cancel:       consumerCancel,
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
}

// Start inicia o loop de consumo assíncrono em background.
func (c *SQSConsumer) Start() {
	if !c.started.CompareAndSwap(false, true) {
		return
	}
	c.logger.Info("Starting SQS consumer", "queue", c.cfg.QueueURL, "consumer", c.cfg.ConsumerName)
	go c.pollLoop()
}

// Stop realiza o graceful shutdown do consumidor, aguardando mensagens em voo ou liberando visibilidade imediata (0s).
func (c *SQSConsumer) Stop(ctx context.Context) error {
	if !c.started.Load() || !c.stopped.CompareAndSwap(false, true) {
		return nil
	}
	c.logger.Info("Stopping SQS consumer...", "consumer", c.cfg.ConsumerName)
	c.cancel()
	close(c.stopCh)

	// 1. Aguardar o término do pollLoop para garantir que nenhuma mensagem nova será recebida nem wg.Add será chamado
	select {
	case <-c.doneCh:
	case <-ctx.Done():
		c.releaseInFlightToZero()
		return ctx.Err()
	}

	// 2. Aguardar as mensagens em voo terminarem ou o timeout expirar
	waitDone := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		c.logger.Info("SQS consumer stopped cleanly, all in-flight messages completed")
		return nil
	case <-ctx.Done():
		c.releaseInFlightToZero()
		return ctx.Err()
	}
}

func (c *SQSConsumer) releaseInFlightToZero() {
	c.logger.Warn("Shutdown timeout reached while stopping SQS consumer, releasing visibility of in-flight messages to 0s")
	c.inFlightMu.Lock()
	defer c.inFlightMu.Unlock()

	for handle, item := range c.inFlightMsgs {
		item.released = true
		msgID := ""
		if item.msg.MessageId != nil {
			msgID = *item.msg.MessageId
		}
		_, err := c.client.ChangeMessageVisibility(context.Background(), &sqs.ChangeMessageVisibilityInput{
			QueueUrl:          &c.cfg.QueueURL,
			ReceiptHandle:     &handle,
			VisibilityTimeout: 0,
		})
		if err != nil {
			c.logger.Error("Failed to release visibility timeout for message", "messageId", msgID, "error", err)
		} else {
			c.logger.Info("Released visibility timeout for message to 0s", "messageId", msgID)
		}
	}
}

func (c *SQSConsumer) pollLoop() {
	defer close(c.doneCh)

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		pollCtx, cancel := context.WithTimeout(c.ctx, time.Duration(c.cfg.WaitTimeSeconds+5)*time.Second)
		output, err := c.client.ReceiveMessage(pollCtx, &sqs.ReceiveMessageInput{
			QueueUrl:              &c.cfg.QueueURL,
			MaxNumberOfMessages:   c.cfg.MaxNumberOfMessages,
			WaitTimeSeconds:       c.cfg.WaitTimeSeconds,
			VisibilityTimeout:     c.cfg.VisibilityTimeout,
			MessageAttributeNames: []string{"All"},
			AttributeNames:        []types.QueueAttributeName{types.QueueAttributeNameAll},
		})
		cancel()

		if err != nil {
			select {
			case <-c.ctx.Done():
				return
			case <-c.stopCh:
				return
			default:
				c.logger.Error("Error receiving messages from SQS", "error", err)
				time.Sleep(1 * time.Second)
				continue
			}
		}

		if output == nil || len(output.Messages) == 0 {
			continue
		}

		for _, msg := range output.Messages {
			if msg.ReceiptHandle == nil {
				continue
			}

			// Rastrear mensagem em voo
			handle := *msg.ReceiptHandle
			c.inFlightMu.Lock()
			c.inFlightMsgs[handle] = &inFlightItem{msg: &msg, released: false}
			c.inFlightMu.Unlock()

			c.wg.Add(1)
			go func(m types.Message) {
				defer c.wg.Done()
				defer func() {
					c.inFlightMu.Lock()
					delete(c.inFlightMsgs, *m.ReceiptHandle)
					c.inFlightMu.Unlock()
				}()

				c.processMessage(m)
			}(msg)
		}
	}
}

func (c *SQSConsumer) processMessage(msg types.Message) {
	receiptHandle := *msg.ReceiptHandle
	body := ""
	if msg.Body != nil {
		body = *msg.Body
	}

	h := sha256.Sum256([]byte(body))
	payloadHash := hex.EncodeToString(h[:])

	var env WagerMessageEnvelope
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		c.logger.Error("Invalid message envelope JSON, permanent error, moving to DLQ or discarding", "error", err, "body", body)
		// Erro permanente na desserialização da mensagem: não chama delete se quiser DLQ após maxReceiveCount
		return
	}

	msgID := env.MessageID
	if msgID == "" && msg.MessageId != nil {
		msgID = *msg.MessageId
	}
	if msgID == "" {
		msgID = payloadHash
	}

	// Parsing do montante financeiro
	var amountCentavos int64 = 0
	currency := env.Data.Money.Currency
	if currency == "" {
		currency = "BRL"
	}
	if env.Data.Money.Amount != "" {
		m, err := money.Parse(env.Data.Money.Amount, currency)
		if err != nil {
			c.logger.Error("Invalid monetary amount in message", "amount", env.Data.Money.Amount, "error", err)
			return
		}
		amountCentavos = m.Amount()
	}

	var refExtID *string
	if env.Data.ReferenceExternalTransactionID != "" {
		refExtID = &env.Data.ReferenceExternalTransactionID
	}

	req := service.ProcessWagerRequest{
		Origin:                         domain.OriginExternal,
		PlayerID:                       env.Data.PlayerID,
		ProviderID:                     &env.Data.ProviderID,
		ExternalID:                     &env.Data.ExternalTransactionID,
		IdempotencyKey:                 &env.Data.IdempotencyKey,
		PayloadHash:                    &payloadHash,
		RoundID:                        &env.Data.RoundID,
		GameID:                         &env.Data.GameID,
		Type:                           domain.TransactionType(env.Data.Kind),
		Amount:                         amountCentavos,
		Currency:                       currency,
		ReferenceExternalTransactionID: refExtID,
	}

	inboxRecord := domain.NewInboxRecord(msgID, c.cfg.ConsumerName, "SQS", payloadHash)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	res, err := c.wagerService.ProcessWagerWithInbox(ctx, req, inboxRecord)
	if err != nil {
		c.logger.Error("Transient error processing wager transaction with inbox, leaving in queue for retry", "messageId", msgID, "error", err)
		return
	}

	// Ponto 1: Reentrega pós-commit (IdempotentReplay == true)
	if res.IdempotentReplay {
		c.logger.Info("Idempotent replay detected by inbox, immediately expunging duplicate message from SQS", "messageId", msgID)
		c.deleteFromQueue(receiptHandle)
		return
	}

	// Ponto 2: Mensagem fora de ordem (Status == PENDING_REFERENCE)
	if res.Status == domain.StatusPendingReference {
		c.logger.Info("Message processed as PENDING_REFERENCE, removing from SQS for resolution worker", "messageId", msgID, "txId", res.TransactionID)
		c.deleteFromQueue(receiptHandle)
		return
	}

	// Rejeição de negócio (Status == REJECTED)
	if res.Status == domain.StatusRejected {
		c.logger.Info("Message terminal business rejection, removing from SQS", "messageId", msgID, "txId", res.TransactionID)
		c.deleteFromQueue(receiptHandle)
		return
	}

	// Processamento com sucesso normal
	c.logger.Info("Message processed successfully, removing from SQS", "messageId", msgID, "txId", res.TransactionID, "balance", res.Balance.String())
	c.deleteFromQueue(receiptHandle)
}

func (c *SQSConsumer) deleteFromQueue(receiptHandle string) {
	c.inFlightMu.Lock()
	item, exists := c.inFlightMsgs[receiptHandle]
	if exists && item.released {
		c.inFlightMu.Unlock()
		c.logger.Info("Skipping delete for message whose visibility was released to 0s during shutdown", "receiptHandle", receiptHandle)
		return
	}
	c.inFlightMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &c.cfg.QueueURL,
		ReceiptHandle: &receiptHandle,
	})
	if err != nil {
		c.logger.Error("Failed to delete message from SQS queue", "receiptHandle", receiptHandle, "error", err)
	}
}

// InFlightCount retorna a quantidade de mensagens sendo processadas no momento.
func (c *SQSConsumer) InFlightCount() int {
	c.inFlightMu.Lock()
	defer c.inFlightMu.Unlock()
	return len(c.inFlightMsgs)
}
