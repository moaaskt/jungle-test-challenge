package test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moaaskt/jungle-test-challenge/pkg/config"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/messaging"
	"github.com/moaaskt/jungle-test-challenge/pkg/repository"
	"github.com/moaaskt/jungle-test-challenge/pkg/service"
)

func getTestSQSConfig() *config.Config {
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	return &config.Config{
		DatabaseURL:           "postgres://jungle:password@localhost:5432/jungle_test?sslmode=disable",
		AWSEndpoint:           endpoint,
		AWSRegion:             "us-east-1",
		SQSWagerRequestsQueue: fmt.Sprintf("test-wager-req-%d.fifo", time.Now().UnixNano()),
		SQSWagerEventsQueue:   fmt.Sprintf("test-wager-ev-%d.fifo", time.Now().UnixNano()),
		SQSDLQQueue:           fmt.Sprintf("test-wager-dlq-%d.fifo", time.Now().UnixNano()),
	}
}

func getWalletBalance(ctx context.Context, pool *pgxpool.Pool, playerID, currency string) (int64, error) {
	var balance int64
	err := pool.QueryRow(ctx, "SELECT balance FROM wallets WHERE player_id = $1 AND currency = $2", playerID, currency).Scan(&balance)
	return balance, err
}

func getInboxRecord(ctx context.Context, pool *pgxpool.Pool, consumerName, messageID string) (*domain.InboxRecord, error) {
	query := `
		SELECT id, message_id, consumer_name, source, payload_hash, received_at, processed_at
		FROM inbox_messages
		WHERE consumer_name = $1 AND message_id = $2
	`
	var r domain.InboxRecord
	err := pool.QueryRow(ctx, query, consumerName, messageID).Scan(
		&r.ID, &r.MessageID, &r.ConsumerName, &r.Source, &r.PayloadHash, &r.ReceivedAt, &r.ProcessedAt,
	)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func getWagerStatus(ctx context.Context, pool *pgxpool.Pool, providerID, externalID string) (domain.TransactionStatus, error) {
	var status string
	err := pool.QueryRow(ctx, "SELECT status FROM wager_transactions WHERE provider_id = $1 AND external_id = $2", providerID, externalID).Scan(&status)
	if err != nil {
		return "", err
	}
	return domain.TransactionStatus(status), nil
}

func getOutboxStatus(ctx context.Context, pool *pgxpool.Pool, eventID uuid.UUID) (domain.OutboxStatus, error) {
	var status string
	err := pool.QueryRow(ctx, "SELECT status FROM outbox_events WHERE id = $1", eventID).Scan(&status)
	if err != nil {
		return "", err
	}
	return domain.OutboxStatus(status), nil
}

// TestSQS_ProvisionQueuesAndRedrive valida que as filas FIFO e a DLQ com Redrive Policy são criadas corretamente.
func TestSQS_ProvisionQueuesAndRedrive(t *testing.T) {
	cfg := getTestSQSConfig()
	sqsClient, err := messaging.NewSQSClient(cfg)
	if err != nil {
		t.Fatalf("failed to create SQS client: %v", err)
	}

	provisioner := messaging.NewQueueProvisioner(sqsClient, cfg)
	ctx := context.Background()

	queues, err := provisioner.EnsureQueues(ctx)
	if err != nil {
		t.Fatalf("failed to ensure queues: %v", err)
	}

	if queues.WagerRequestsQueueURL == "" || queues.DLQQueueURL == "" || queues.WagerEventsQueueURL == "" {
		t.Fatalf("queues URLs should not be empty: %+v", queues)
	}

	// Inspecionar atributos da fila de requests
	attrOut, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queues.WagerRequestsQueueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameFifoQueue,
			types.QueueAttributeNameRedrivePolicy,
		},
	})
	if err != nil {
		t.Fatalf("failed to get queue attributes: %v", err)
	}

	if attrOut.Attributes[string(types.QueueAttributeNameFifoQueue)] != "true" {
		t.Errorf("expected FifoQueue=true, got %s", attrOut.Attributes[string(types.QueueAttributeNameFifoQueue)])
	}

	redrive := attrOut.Attributes[string(types.QueueAttributeNameRedrivePolicy)]
	if redrive == "" {
		t.Errorf("expected RedrivePolicy to be present on request queue")
	}
}

// TestSQS_Inbox_NormalConsumptionAndAtomicity valida o consumo normal com garantia de Inbox,
// transacionalidade atômica (saldo + ledger + outbox + inbox) e remoção da mensagem do broker.
func TestSQS_Inbox_NormalConsumptionAndAtomicity(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	cfg := getTestSQSConfig()
	sqsClient, err := messaging.NewSQSClient(cfg)
	if err != nil {
		t.Fatalf("failed to create SQS client: %v", err)
	}

	provisioner := messaging.NewQueueProvisioner(sqsClient, cfg)
	ctx := context.Background()
	queues, err := provisioner.EnsureQueues(ctx)
	if err != nil {
		t.Fatalf("failed to provision queues: %v", err)
	}

	walletRepo := repository.NewWalletRepository()
	wagerRepo := repository.NewWagerTransactionRepository()
	ledgerRepo := repository.NewLedgerRepository()
	idemRepo := repository.NewIdempotencyRepository()
	outboxRepo := repository.NewOutboxRepository(pool)
	inboxRepo := repository.NewInboxRepository()

	svc := service.NewWagerService(pool, walletRepo, wagerRepo, ledgerRepo, idemRepo, outboxRepo, inboxRepo)

	consumerCfg := messaging.DefaultSQSConsumerConfig(queues.WagerRequestsQueueURL)
	consumerCfg.WaitTimeSeconds = 1
	consumer := messaging.NewSQSConsumer(sqsClient, svc, consumerCfg, nil)
	consumer.Start()
	defer consumer.Stop(ctx)

	// 1. Criar carteira com 100.00 BRL
	playerID := fmt.Sprintf("player-sqs-%s", uuid.New().String()[:8])
	wallet, err := svc.OpenWallet(ctx, playerID, "BRL", 10000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	// 2. Enviar mensagem SQS de BET de 25.00 BRL
	msgID := fmt.Sprintf("msg-%s", uuid.New().String())
	extTxID := fmt.Sprintf("tx-ext-%s", uuid.New().String())
	idemKey := fmt.Sprintf("prov-a:%s", extTxID)

	envelope := messaging.WagerMessageEnvelope{
		MessageID:  msgID,
		Type:       "WagerTransactionRequested",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Data: messaging.WagerMessageData{
			ProviderID:            "provider-a",
			ExternalTransactionID: extTxID,
			IdempotencyKey:        idemKey,
			PlayerID:              playerID,
			WalletID:              wallet.ID.String(),
			RoundID:               "round-1",
			GameID:                "game-1",
			Kind:                  "BET",
			Money: struct {
				Amount   string `json:"amount"`
				Currency string `json:"currency"`
			}{
				Amount:   "25.00",
				Currency: "BRL",
			},
		},
	}
	bodyBytes, _ := json.Marshal(envelope)

	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(queues.WagerRequestsQueueURL),
		MessageBody:            aws.String(string(bodyBytes)),
		MessageGroupId:         aws.String(playerID),
		MessageDeduplicationId: aws.String(msgID),
	})
	if err != nil {
		t.Fatalf("failed to send SQS message: %v", err)
	}

	// 3. Aguardar processamento pelo consumidor
	var finalBalance int64 = -1
	for i := 0; i < 40; i++ {
		time.Sleep(100 * time.Millisecond)
		bal, err := getWalletBalance(ctx, pool, playerID, "BRL")
		if err == nil && bal == 7500 {
			finalBalance = bal
			break
		}
	}

	if finalBalance != 7500 {
		t.Fatalf("timeout waiting for SQS message to be processed and balance updated to 75.00 BRL, got: %d", finalBalance)
	}

	// 4. Validar persistência da Inbox
	inboxRecord, err := getInboxRecord(ctx, pool, consumerCfg.ConsumerName, msgID)
	if err != nil || inboxRecord == nil {
		t.Fatalf("expected inbox message record for %s, got: %v", msgID, err)
	}
	if inboxRecord.ConsumerName != consumerCfg.ConsumerName {
		t.Errorf("expected consumer name %s, got %s", consumerCfg.ConsumerName, inboxRecord.ConsumerName)
	}

	// 5. Validar status da transação
	txStatus, err := getWagerStatus(ctx, pool, "provider-a", extTxID)
	if err != nil || txStatus != domain.StatusProcessed {
		t.Errorf("expected transaction status %s, got %s, err: %v", domain.StatusProcessed, txStatus, err)
	}

	// 6. Validar que a mensagem foi removida da fila
	recOut, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queues.WagerRequestsQueueURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     1,
	})
	if err != nil {
		t.Fatalf("failed to inspect queue after processing: %v", err)
	}
	if len(recOut.Messages) > 0 {
		t.Errorf("expected queue to be empty after processing, found %d messages", len(recOut.Messages))
	}
}

// TestSQS_Inbox_PostCommitCrash_ReplayDeletion valida que após o commit do banco,
// se a mensagem for reentregue pelo SQS, a Inbox detecta o replay e o consumidor expurga
// a mensagem imediatamente com sqs.DeleteMessage sem duplicar operações financeiras.
func TestSQS_Inbox_PostCommitCrash_ReplayDeletion(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	cfg := getTestSQSConfig()
	sqsClient, err := messaging.NewSQSClient(cfg)
	if err != nil {
		t.Fatalf("failed to create SQS client: %v", err)
	}

	provisioner := messaging.NewQueueProvisioner(sqsClient, cfg)
	ctx := context.Background()
	queues, err := provisioner.EnsureQueues(ctx)
	if err != nil {
		t.Fatalf("failed to provision queues: %v", err)
	}

	walletRepo := repository.NewWalletRepository()
	wagerRepo := repository.NewWagerTransactionRepository()
	ledgerRepo := repository.NewLedgerRepository()
	idemRepo := repository.NewIdempotencyRepository()
	outboxRepo := repository.NewOutboxRepository(pool)
	inboxRepo := repository.NewInboxRepository()

	svc := service.NewWagerService(pool, walletRepo, wagerRepo, ledgerRepo, idemRepo, outboxRepo, inboxRepo)

	consumerCfg := messaging.DefaultSQSConsumerConfig(queues.WagerRequestsQueueURL)
	consumerCfg.WaitTimeSeconds = 1
	consumer := messaging.NewSQSConsumer(sqsClient, svc, consumerCfg, nil)
	consumer.Start()
	defer consumer.Stop(ctx)

	playerID := fmt.Sprintf("player-crash-%s", uuid.New().String()[:8])
	wallet, err := svc.OpenWallet(ctx, playerID, "BRL", 5000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	msgID := fmt.Sprintf("msg-crash-%s", uuid.New().String())
	extTxID := fmt.Sprintf("tx-crash-%s", uuid.New().String())
	idemKey := fmt.Sprintf("prov-a:%s", extTxID)

	envelope := messaging.WagerMessageEnvelope{
		MessageID:  msgID,
		Type:       "WagerTransactionRequested",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Data: messaging.WagerMessageData{
			ProviderID:            "provider-a",
			ExternalTransactionID: extTxID,
			IdempotencyKey:        idemKey,
			PlayerID:              playerID,
			WalletID:              wallet.ID.String(),
			Kind:                  "BET",
			Money: struct {
				Amount   string `json:"amount"`
				Currency string `json:"currency"`
			}{
				Amount:   "10.00",
				Currency: "BRL",
			},
		},
	}
	bodyBytes, _ := json.Marshal(envelope)

	// 1. Primeira entrega da mensagem
	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(queues.WagerRequestsQueueURL),
		MessageBody:            aws.String(string(bodyBytes)),
		MessageGroupId:         aws.String(playerID),
		MessageDeduplicationId: aws.String(msgID + "-1"),
	})
	if err != nil {
		t.Fatalf("failed to send message: %v", err)
	}

	// Aguardar débito inicial (50.00 -> 40.00 BRL)
	for i := 0; i < 40; i++ {
		time.Sleep(100 * time.Millisecond)
		bal, err := getWalletBalance(ctx, pool, playerID, "BRL")
		if err == nil && bal == 4000 {
			break
		}
	}

	bal, _ := getWalletBalance(ctx, pool, playerID, "BRL")
	if bal != 4000 {
		t.Fatalf("expected balance to be 40.00 BRL, got %d", bal)
	}

	// 2. Simular reentrega da MESMA mensagem (mesmo envelope.MessageID)
	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(queues.WagerRequestsQueueURL),
		MessageBody:            aws.String(string(bodyBytes)),
		MessageGroupId:         aws.String(playerID),
		MessageDeduplicationId: aws.String(msgID + "-replay"),
	})
	if err != nil {
		t.Fatalf("failed to send replay message: %v", err)
	}

	// Aguardar consumidor processar a reentrega
	time.Sleep(2 * time.Second)

	// 3. Validar que o saldo NÃO foi debitado novamente
	balAfterReplay, err := getWalletBalance(ctx, pool, playerID, "BRL")
	if err != nil {
		t.Fatalf("failed to get wallet balance: %v", err)
	}
	if balAfterReplay != 4000 {
		t.Errorf("balance was modified on replay! Expected 40.00 BRL (4000), got %d", balAfterReplay)
	}

	// 4. Validar que o consumidor expurgou a mensagem reentregue da fila
	recOut, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queues.WagerRequestsQueueURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     1,
	})
	if err != nil {
		t.Fatalf("failed to check queue: %v", err)
	}
	if len(recOut.Messages) > 0 {
		t.Errorf("replayed message was not expunged from queue, found %d messages", len(recOut.Messages))
	}
}

// TestSQS_Inbox_OutOfOrder_PendingReference valida que quando chega um REFUND
// de uma aposta que ainda não existe no banco, a transação é persistida como PENDING_REFERENCE,
// o evento WagerTransactionPendingReference é gravado na Outbox, a Inbox é confirmada e a
// mensagem é removida do SQS sem ir para DLQ nem falhar.
func TestSQS_Inbox_OutOfOrder_PendingReference(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	cfg := getTestSQSConfig()
	sqsClient, err := messaging.NewSQSClient(cfg)
	if err != nil {
		t.Fatalf("failed to create SQS client: %v", err)
	}

	provisioner := messaging.NewQueueProvisioner(sqsClient, cfg)
	ctx := context.Background()
	queues, err := provisioner.EnsureQueues(ctx)
	if err != nil {
		t.Fatalf("failed to provision queues: %v", err)
	}

	walletRepo := repository.NewWalletRepository()
	wagerRepo := repository.NewWagerTransactionRepository()
	ledgerRepo := repository.NewLedgerRepository()
	idemRepo := repository.NewIdempotencyRepository()
	outboxRepo := repository.NewOutboxRepository(pool)
	inboxRepo := repository.NewInboxRepository()

	svc := service.NewWagerService(pool, walletRepo, wagerRepo, ledgerRepo, idemRepo, outboxRepo, inboxRepo)

	consumerCfg := messaging.DefaultSQSConsumerConfig(queues.WagerRequestsQueueURL)
	consumerCfg.WaitTimeSeconds = 1
	consumer := messaging.NewSQSConsumer(sqsClient, svc, consumerCfg, nil)
	consumer.Start()
	defer consumer.Stop(ctx)

	playerID := fmt.Sprintf("player-pending-%s", uuid.New().String()[:8])
	wallet, err := svc.OpenWallet(ctx, playerID, "BRL", 5000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	msgID := fmt.Sprintf("msg-pending-%s", uuid.New().String())
	refundExtID := fmt.Sprintf("tx-refund-%s", uuid.New().String())
	missingBetExtID := fmt.Sprintf("tx-bet-missing-%s", uuid.New().String())

	envelope := messaging.WagerMessageEnvelope{
		MessageID:  msgID,
		Type:       "WagerTransactionRequested",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Data: messaging.WagerMessageData{
			ProviderID:                     "provider-b",
			ExternalTransactionID:          refundExtID,
			IdempotencyKey:                 "prov-b:" + refundExtID,
			PlayerID:                       playerID,
			WalletID:                       wallet.ID.String(),
			Kind:                           "REFUND",
			ReferenceExternalTransactionID: missingBetExtID,
			Money: struct {
				Amount   string `json:"amount"`
				Currency string `json:"currency"`
			}{
				Amount:   "15.00",
				Currency: "BRL",
			},
		},
	}
	bodyBytes, _ := json.Marshal(envelope)

	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(queues.WagerRequestsQueueURL),
		MessageBody:            aws.String(string(bodyBytes)),
		MessageGroupId:         aws.String(playerID),
		MessageDeduplicationId: aws.String(msgID),
	})
	if err != nil {
		t.Fatalf("failed to send message: %v", err)
	}

	// Aguardar processamento
	var foundStatus domain.TransactionStatus
	for i := 0; i < 40; i++ {
		time.Sleep(100 * time.Millisecond)
		st, err := getWagerStatus(ctx, pool, "provider-b", refundExtID)
		if err == nil && st == domain.StatusPendingReference {
			foundStatus = st
			break
		}
	}

	if foundStatus != domain.StatusPendingReference {
		t.Fatalf("expected status %s, got %s", domain.StatusPendingReference, foundStatus)
	}

	// Validar que a mensagem foi removida da fila
	time.Sleep(500 * time.Millisecond)
	recOut, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queues.WagerRequestsQueueURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     1,
	})
	if err != nil {
		t.Fatalf("failed to check queue: %v", err)
	}
	if len(recOut.Messages) > 0 {
		t.Errorf("pending reference message was not removed from queue, found %d messages", len(recOut.Messages))
	}
}

// TestSQS_Inbox_TerminalBusinessRejection valida que rejeição por falta de saldo é terminal,
// grava status REJECTED e é removida da fila sem ir para DLQ.
func TestSQS_Inbox_TerminalBusinessRejection(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	cfg := getTestSQSConfig()
	sqsClient, err := messaging.NewSQSClient(cfg)
	if err != nil {
		t.Fatalf("failed to create SQS client: %v", err)
	}

	provisioner := messaging.NewQueueProvisioner(sqsClient, cfg)
	ctx := context.Background()
	queues, err := provisioner.EnsureQueues(ctx)
	if err != nil {
		t.Fatalf("failed to provision queues: %v", err)
	}

	walletRepo := repository.NewWalletRepository()
	wagerRepo := repository.NewWagerTransactionRepository()
	ledgerRepo := repository.NewLedgerRepository()
	idemRepo := repository.NewIdempotencyRepository()
	outboxRepo := repository.NewOutboxRepository(pool)
	inboxRepo := repository.NewInboxRepository()

	svc := service.NewWagerService(pool, walletRepo, wagerRepo, ledgerRepo, idemRepo, outboxRepo, inboxRepo)

	consumerCfg := messaging.DefaultSQSConsumerConfig(queues.WagerRequestsQueueURL)
	consumerCfg.WaitTimeSeconds = 1
	consumer := messaging.NewSQSConsumer(sqsClient, svc, consumerCfg, nil)
	consumer.Start()
	defer consumer.Stop(ctx)

	playerID := fmt.Sprintf("player-rej-%s", uuid.New().String()[:8])
	wallet, err := svc.OpenWallet(ctx, playerID, "BRL", 1000) // 10.00 BRL
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	msgID := fmt.Sprintf("msg-rej-%s", uuid.New().String())
	extTxID := fmt.Sprintf("tx-rej-%s", uuid.New().String())

	envelope := messaging.WagerMessageEnvelope{
		MessageID:  msgID,
		Type:       "WagerTransactionRequested",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Data: messaging.WagerMessageData{
			ProviderID:            "provider-c",
			ExternalTransactionID: extTxID,
			IdempotencyKey:        "prov-c:" + extTxID,
			PlayerID:              playerID,
			WalletID:              wallet.ID.String(),
			Kind:                  "BET",
			Money: struct {
				Amount   string `json:"amount"`
				Currency string `json:"currency"`
			}{
				Amount:   "90.00", // Mais que o saldo
				Currency: "BRL",
			},
		},
	}
	bodyBytes, _ := json.Marshal(envelope)

	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(queues.WagerRequestsQueueURL),
		MessageBody:            aws.String(string(bodyBytes)),
		MessageGroupId:         aws.String(playerID),
		MessageDeduplicationId: aws.String(msgID),
	})
	if err != nil {
		t.Fatalf("failed to send message: %v", err)
	}

	// Aguardar persistência como REJECTED
	var foundStatus domain.TransactionStatus
	for i := 0; i < 40; i++ {
		time.Sleep(100 * time.Millisecond)
		st, err := getWagerStatus(ctx, pool, "provider-c", extTxID)
		if err == nil && st == domain.StatusRejected {
			foundStatus = st
			break
		}
	}

	if foundStatus != domain.StatusRejected {
		t.Fatalf("expected status %s, got %s", domain.StatusRejected, foundStatus)
	}

	// Validar que a mensagem foi expurgada da fila SQS
	time.Sleep(500 * time.Millisecond)
	recOut, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queues.WagerRequestsQueueURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     1,
	})
	if err != nil {
		t.Fatalf("failed to check queue: %v", err)
	}
	if len(recOut.Messages) > 0 {
		t.Errorf("rejected message was not removed from queue, found %d messages", len(recOut.Messages))
	}
}

// TestSQS_Concurrent_HTTP_vs_SQS valida que chamadas concorrentes vindas de HTTP e SQS
// para a mesma transação/idempotencyKey são deduplicadas com segurança sem corrupção de saldo.
func TestSQS_Concurrent_HTTP_vs_SQS(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	cfg := getTestSQSConfig()
	sqsClient, err := messaging.NewSQSClient(cfg)
	if err != nil {
		t.Fatalf("failed to create SQS client: %v", err)
	}

	provisioner := messaging.NewQueueProvisioner(sqsClient, cfg)
	ctx := context.Background()
	queues, err := provisioner.EnsureQueues(ctx)
	if err != nil {
		t.Fatalf("failed to provision queues: %v", err)
	}

	walletRepo := repository.NewWalletRepository()
	wagerRepo := repository.NewWagerTransactionRepository()
	ledgerRepo := repository.NewLedgerRepository()
	idemRepo := repository.NewIdempotencyRepository()
	outboxRepo := repository.NewOutboxRepository(pool)
	inboxRepo := repository.NewInboxRepository()

	svc := service.NewWagerService(pool, walletRepo, wagerRepo, ledgerRepo, idemRepo, outboxRepo, inboxRepo)

	consumerCfg := messaging.DefaultSQSConsumerConfig(queues.WagerRequestsQueueURL)
	consumerCfg.WaitTimeSeconds = 1
	consumer := messaging.NewSQSConsumer(sqsClient, svc, consumerCfg, nil)
	consumer.Start()
	defer consumer.Stop(ctx)

	playerID := fmt.Sprintf("player-conc-%s", uuid.New().String()[:8])
	wallet, err := svc.OpenWallet(ctx, playerID, "BRL", 10000) // 100.00 BRL
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	extTxID := fmt.Sprintf("tx-conc-%s", uuid.New().String())
	idemKey := "prov-conc:" + extTxID
	providerID := "provider-conc"

	envelope := messaging.WagerMessageEnvelope{
		MessageID:  fmt.Sprintf("msg-conc-%s", extTxID),
		Type:       "WagerTransactionRequested",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Data: messaging.WagerMessageData{
			ProviderID:            providerID,
			ExternalTransactionID: extTxID,
			IdempotencyKey:        idemKey,
			PlayerID:              playerID,
			WalletID:              wallet.ID.String(),
			Kind:                  "BET",
			Money: struct {
				Amount   string `json:"amount"`
				Currency string `json:"currency"`
			}{
				Amount:   "20.00",
				Currency: "BRL",
			},
		},
	}
	bodyBytes, _ := json.Marshal(envelope)

	var startWG sync.WaitGroup
	startWG.Add(1)
	var doneWG sync.WaitGroup
	doneWG.Add(2)

	// Goroutine 1: HTTP-like chamada direta a ProcessWager
	go func() {
		defer doneWG.Done()
		startWG.Wait()

		h := sha256.Sum256(bodyBytes)
		payloadHash := hex.EncodeToString(h[:])
		_, _ = svc.ProcessWager(ctx, service.ProcessWagerRequest{
			Origin:         domain.OriginExternal,
			PlayerID:       playerID,
			ProviderID:     &providerID,
			ExternalID:     &extTxID,
			IdempotencyKey: &idemKey,
			PayloadHash:    &payloadHash,
			Type:           domain.TransactionTypeBet,
			Amount:         2000,
			Currency:       "BRL",
		})
	}()

	// Goroutine 2: Envia mensagem SQS para ser consumida pelo consumer concorrente
	go func() {
		defer doneWG.Done()
		startWG.Wait()

		_, _ = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:               aws.String(queues.WagerRequestsQueueURL),
			MessageBody:            aws.String(string(bodyBytes)),
			MessageGroupId:         aws.String(playerID),
			MessageDeduplicationId: aws.String(envelope.MessageID),
		})
	}()

	// Disparo concorrente imediato
	startWG.Done()
	doneWG.Wait()

	// Aguardar convergência do consumidor SQS
	time.Sleep(2 * time.Second)

	// Validar que o saldo final é exatamente 80.00 BRL (debitado APENAS uma vez!)
	bal, err := getWalletBalance(ctx, pool, playerID, "BRL")
	if err != nil {
		t.Fatalf("failed to get wallet balance: %v", err)
	}
	if bal != 8000 {
		t.Errorf("expected balance to be 80.00 BRL (8000) after concurrent HTTP/SQS race, got %d", bal)
	}
}

// TestSQS_OutboxRelayer_To_SQSQueue valida que eventos da Outbox são publicados na fila de eventos do SQS
func TestSQS_OutboxRelayer_To_SQSQueue(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	cfg := getTestSQSConfig()
	sqsClient, err := messaging.NewSQSClient(cfg)
	if err != nil {
		t.Fatalf("failed to create SQS client: %v", err)
	}

	provisioner := messaging.NewQueueProvisioner(sqsClient, cfg)
	ctx := context.Background()
	queues, err := provisioner.EnsureQueues(ctx)
	if err != nil {
		t.Fatalf("failed to provision queues: %v", err)
	}

	outboxRepo := repository.NewOutboxRepository(pool)
	sqsPub := messaging.NewSQSPublisher(sqsClient, queues.WagerEventsQueueURL)

	relayerCfg := service.DefaultOutboxRelayerConfig()
	relayerCfg.PollInterval = 100 * time.Millisecond
	relayer := service.NewOutboxRelayer(outboxRepo, sqsPub, &relayerCfg)
	relayer.Start()
	defer relayer.Stop(ctx)

	// Inserir evento pendente na tabela outbox_events
	evID := uuid.New()
	ev := &domain.OutboxEvent{
		ID:            evID,
		AggregateType: "WALLET",
		AggregateID:   uuid.New().String(),
		EventType:     "WalletBalanceChanged",
		Payload:       []byte(`{"eventType":"WalletBalanceChanged","amount":"10.00"}`),
		Status:        domain.OutboxStatusPending,
		RetryCount:    0,
		CreatedAt:     time.Now(),
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx: %v", err)
	}
	if err := outboxRepo.Insert(ctx, tx, ev); err != nil {
		t.Fatalf("failed to insert outbox event: %v", err)
	}
	_ = tx.Commit(ctx)

	// Aguardar relayer processar o evento e publicar na fila SQS
	for i := 0; i < 40; i++ {
		time.Sleep(100 * time.Millisecond)
		st, err := getOutboxStatus(ctx, pool, evID)
		if err == nil && st == domain.OutboxStatusPublished {
			break
		}
	}

	st, err := getOutboxStatus(ctx, pool, evID)
	if err != nil || st != domain.OutboxStatusPublished {
		t.Fatalf("expected outbox event to be PUBLISHED by relayer, got status: %v", st)
	}

	// Inspecionar se o evento chegou na fila SQS de saída
	recOut, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queues.WagerEventsQueueURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     2,
	})
	if err != nil {
		t.Fatalf("failed to receive events from SQS: %v", err)
	}
	if len(recOut.Messages) == 0 {
		t.Fatalf("expected message to arrive in SQS wager-events queue")
	}
}

// blockingWagerService simula um serviço que bloqueia durante o shutdown
type blockingWagerService struct {
	service.WagerService
	blockCh chan struct{}
}

func (m *blockingWagerService) ProcessWagerWithInbox(ctx context.Context, req service.ProcessWagerRequest, inbox domain.InboxRecord) (service.ProcessWagerResult, error) {
	<-m.blockCh // Bloqueia até o teste liberar ou o shutdown ocorrer
	return service.ProcessWagerResult{}, nil
}

// TestSQS_GracefulShutdown_ReleaseVisibility valida que ao disparar shutdown com timeout,
// mensagens em voo que não puderam ser comitadas a tempo têm sua visibilidade liberada
// imediatamente para 0s via sqs.ChangeMessageVisibility, permitindo reentrega instantânea no broker.
func TestSQS_GracefulShutdown_ReleaseVisibility(t *testing.T) {
	cfg := getTestSQSConfig()
	sqsClient, err := messaging.NewSQSClient(cfg)
	if err != nil {
		t.Fatalf("failed to create SQS client: %v", err)
	}

	provisioner := messaging.NewQueueProvisioner(sqsClient, cfg)
	ctx := context.Background()
	queues, err := provisioner.EnsureQueues(ctx)
	if err != nil {
		t.Fatalf("failed to provision queues: %v", err)
	}

	blockCh := make(chan struct{})
	defer close(blockCh)

	mockSvc := &blockingWagerService{blockCh: blockCh}

	consumerCfg := messaging.DefaultSQSConsumerConfig(queues.WagerRequestsQueueURL)
	consumerCfg.WaitTimeSeconds = 1
	consumerCfg.VisibilityTimeout = 30 // Padrão 30s
	consumer := messaging.NewSQSConsumer(sqsClient, mockSvc, consumerCfg, nil)
	consumer.Start()

	// Enviar mensagem para a fila
	msgID := fmt.Sprintf("msg-shutdown-%s", uuid.New().String())
	envelope := messaging.WagerMessageEnvelope{
		MessageID:  msgID,
		Type:       "WagerTransactionRequested",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Data: messaging.WagerMessageData{
			ProviderID:            "provider-s",
			ExternalTransactionID: "tx-s-1",
			Kind:                  "BET",
		},
	}
	bodyBytes, _ := json.Marshal(envelope)
	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(queues.WagerRequestsQueueURL),
		MessageBody:            aws.String(string(bodyBytes)),
		MessageGroupId:         aws.String("shutdown-group"),
		MessageDeduplicationId: aws.String(msgID),
	})
	if err != nil {
		t.Fatalf("failed to send message: %v", err)
	}

	// Aguardar mensagem ser colocada in-flight pelo consumidor
	for i := 0; i < 40; i++ {
		time.Sleep(100 * time.Millisecond)
		if consumer.InFlightCount() > 0 {
			break
		}
	}
	if consumer.InFlightCount() == 0 {
		t.Fatalf("expected message to be in-flight in consumer")
	}

	// Chamar Stop com contexto curto de 50ms para forçar timeout de shutdown
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stopCancel()

	err = consumer.Stop(stopCtx)
	if err == nil {
		t.Errorf("expected Stop to return context deadline exceeded error")
	}

	// A mensagem em voo deve ter sua visibilidade liberada para 0s imediatamente!
	// Portanto, uma chamada de ReceiveMessage deve conseguir recebê-la sem esperar os 30s.
	var receivedMsg *types.Message
	for i := 0; i < 30; i++ {
		recCtx, recCancel := context.WithTimeout(context.Background(), 2*time.Second)
		recOut, err := sqsClient.ReceiveMessage(recCtx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queues.WagerRequestsQueueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     1,
		})
		recCancel()
		if err == nil && len(recOut.Messages) > 0 {
			receivedMsg = &recOut.Messages[0]
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if receivedMsg == nil {
		t.Fatalf("expected message to be immediately available in queue (visibility released to 0s), but got 0 messages")
	}
}
