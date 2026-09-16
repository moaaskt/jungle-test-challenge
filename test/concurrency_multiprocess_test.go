package test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os/exec"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type WorkerResult struct {
	StatusCode     int            `json:"statusCode"`
	Body           map[string]any `json:"body,omitempty"`
	RawBody        string         `json:"rawBody,omitempty"`
	Error          string         `json:"error,omitempty"`
	DurationMillis int64          `json:"durationMillis"`
}

func runWorkerProcess(t *testing.T, args []string) WorkerResult {
	cmd := exec.Command("/tmp/wager_worker", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		t.Fatalf("Worker execution failed: %v, stderr: %s", err, stderr.String())
	}

	var res WorkerResult
	if err := json.Unmarshal(stdout.Bytes(), &res); err != nil {
		t.Fatalf("Failed to parse worker stdout: %v. Output: %s", err, stdout.String())
	}
	return res
}

func TestConcurrency_Section8_Spec_ThreeIndependentProcesses(t *testing.T) {
	// 0. Garante que o binário /tmp/wager_worker existe
	compileCmd := exec.Command("go", "build", "-o", "/tmp/wager_worker", "./test/worker_cli")
	compileCmd.Dir = ".."
	if err := compileCmd.Run(); err != nil {
		// tenta no diretório corrente
		compileCmd2 := exec.Command("go", "build", "-o", "/tmp/wager_worker", "./test/worker_cli")
		if err2 := compileCmd2.Run(); err2 != nil {
			t.Fatalf("Failed to compile /tmp/wager_worker: %v / %v", err, err2)
		}
	}

	// 1. Criar Carteira 1 com 100.00 BRL
	player1ID := "player-sec8-" + uuid.NewString()
	wallet1Payload := map[string]any{
		"playerId": player1ID,
		"initialBalance": map[string]string{
			"amount":   "100.00",
			"currency": "BRL",
		},
	}
	body1, _ := json.Marshal(wallet1Payload)
	resp1, err := http.Post("http://localhost:8080/wallets", "application/json", bytes.NewReader(body1))
	if err != nil {
		t.Fatalf("Failed to create wallet 1: %v", err)
	}
	defer resp1.Body.Close()
	if resp1.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp1.Body)
		t.Fatalf("Expected 201 Created for wallet 1, got %d: %s", resp1.StatusCode, string(b))
	}
	var w1Resp map[string]any
	json.NewDecoder(resp1.Body).Decode(&w1Resp)
	wallet1ID := w1Resp["id"].(string)

	// 2. Criar Carteira 2 com 200.00 BRL para validar isolamento e paralelismo
	player2ID := "player-sec8-" + uuid.NewString()
	wallet2Payload := map[string]any{
		"playerId": player2ID,
		"initialBalance": map[string]string{
			"amount":   "200.00",
			"currency": "BRL",
		},
	}
	body2, _ := json.Marshal(wallet2Payload)
	resp2, err := http.Post("http://localhost:8080/wallets", "application/json", bytes.NewReader(body2))
	if err != nil {
		t.Fatalf("Failed to create wallet 2: %v", err)
	}
	defer resp2.Body.Close()
	var w2Resp map[string]any
	json.NewDecoder(resp2.Body).Decode(&w2Resp)
	wallet2ID := w2Resp["id"].(string)

	// 3. Configurar os 3 processos independentes
	// Processo 1: Carteira 1, aposta 1 de 80.00 BRL
	extID1 := "tx-p1-" + uuid.NewString()
	idemKey1 := "idem-p1-" + uuid.NewString()
	args1 := []string{
		"-wallet-id", wallet1ID,
		"-player-id", player1ID,
		"-external-id", extID1,
		"-idempotency-key", idemKey1,
		"-amount", "80.00",
		"-currency", "BRL",
		"-kind", "BET",
	}

	// Processo 2: Carteira 1, aposta 2 de 80.00 BRL (concorrente com Processo 1 na mesma carteira)
	extID2 := "tx-p2-" + uuid.NewString()
	idemKey2 := "idem-p2-" + uuid.NewString()
	args2 := []string{
		"-wallet-id", wallet1ID,
		"-player-id", player1ID,
		"-external-id", extID2,
		"-idempotency-key", idemKey2,
		"-amount", "80.00",
		"-currency", "BRL",
		"-kind", "BET",
	}

	// Processo 3: Carteira 2, aposta 3 de 50.00 BRL (concorrente em carteira distinta)
	extID3 := "tx-p3-" + uuid.NewString()
	idemKey3 := "idem-p3-" + uuid.NewString()
	args3 := []string{
		"-wallet-id", wallet2ID,
		"-player-id", player2ID,
		"-external-id", extID3,
		"-idempotency-key", idemKey3,
		"-amount", "50.00",
		"-currency", "BRL",
		"-kind", "BET",
	}

	var wg sync.WaitGroup
	wg.Add(3)

	var res1, res2, res3 WorkerResult

	startSignal := make(chan struct{})

	// Dispara Processo 1
	go func() {
		defer wg.Done()
		<-startSignal
		res1 = runWorkerProcess(t, args1)
	}()

	// Dispara Processo 2
	go func() {
		defer wg.Done()
		<-startSignal
		res2 = runWorkerProcess(t, args2)
	}()

	// Dispara Processo 3
	go func() {
		defer wg.Done()
		<-startSignal
		res3 = runWorkerProcess(t, args3)
	}()

	// Inicia os 3 simultaneamente
	close(startSignal)
	wg.Wait()

	t.Logf("Processo 1 (Wallet 1 - 80.00): Status %d, Body: %+v", res1.StatusCode, res1.Body)
	t.Logf("Processo 2 (Wallet 1 - 80.00): Status %d, Body: %+v", res2.StatusCode, res2.Body)
	t.Logf("Processo 3 (Wallet 2 - 50.00): Status %d, Body: %+v", res3.StatusCode, res3.Body)

	// 4. Verificações dos resultados HTTP
	if res1.StatusCode != http.StatusOK {
		t.Errorf("Processo 1 esperava HTTP 200 OK, recebeu %d: %s", res1.StatusCode, res1.RawBody)
	}
	if res2.StatusCode != http.StatusOK {
		t.Errorf("Processo 2 esperava HTTP 200 OK, recebeu %d: %s", res2.StatusCode, res2.RawBody)
	}
	if res3.StatusCode != http.StatusOK {
		t.Errorf("Processo 3 esperava HTTP 200 OK, recebeu %d: %s", res3.StatusCode, res3.RawBody)
	}

	// 5. Verificação da Seção 8 da Spec:
	// Na Wallet 1 (100.00 inicial, duas apostas de 80.00):
	// Exatamente 1 PROCESSED e exatamente 1 REJECTED
	status1 := res1.Body["status"].(string)
	status2 := res2.Body["status"].(string)

	processedCount := 0
	rejectedCount := 0
	if status1 == "PROCESSED" {
		processedCount++
	} else if status1 == "REJECTED" {
		rejectedCount++
	}

	if status2 == "PROCESSED" {
		processedCount++
	} else if status2 == "REJECTED" {
		rejectedCount++
	}

	if processedCount != 1 || rejectedCount != 1 {
		t.Fatalf("Esperado exatamente 1 PROCESSED e 1 REJECTED na Wallet 1. Obtido: processed=%d, rejected=%d", processedCount, rejectedCount)
	}

	// Carteira 2 (200.00 inicial, aposta de 50.00): deve ser PROCESSED
	status3 := res3.Body["status"].(string)
	if status3 != "PROCESSED" {
		t.Fatalf("Carteira 2 deveria ter sido PROCESSED em paralelo. Obtido: %s", status3)
	}

	// 6. Verificação direta no banco de dados via PostgreSQL Pool
	dbURL := "postgres://jungle:password@localhost:5432/jungle_test"
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("Failed to connect to database for verification: %v", err)
	}
	defer pool.Close()

	// A) Saldo final da Wallet 1 deve ser rigorosamente 20.00 BRL (2000 centavos)
	var finalBalance1 int64
	err = pool.QueryRow(context.Background(), "SELECT balance FROM wallets WHERE id = $1", wallet1ID).Scan(&finalBalance1)
	if err != nil {
		t.Fatalf("Failed to query wallet 1 balance: %v", err)
	}
	if finalBalance1 != 2000 {
		t.Errorf("Saldo final da Wallet 1 deveria ser 2000 centavos (20.00 BRL), obtido: %d", finalBalance1)
	}

	// B) Saldo final da Wallet 2 deve ser rigorosamente 150.00 BRL (15000 centavos)
	var finalBalance2 int64
	err = pool.QueryRow(context.Background(), "SELECT balance FROM wallets WHERE id = $1", wallet2ID).Scan(&finalBalance2)
	if err != nil {
		t.Fatalf("Failed to query wallet 2 balance: %v", err)
	}
	if finalBalance2 != 15000 {
		t.Errorf("Saldo final da Wallet 2 deveria ser 15000 centavos (150.00 BRL), obtido: %d", finalBalance2)
	}

	// C) Ledger da Wallet 1: Deve ter exatamente 1 lançamento DEBIT de 80.00 (8000 centavos)
	// (além do crédito OPENING de 10000 centavos)
	var debitCount int
	var debitAmount int64
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM wallet_ledger_entries WHERE wallet_id = $1 AND type = 'DEBIT'", wallet1ID).Scan(&debitCount, &debitAmount)
	if err != nil {
		t.Fatalf("Failed to query wallet 1 ledger debits: %v", err)
	}
	if debitCount != 1 {
		t.Errorf("Esperado exatamente 1 lançamento DEBIT no ledger da Wallet 1, encontrado: %d", debitCount)
	}
	if debitAmount != 8000 {
		t.Errorf("Valor do débito deveria ser 8000 centavos (80.00 BRL), encontrado: %d", debitAmount)
	}

	// 7. Teste de Reenvio Idempotente: Reenviar aposta 1 e aposta 2
	t.Log("Executando reenvio das requisições para validar replay idempotente...")
	replayRes1 := runWorkerProcess(t, args1)
	replayRes2 := runWorkerProcess(t, args2)

	if replayRes1.Body["idempotentReplay"] != true {
		t.Errorf("Replay da requisição 1 deveria ter idempotentReplay: true")
	}
	if replayRes2.Body["idempotentReplay"] != true {
		t.Errorf("Replay da requisição 2 deveria ter idempotentReplay: true")
	}

	// Saldo no banco continua rigorosamente 20.00 BRL após os reenvios
	var balanceAfterReplays int64
	_ = pool.QueryRow(context.Background(), "SELECT balance FROM wallets WHERE id = $1", wallet1ID).Scan(&balanceAfterReplays)
	if balanceAfterReplays != 2000 {
		t.Errorf("Saldo da Wallet 1 alterado após replays! Esperado 2000 centavos, obtido: %d", balanceAfterReplays)
	}

	t.Log("Sucesso absoluto! Todas as exigências da Seção 8 foram comprovadas entre 3 processos independentes.")
}
