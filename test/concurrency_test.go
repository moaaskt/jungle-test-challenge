package test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

type MoneyDTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type WagerRequest struct {
	ProviderID     string   `json:"providerId"`
	ExternalID     string   `json:"externalTransactionId"`
	PlayerID       string   `json:"playerId"`
	WalletID       string   `json:"walletId"`
	RoundID        string   `json:"roundId"`
	GameID         string   `json:"gameId"`
	Kind           string   `json:"kind"`
	Money          MoneyDTO `json:"money"`
	ReferenceExtID *string  `json:"referenceExternalTransactionId,omitempty"`
}

func TestAPI_ConcurrencyIdempotency(t *testing.T) {
	// 1. Create a Wallet first
	walletPayload := map[string]any{
		"playerId": "conc-player-" + uuid.NewString(),
		"initialBalance": map[string]string{
			"amount":   "1000.00",
			"currency": "BRL",
		},
	}
	body, _ := json.Marshal(walletPayload)
	resp, err := http.Post("http://localhost:8080/wallets", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to create wallet: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 201 Created for wallet, got %d. Body: %s", resp.StatusCode, string(b))
	}

	var walletResp map[string]any
	json.NewDecoder(resp.Body).Decode(&walletResp)
	walletID := walletResp["id"].(string)

	// 2. Prepare the concurrent Wager Request
	reqPayload := WagerRequest{
		ProviderID: "provider-x",
		ExternalID: "txn-" + uuid.NewString(),
		PlayerID:   walletPayload["playerId"].(string),
		WalletID:   walletID,
		RoundID:    "round-1",
		GameID:     "fortune-chimp",
		Kind:       "BET",
		Money: MoneyDTO{
			Amount:   "25.00",
			Currency: "BRL",
		},
	}
	reqBytes, _ := json.Marshal(reqPayload)
	idempotencyKey := "idem-key-" + uuid.NewString()

	// 3. Fire 30 requests in parallel
	numRequests := 30
	var wg sync.WaitGroup
	wg.Add(numRequests)

	type testResult struct {
		StatusCode       int
		IdempotentReplay bool
		TransactionID    string
		Balance          string
	}

	results := make(chan testResult, numRequests)

	t.Logf("Firing %d identical requests concurrently to /wagering/transactions", numRequests)

	// Ensure they all fire as close to the same time as possible
	startSignal := make(chan struct{})

	for i := 0; i < numRequests; i++ {
		go func() {
			defer wg.Done()

			// Wait for the signal to start
			<-startSignal

			req, _ := http.NewRequest("POST", "http://localhost:8080/wagering/transactions", bytes.NewReader(reqBytes))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", idempotencyKey)

			client := &http.Client{Timeout: 5 * time.Second}
			res, err := client.Do(req)
			if err != nil {
				// We don't fail here, just record it
				return
			}
			defer res.Body.Close()

			var respMap map[string]any
			json.NewDecoder(res.Body).Decode(&respMap)

			replay := false
			if v, ok := respMap["idempotentReplay"].(bool); ok {
				replay = v
			}

			txId := ""
			if v, ok := respMap["transactionId"].(string); ok {
				txId = v
			}

			bal := ""
			if bMap, ok := respMap["balance"].(map[string]any); ok {
				if a, ok2 := bMap["amount"].(string); ok2 {
					bal = a
				}
			}

			results <- testResult{
				StatusCode:       res.StatusCode,
				IdempotentReplay: replay,
				TransactionID:    txId,
				Balance:          bal,
			}
		}()
	}

	// Go!
	close(startSignal)
	wg.Wait()
	close(results)

	var successCount int
	var replayCount int
	var originalTxID string
	var finalBalance string

	for r := range results {
		if r.StatusCode == 200 {
			if !r.IdempotentReplay {
				successCount++
				originalTxID = r.TransactionID
				finalBalance = r.Balance
			} else {
				replayCount++
				// In a replay, txID and balance must match the original exactly
				if originalTxID != "" && r.TransactionID != originalTxID {
					t.Errorf("Replay returned different transactionID: %s (expected %s)", r.TransactionID, originalTxID)
				}
				if finalBalance != "" && r.Balance != finalBalance {
					t.Errorf("Replay returned different balance: %s (expected %s)", r.Balance, finalBalance)
				}
			}
		} else {
			t.Errorf("Unexpected status code %d", r.StatusCode)
		}
	}

	t.Logf("Processed: %d, Replays: %d", successCount, replayCount)

	if successCount != 1 {
		t.Errorf("Expected exactly 1 request to be processed normally, got %d", successCount)
	}

	if replayCount != numRequests-1 {
		t.Errorf("Expected exactly %d replays, got %d", numRequests-1, replayCount)
	}

	if finalBalance != "975.00" {
		t.Errorf("Expected final balance to be 975.00, got %s", finalBalance)
	}
}
