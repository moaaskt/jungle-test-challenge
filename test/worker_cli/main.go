package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
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

type WorkerOutput struct {
	StatusCode       int            `json:"statusCode"`
	Body             map[string]any `json:"body,omitempty"`
	RawBody          string         `json:"rawBody,omitempty"`
	Error            string         `json:"error,omitempty"`
	DurationMillis   int64          `json:"durationMillis"`
}

func main() {
	urlFlag := flag.String("url", "http://localhost:8080/wagering/transactions", "API URL")
	providerID := flag.String("provider", "provider-test", "Provider ID")
	externalID := flag.String("external-id", "", "External Transaction ID")
	playerID := flag.String("player-id", "", "Player ID")
	walletID := flag.String("wallet-id", "", "Wallet ID")
	roundID := flag.String("round-id", "round-1", "Round ID")
	gameID := flag.String("game-id", "game-1", "Game ID")
	kind := flag.String("kind", "BET", "Kind of transaction")
	amount := flag.String("amount", "80.00", "Amount")
	currency := flag.String("currency", "BRL", "Currency")
	idempotencyKey := flag.String("idempotency-key", "", "Idempotency-Key Header")

	flag.Parse()

	if *walletID == "" || *playerID == "" || *externalID == "" || *idempotencyKey == "" {
		fmt.Fprintf(os.Stderr, "missing required flags\n")
		os.Exit(1)
	}

	payload := WagerRequest{
		ProviderID: *providerID,
		ExternalID: *externalID,
		PlayerID:   *playerID,
		WalletID:   *walletID,
		RoundID:    *roundID,
		GameID:     *gameID,
		Kind:       *kind,
		Money: MoneyDTO{
			Amount:   *amount,
			Currency: *currency,
		},
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		emitOutput(WorkerOutput{Error: fmt.Sprintf("failed to marshal json: %v", err)})
		return
	}

	req, err := http.NewRequest("POST", *urlFlag, bytes.NewReader(bodyBytes))
	if err != nil {
		emitOutput(WorkerOutput{Error: fmt.Sprintf("failed to create request: %v", err)})
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", *idempotencyKey)

	start := time.Now()
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	duration := time.Since(start).Milliseconds()

	if err != nil {
		emitOutput(WorkerOutput{
			Error:          fmt.Sprintf("request failed: %v", err),
			DurationMillis: duration,
		})
		return
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)
	var respMap map[string]any
	_ = json.Unmarshal(respBytes, &respMap)

	emitOutput(WorkerOutput{
		StatusCode:     resp.StatusCode,
		Body:           respMap,
		RawBody:        string(respBytes),
		DurationMillis: duration,
	})
}

func emitOutput(out WorkerOutput) {
	enc := json.NewEncoder(os.Stdout)
	_ = enc.Encode(out)
}
