package idempotency

import (
	"testing"
)

func TestHashPayload_Deterministic(t *testing.T) {
	payload1 := BusinessPayload{
		ExternalTransactionID: "txn-123",
		GameID:                "fortune-chimp",
		Kind:                  "BET",
		Money: MoneyPayload{
			Amount:   "25.00",
			Currency: "BRL",
		},
		PlayerID:   "player-456",
		ProviderID: "provider-a",
		RoundID:    "round-789",
		WalletID:   "wallet-abc",
	}

	payload2 := BusinessPayload{
		WalletID:              "wallet-abc",
		ProviderID:            "provider-a",
		PlayerID:              "player-456",
		Money: MoneyPayload{Currency: "BRL", Amount: "25.00"},
		Kind:                  "BET",
		GameID:                "fortune-chimp",
		ExternalTransactionID: "txn-123",
		RoundID:               "round-789",
	}

	hash1, err := HashPayload(payload1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	hash2, err := HashPayload(payload2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if hash1 != hash2 {
		t.Errorf("expected deterministic hash. hash1: %s, hash2: %s", hash1, hash2)
	}
}

func TestHashPayload_Difference(t *testing.T) {
	payload1 := BusinessPayload{
		ExternalTransactionID: "txn-123",
		Money: MoneyPayload{
			Amount:   "25.00",
			Currency: "BRL",
		},
	}

	payload2 := BusinessPayload{
		ExternalTransactionID: "txn-123",
		Money: MoneyPayload{
			Amount:   "26.00",
			Currency: "BRL",
		},
	}

	hash1, err := HashPayload(payload1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	hash2, err := HashPayload(payload2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if hash1 == hash2 {
		t.Errorf("expected different hashes for different payloads")
	}
}
