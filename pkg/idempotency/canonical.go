package idempotency

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// MoneyPayload represents the money object in the request
type MoneyPayload struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// BusinessPayload represents the exact business fields needed for the canonical JSON hash,
// explicitly excluding Idempotency-Key and transport metadata as per Spec Section 9.
type BusinessPayload struct {
	ExternalTransactionID          string       `json:"externalTransactionId"`
	GameID                         string       `json:"gameId"`
	Kind                           string       `json:"kind"`
	Money                          MoneyPayload `json:"money"`
	PlayerID                       string       `json:"playerId"`
	ProviderID                     string       `json:"providerId"`
	ReferenceExternalTransactionID *string      `json:"referenceExternalTransactionId,omitempty"`
	RoundID                        string       `json:"roundId"`
	WalletID                       string       `json:"walletId"`
}

// HashPayload calculates a deterministic SHA-256 hash of the canonical JSON representation
// of the BusinessPayload.
func HashPayload(payload BusinessPayload) (string, error) {
	// 1. Marshal to JSON to get the structure
	b, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}

	// 2. Unmarshal into a map[string]any to ensure alphabetical key sorting when re-marshaling
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return "", fmt.Errorf("failed to unmarshal into map: %w", err)
	}

	// 3. Marshal the map. Go's encoding/json package guarantees that map keys are sorted
	// alphabetically when marshaling.
	canonicalBytes, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("failed to marshal canonical map: %w", err)
	}

	// 4. Calculate SHA-256
	hash := sha256.Sum256(canonicalBytes)
	return hex.EncodeToString(hash[:]), nil
}
