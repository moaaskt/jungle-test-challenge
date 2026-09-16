package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// IdempotencyRecord represents a processed request's result to be replayed for idempotent calls.
type IdempotencyRecord struct {
	ID             uuid.UUID
	Key            string
	PayloadHash    string
	ResponseStatus int
	ResponseBody   json.RawMessage
	CreatedAt      time.Time
}
