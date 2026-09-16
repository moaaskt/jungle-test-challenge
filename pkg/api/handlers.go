package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/service"
)

type Handlers struct {
	wagerService service.WagerService
}

func NewHandlers(wagerService service.WagerService) *Handlers {
	return &Handlers{
		wagerService: wagerService,
	}
}

type OpenWalletRequest struct {
	PlayerID string `json:"player_id"`
	Currency string `json:"currency"`
}

func (h *Handlers) HandleOpenWallet(w http.ResponseWriter, r *http.Request) {
	var req OpenWalletRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request payload")
		return
	}

	if req.PlayerID == "" || req.Currency == "" {
		h.respondError(w, http.StatusBadRequest, "player_id and currency are required")
		return
	}

	wallet, err := h.wagerService.OpenWallet(r.Context(), req.PlayerID, req.Currency)
	if err != nil {
		h.respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.respondJSON(w, http.StatusCreated, map[string]any{
		"id":         wallet.ID,
		"player_id":  wallet.PlayerID,
		"currency":   wallet.Currency,
		"balance":    wallet.Balance().FormattedAmount(),
		"version":    wallet.Version,
		"created_at": wallet.CreatedAt,
	})
}

type WagerRequest struct {
	ProviderID     string `json:"provider_id"`
	ExternalID     string `json:"external_id"`
	IdempotencyKey string `json:"idempotency_key"`
	PayloadHash    string `json:"payload_hash"`
	PlayerID       string `json:"player_id"`
	RoundID        string `json:"round_id"`
	GameID         string `json:"game_id"`
	Type           string `json:"type"`
	Amount         int64  `json:"amount"` // Em unidades mínimas
	Currency       string `json:"currency"`
}

func (h *Handlers) HandleWagerTransaction(w http.ResponseWriter, r *http.Request) {
	var req WagerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request payload")
		return
	}

	svcReq := service.ProcessWagerRequest{
		Origin:         domain.OriginExternal,
		PlayerID:       req.PlayerID,
		ProviderID:     &req.ProviderID,
		ExternalID:     &req.ExternalID,
		IdempotencyKey: &req.IdempotencyKey,
		PayloadHash:    &req.PayloadHash,
		RoundID:        &req.RoundID,
		GameID:         &req.GameID,
		Type:           domain.TransactionType(req.Type),
		Amount:         req.Amount,
		Currency:       req.Currency,
	}

	tx, err := h.wagerService.ProcessWager(r.Context(), svcReq)
	
	if err != nil {
		// Mapear erros de domínio para status HTTP adequados
		if errors.Is(err, domain.ErrInsufficientFunds) || errors.Is(err, domain.ErrCurrencyMismatch) || errors.Is(err, domain.ErrZeroAmountRequired) {
			h.respondError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		h.respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]any{
		"transaction_id": tx.ID,
		"status":         tx.Status,
		"amount":         tx.Amount.FormattedAmount(),
		"currency":       tx.Currency,
		"failure_code":   tx.FailureCode,
	})
}

func (h *Handlers) HandleLiveness(w http.ResponseWriter, r *http.Request) {
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "UP"})
}

func (h *Handlers) HandleReadiness(w http.ResponseWriter, r *http.Request) {
	// A validação profunda de readiness (ex: ping no BD) será integrada depois, 
	// por ora retornamos OK se o servidor HTTP está de pé.
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "READY"})
}

func (h *Handlers) respondJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(payload)
}

func (h *Handlers) respondError(w http.ResponseWriter, status int, message string) {
	h.respondJSON(w, status, map[string]string{"error": message})
}
