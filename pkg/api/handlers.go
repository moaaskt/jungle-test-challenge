package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/idempotency"
	"github.com/moaaskt/jungle-test-challenge/pkg/money"
	"github.com/moaaskt/jungle-test-challenge/pkg/repository"
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

type MoneyDTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type OpenWalletRequest struct {
	PlayerID       string    `json:"playerId"`
	InitialBalance *MoneyDTO `json:"initialBalance,omitempty"`
}

func (h *Handlers) HandleOpenWallet(w http.ResponseWriter, r *http.Request) {
	var req OpenWalletRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request payload")
		return
	}

	if req.PlayerID == "" {
		h.respondError(w, http.StatusBadRequest, "playerId is required")
		return
	}

	var currency string
	var initialBalance int64
	if req.InitialBalance != nil {
		currency = req.InitialBalance.Currency
		if req.InitialBalance.Amount != "" {
			amt, err := money.Parse(req.InitialBalance.Amount, currency)
			if err != nil {
				h.respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid initial balance: %v", err))
				return
			}
			initialBalance = amt.Amount()
		}
	} else {
		// Padrão caso não envie initialBalance, mas como o teste não define padrão...
		// Normalmente precisaria da currency. Se não tem currency, falha.
		h.respondError(w, http.StatusBadRequest, "initialBalance is required to provide currency")
		return
	}

	wallet, err := h.wagerService.OpenWallet(r.Context(), req.PlayerID, currency, initialBalance)
	if err != nil {
		if errors.Is(err, repository.ErrWalletAlreadyExists) {
			h.respondError(w, http.StatusConflict, err.Error())
			return
		}
		h.respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.respondJSON(w, http.StatusCreated, map[string]any{
		"id":       wallet.ID,
		"playerId": wallet.PlayerID,
		"balance":  map[string]string{"amount": wallet.Balance().FormattedAmount(), "currency": wallet.Currency},
		"version":  wallet.Version,
	})
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

func (h *Handlers) HandleWagerTransaction(w http.ResponseWriter, r *http.Request) {
	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey == "" {
		h.respondError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}

	var req WagerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request payload")
		return
	}

	if req.WalletID == "" {
		h.respondError(w, http.StatusBadRequest, "walletId is required")
		return
	}

	amt, err := money.Parse(req.Money.Amount, req.Money.Currency)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid money: %v", err))
		return
	}

	businessPayload := idempotency.BusinessPayload{
		ProviderID:            req.ProviderID,
		ExternalTransactionID: req.ExternalID,
		PlayerID:              req.PlayerID,
		WalletID:              req.WalletID,
		RoundID:               req.RoundID,
		GameID:                req.GameID,
		Kind:                  req.Kind,
		Money: idempotency.MoneyPayload{
			Amount:   req.Money.Amount,
			Currency: req.Money.Currency,
		},
		ReferenceExternalTransactionID: req.ReferenceExtID,
	}

	payloadHash, err := idempotency.HashPayload(businessPayload)
	if err != nil {
		h.respondError(w, http.StatusInternalServerError, "failed to hash payload")
		return
	}

	svcReq := service.ProcessWagerRequest{
		Origin:         domain.OriginExternal,
		PlayerID:       req.PlayerID,
		ProviderID:     &req.ProviderID,
		ExternalID:     &req.ExternalID,
		IdempotencyKey: &idemKey,
		PayloadHash:    &payloadHash,
		RoundID:        &req.RoundID,
		GameID:         &req.GameID,
		Type:           domain.TransactionType(req.Kind),
		Amount:         amt.Amount(),
		Currency:       req.Money.Currency,
	}

	result, err := h.wagerService.ProcessWager(r.Context(), svcReq)

	if err != nil {
		if strings.Contains(err.Error(), "idempotency key conflict") {
			h.respondError(w, http.StatusConflict, err.Error())
			return
		}
		// Mapear erros de domínio para status HTTP adequados
		if errors.Is(err, domain.ErrInsufficientFunds) || errors.Is(err, domain.ErrCurrencyMismatch) || errors.Is(err, domain.ErrZeroAmountRequired) {
			h.respondError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		if errors.Is(err, repository.ErrOptimisticLockFailed) {
			h.respondError(w, http.StatusConflict, err.Error())
			return
		}
		h.respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if result.IdempotentReplay {
		// Substitui a flag false para true no JSON de resposta
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		var rawMap map[string]any
		json.Unmarshal(result.RawResponse, &rawMap)
		rawMap["idempotentReplay"] = true
		json.NewEncoder(w).Encode(rawMap)
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]any{
		"transactionId":    result.TransactionID,
		"status":           result.Status,
		"balance":          map[string]string{"amount": result.Balance.FormattedAmount(), "currency": result.Balance.Currency()},
		"idempotentReplay": false,
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
