package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/moaaskt/jungle-test-challenge/pkg/auth"
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

	// Validação de autorização antecipada: provedor autenticado deve corresponder ao providerId da aposta
	// Esta checagem executa estritamente ANTES de qualquer verificação ou retorno de replay do banco de dados.
	authCtx, ok := auth.GetAuthContext(r.Context())
	if ok && !authCtx.IsInternal {
		if req.ProviderID != authCtx.ProviderID {
			h.respondError(w, http.StatusForbidden, "forbidden: authenticated client cannot operate for another provider")
			return
		}
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
		if errors.Is(err, domain.ErrCrossProviderReplay) {
			h.respondError(w, http.StatusForbidden, err.Error())
			return
		}
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

// ---------------------------------------------------------------------------
// Endpoints de Consulta (Seção 9 do desafio)
// ---------------------------------------------------------------------------

// HandleGetWallet — GET /wallets/{walletId}
func (h *Handlers) HandleGetWallet(w http.ResponseWriter, r *http.Request) {
	walletIDStr := r.PathValue("walletId")
	walletID, err := uuid.Parse(walletIDStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid walletId format")
		return
	}

	wallet, err := h.wagerService.GetWallet(r.Context(), walletID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			h.respondError(w, http.StatusNotFound, "wallet not found")
			return
		}
		h.respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]any{
		"id":        wallet.ID,
		"walletId":  wallet.ID,
		"playerId":  wallet.PlayerID,
		"currency":  wallet.Currency,
		"balance":   map[string]string{"amount": wallet.Balance().FormattedAmount(), "currency": wallet.Currency},
		"version":   wallet.Version,
		"createdAt": wallet.CreatedAt,
		"updatedAt": wallet.UpdatedAt,
	})
}

// HandleGetLedger — GET /wallets/{walletId}/ledger?cursor=...&limit=50
func (h *Handlers) HandleGetLedger(w http.ResponseWriter, r *http.Request) {
	walletIDStr := r.PathValue("walletId")
	walletID, err := uuid.Parse(walletIDStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid walletId format")
		return
	}

	// Primeiro buscar a wallet para obter a currency
	wallet, err := h.wagerService.GetWallet(r.Context(), walletID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			h.respondError(w, http.StatusNotFound, "wallet not found")
			return
		}
		h.respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Parse limit (default 20, max 100)
	limit := 20
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		parsed, err := strconv.Atoi(limitStr)
		if err != nil || parsed < 1 || parsed > 100 {
			h.respondError(w, http.StatusBadRequest, "limit must be an integer between 1 and 100")
			return
		}
		limit = parsed
	}

	// Parse cursor opaco
	var cursor *repository.LedgerCursor
	if cursorStr := r.URL.Query().Get("cursor"); cursorStr != "" {
		decoded, err := repository.DecodeCursor(cursorStr)
		if err != nil {
			h.respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid cursor: %v", err))
			return
		}
		cursor = decoded
	}

	entries, nextCursor, err := h.wagerService.GetLedger(r.Context(), walletID, wallet.Currency, cursor, limit)
	if err != nil {
		h.respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Serializar entries para o JSON de resposta
	entryDTOs := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		entryDTOs = append(entryDTOs, map[string]any{
			"id":            e.ID,
			"transactionId": e.TransactionID,
			"type":          e.Type,
			"amount":        map[string]string{"amount": e.Amount.FormattedAmount(), "currency": e.Amount.Currency()},
			"balanceBefore":  map[string]string{"amount": e.BalanceBefore.FormattedAmount(), "currency": e.BalanceBefore.Currency()},
			"balanceAfter":   map[string]string{"amount": e.BalanceAfter.FormattedAmount(), "currency": e.BalanceAfter.Currency()},
			"createdAt":     e.CreatedAt,
		})
	}

	response := map[string]any{
		"walletId": walletID,
		"entries":  entryDTOs,
		"limit":    limit,
	}

	if nextCursor != nil {
		response["nextCursor"] = repository.EncodeCursor(*nextCursor)
	} else {
		response["nextCursor"] = nil
	}

	h.respondJSON(w, http.StatusOK, response)
}

// HandleGetTransaction — GET /wagering/transactions/{transactionId}
func (h *Handlers) HandleGetTransaction(w http.ResponseWriter, r *http.Request) {
	txIDStr := r.PathValue("transactionId")
	txID, err := uuid.Parse(txIDStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid transactionId format")
		return
	}

	tx, err := h.wagerService.GetTransaction(r.Context(), txID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			h.respondError(w, http.StatusNotFound, "transaction not found")
			return
		}
		h.respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Provedores externos só podem consultar transações pertencentes a eles mesmos
	authCtx, ok := auth.GetAuthContext(r.Context())
	if ok && !authCtx.IsInternal {
		if tx.ProviderID != nil && *tx.ProviderID != authCtx.ProviderID {
			h.respondError(w, http.StatusForbidden, "forbidden: provider can only access its own transactions")
			return
		}
	}

	h.respondJSON(w, http.StatusOK, h.transactionToDTO(tx))
}

// HandleGetTransactionByExternal — GET /providers/{providerId}/wagering/transactions/{externalTransactionId}
func (h *Handlers) HandleGetTransactionByExternal(w http.ResponseWriter, r *http.Request) {
	providerID := r.PathValue("providerId")
	externalTxID := r.PathValue("externalTransactionId")

	if providerID == "" || externalTxID == "" {
		h.respondError(w, http.StatusBadRequest, "providerId and externalTransactionId are required")
		return
	}

	// Provedores externos só podem consultar sua própria hierarquia de transações
	authCtx, ok := auth.GetAuthContext(r.Context())
	if ok && !authCtx.IsInternal {
		if providerID != authCtx.ProviderID {
			h.respondError(w, http.StatusForbidden, "forbidden: provider can only access its own transactions")
			return
		}
	}

	tx, err := h.wagerService.GetTransactionByExternal(r.Context(), providerID, externalTxID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			h.respondError(w, http.StatusNotFound, "transaction not found")
			return
		}
		h.respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.respondJSON(w, http.StatusOK, h.transactionToDTO(tx))
}

// transactionToDTO converte uma WagerTransaction para o formato JSON de resposta.
func (h *Handlers) transactionToDTO(tx *domain.WagerTransaction) map[string]any {
	dto := map[string]any{
		"id":            tx.ID,
		"transactionId": tx.ID,
		"walletId":      tx.WalletID,
		"playerId":      tx.PlayerID,
		"type":          tx.Type,
		"amount":        map[string]string{"amount": tx.Amount.FormattedAmount(), "currency": tx.Currency},
		"status":        tx.Status,
		"origin":        tx.Origin,
		"createdAt":     tx.CreatedAt,
		"updatedAt":     tx.UpdatedAt,
	}

	if tx.ReferenceID != nil {
		dto["referenceId"] = tx.ReferenceID
	} else {
		dto["referenceId"] = nil
	}

	extID := tx.ExternalID
	if extID == nil {
		extID = tx.ExternalTransactionID
	}
	if extID != nil {
		dto["externalId"] = *extID
		dto["externalTransactionId"] = *extID
	} else {
		dto["externalId"] = nil
		dto["externalTransactionId"] = nil
	}

	refExtID := tx.ReferenceExternalTransactionID
	if refExtID != nil {
		dto["externalReferenceId"] = *refExtID
		dto["referenceExternalTransactionId"] = *refExtID
	} else {
		dto["externalReferenceId"] = nil
		dto["referenceExternalTransactionId"] = nil
	}

	if tx.ProviderID != nil {
		dto["providerId"] = *tx.ProviderID
	}
	if tx.RoundID != nil {
		dto["roundId"] = *tx.RoundID
	} else {
		dto["roundId"] = nil
	}
	if tx.GameID != nil {
		dto["gameId"] = *tx.GameID
	} else {
		dto["gameId"] = nil
	}
	if tx.FailureCode != nil {
		dto["errorCode"] = *tx.FailureCode
		dto["failureCode"] = *tx.FailureCode
	}

	return dto
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
