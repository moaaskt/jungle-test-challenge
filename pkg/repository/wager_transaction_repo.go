package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/money"
)

var ErrProviderExternalConflict = errors.New("provider external transaction id conflict")

// WagerTransactionRepository define a interface de persistência para as transações de apostas.
type WagerTransactionRepository interface {
	Insert(ctx context.Context, tx pgx.Tx, wt *domain.WagerTransaction) error
	GetByProviderAndExternalID(ctx context.Context, tx pgx.Tx, providerID, externalID string) (*domain.WagerTransaction, error)
	GetByID(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*domain.WagerTransaction, error)
	GetByIdempotencyKey(ctx context.Context, tx pgx.Tx, idempotencyKey string) (*domain.WagerTransaction, error)
	Update(ctx context.Context, tx pgx.Tx, wt *domain.WagerTransaction) error
	GetPendingByExternalReference(ctx context.Context, tx pgx.Tx, providerID, externalRefID string) ([]*domain.WagerTransaction, error)
	GetAllResolvable(ctx context.Context, tx pgx.Tx) ([]*PendingWithResolved, error)
	HasProcessedReversal(ctx context.Context, tx pgx.Tx, referenceID uuid.UUID, txType domain.TransactionType) (bool, error)
}

// PendingWithResolved agrupa uma transação PENDING_REFERENCE com a transação original
// já encontrada (PROCESSED/REJECTED/FAILED) ou nil se TTL expirou sem resolução.
type PendingWithResolved struct {
	Pending    *domain.WagerTransaction
	ResolvedBy *domain.WagerTransaction // nil se TTL expirado sem referência encontrada
}

type pgxWagerTransactionRepository struct{}

func NewWagerTransactionRepository() WagerTransactionRepository {
	return &pgxWagerTransactionRepository{}
}

// ---------------------------------------------------------------------------
// Colunas padrão para queries SELECT
// ---------------------------------------------------------------------------

const wagerTxSelectColumns = `
	id, origin, external_id, provider_id, idempotency_key, payload_hash,
	wallet_id, player_id, round_id, game_id, type, amount, currency,
	status, reference_id, external_reference_id, error_code,
	attempts, next_attempt_at, created_at, updated_at
`

// scanWagerTransaction extrai uma WagerTransaction de um pgx.Row.
func scanWagerTransaction(row pgx.Row) (*domain.WagerTransaction, error) {
	var (
		wt                           domain.WagerTransaction
		rawAmount                    int64
		origin                       string
		txType                       string
		status                       string
		externalIDPtr, providerIDPtr *string
		idemKeyPtr, payloadHashPtr   *string
		roundIDPtr, gameIDPtr        *string
		refIDPtr                     *uuid.UUID
		refExtIDPtr                  *string
		errCodePtr                   *string
		attempts                     int
		nextAttemptAt                *time.Time
	)

	err := row.Scan(
		&wt.ID,
		&origin,
		&externalIDPtr,
		&providerIDPtr,
		&idemKeyPtr,
		&payloadHashPtr,
		&wt.WalletID,
		&wt.PlayerID,
		&roundIDPtr,
		&gameIDPtr,
		&txType,
		&rawAmount,
		&wt.Currency,
		&status,
		&refIDPtr,
		&refExtIDPtr,
		&errCodePtr,
		&attempts,
		&nextAttemptAt,
		&wt.CreatedAt,
		&wt.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	moneyAmt, err := money.New(rawAmount, wt.Currency)
	if err != nil {
		return nil, err
	}

	result := domain.RehydrateWagerTransaction(
		wt.ID,
		domain.Origin(origin),
		wt.WalletID,
		wt.PlayerID,
		providerIDPtr,
		externalIDPtr,
		idemKeyPtr,
		payloadHashPtr,
		roundIDPtr,
		gameIDPtr,
		externalIDPtr,
		refExtIDPtr,
		refIDPtr,
		domain.TransactionType(txType),
		moneyAmt,
		wt.Currency,
		domain.TransactionStatus(status),
		errCodePtr,
		attempts,
		nextAttemptAt,
		wt.CreatedAt,
		wt.UpdatedAt,
	)

	return &result, nil
}

// scanWagerTransactions itera um pgx.Rows e retorna slice de WagerTransaction.
func scanWagerTransactions(rows pgx.Rows) ([]*domain.WagerTransaction, error) {
	defer rows.Close()
	var results []*domain.WagerTransaction
	for rows.Next() {
		wt, err := scanWagerTransaction(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, wt)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (r *pgxWagerTransactionRepository) Insert(ctx context.Context, tx pgx.Tx, wt *domain.WagerTransaction) error {
	query := `
		INSERT INTO wager_transactions (
			id, origin, external_id, provider_id, idempotency_key, payload_hash, 
			wallet_id, player_id, round_id, game_id, type, amount, currency, 
			status, reference_id, external_reference_id, error_code,
			attempts, next_attempt_at, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, 
			$7, $8, $9, $10, $11, $12, $13, 
			$14, $15, $16, $17,
			$18, $19, $20, $21
		)
	`
	_, err := tx.Exec(ctx, query,
		wt.ID,
		wt.Origin,
		wt.ExternalID,
		wt.ProviderID,
		wt.IdempotencyKey,
		wt.PayloadHash,
		wt.WalletID,
		wt.PlayerID,
		wt.RoundID,
		wt.GameID,
		wt.Type,
		wt.Amount.Amount(),
		wt.Currency,
		wt.Status,
		wt.ReferenceID,
		wt.ReferenceExternalTransactionID,
		wt.FailureCode,
		wt.Attempts,
		wt.NextAttemptAt,
		wt.CreatedAt,
		wt.UpdatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "uk_wager_tx_provider_external" {
			return ErrProviderExternalConflict
		}
		return err
	}
	return nil
}

func (r *pgxWagerTransactionRepository) GetByProviderAndExternalID(ctx context.Context, tx pgx.Tx, providerID, externalID string) (*domain.WagerTransaction, error) {
	query := `SELECT ` + wagerTxSelectColumns + `
		FROM wager_transactions
		WHERE provider_id = $1 AND external_id = $2
	`
	row := tx.QueryRow(ctx, query, providerID, externalID)
	return scanWagerTransaction(row)
}

func (r *pgxWagerTransactionRepository) GetByID(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*domain.WagerTransaction, error) {
	query := `SELECT ` + wagerTxSelectColumns + `
		FROM wager_transactions
		WHERE id = $1
	`
	row := tx.QueryRow(ctx, query, id)
	return scanWagerTransaction(row)
}

func (r *pgxWagerTransactionRepository) GetByIdempotencyKey(ctx context.Context, tx pgx.Tx, idempotencyKey string) (*domain.WagerTransaction, error) {
	query := `SELECT ` + wagerTxSelectColumns + `
		FROM wager_transactions
		WHERE idempotency_key = $1
		LIMIT 1
	`
	row := tx.QueryRow(ctx, query, idempotencyKey)
	return scanWagerTransaction(row)
}


// Update persiste campos mutáveis de uma WagerTransaction após resolução.
func (r *pgxWagerTransactionRepository) Update(ctx context.Context, tx pgx.Tx, wt *domain.WagerTransaction) error {
	query := `
		UPDATE wager_transactions
		SET status = $1,
		    reference_id = $2,
		    error_code = $3,
		    attempts = $4,
		    next_attempt_at = $5,
		    updated_at = $6
		WHERE id = $7
	`
	cmdTag, err := tx.Exec(ctx, query,
		wt.Status,
		wt.ReferenceID,
		wt.FailureCode,
		wt.Attempts,
		wt.NextAttemptAt,
		wt.UpdatedAt,
		wt.ID,
	)
	if err != nil {
		return err
	}
	if cmdTag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// GetPendingByExternalReference retorna transações PENDING_REFERENCE que referenciam
// o external_id de outra transação do mesmo provider. Momento A — resolução inline.
// FOR UPDATE garante que duas goroutines não resolvam simultaneamente a mesma referência.
func (r *pgxWagerTransactionRepository) GetPendingByExternalReference(ctx context.Context, tx pgx.Tx, providerID, externalRefID string) ([]*domain.WagerTransaction, error) {
	query := `SELECT ` + wagerTxSelectColumns + `
		FROM wager_transactions
		WHERE status = 'PENDING_REFERENCE'
		  AND provider_id = $1
		  AND external_reference_id = $2
		FOR UPDATE
	`
	rows, err := tx.Query(ctx, query, providerID, externalRefID)
	if err != nil {
		return nil, err
	}
	return scanWagerTransactions(rows)
}

// GetAllResolvable retorna referências pendentes elegíveis para resolução pelo worker periódico.
// Inclui: (1) pendências cuja referência original já existe (PROCESSED/REJECTED/FAILED),
// e (2) pendências expiradas por TTL (60min) ou max tentativas (10).
// FOR UPDATE OF p SKIP LOCKED: segurança multi-instância sem deadlocks.
func (r *pgxWagerTransactionRepository) GetAllResolvable(ctx context.Context, tx pgx.Tx) ([]*PendingWithResolved, error) {
	query := `
		SELECT p.id, p.origin, p.external_id, p.provider_id, p.idempotency_key, p.payload_hash,
		       p.wallet_id, p.player_id, p.round_id, p.game_id, p.type, p.amount, p.currency,
		       p.status, p.reference_id, p.external_reference_id, p.error_code,
		       p.attempts, p.next_attempt_at, p.created_at, p.updated_at,
		       r.id AS r_id, r.origin AS r_origin, r.external_id AS r_external_id,
		       r.provider_id AS r_provider_id, r.idempotency_key AS r_idempotency_key,
		       r.payload_hash AS r_payload_hash, r.wallet_id AS r_wallet_id,
		       r.player_id AS r_player_id, r.round_id AS r_round_id, r.game_id AS r_game_id,
		       r.type AS r_type, r.amount AS r_amount, r.currency AS r_currency,
		       r.status AS r_status, r.reference_id AS r_reference_id,
		       r.external_reference_id AS r_external_reference_id, r.error_code AS r_error_code,
		       r.attempts AS r_attempts, r.next_attempt_at AS r_next_attempt_at,
		       r.created_at AS r_created_at, r.updated_at AS r_updated_at
		FROM wager_transactions p
		JOIN wager_transactions r
		  ON r.provider_id = p.provider_id
		  AND r.external_id = p.external_reference_id
		WHERE p.status = 'PENDING_REFERENCE'
		  AND (p.next_attempt_at IS NULL OR p.next_attempt_at <= now())
		FOR UPDATE OF p SKIP LOCKED
		LIMIT 100
	`
	rows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*PendingWithResolved
	for rows.Next() {
		var (
			// pending fields
			pID, pWalletID                   uuid.UUID
			pOrigin, pTxType, pStatus        string
			pCurrency, pPlayerID             string
			pExternalID, pProviderID         *string
			pIdemKey, pPayloadHash           *string
			pRoundID, pGameID                *string
			pRefID                           *uuid.UUID
			pRefExtID                        *string
			pErrCode                         *string
			pAttempts                        int
			pNextAttemptAt                   *time.Time
			pAmount                          int64
			pCreatedAt, pUpdatedAt           time.Time

			// resolved fields
			rID, rWalletID                   uuid.UUID
			rOrigin, rTxType, rStatus        string
			rCurrency, rPlayerID             string
			rExternalID, rProviderID         *string
			rIdemKey, rPayloadHash           *string
			rRoundID, rGameID                *string
			rRefID                           *uuid.UUID
			rRefExtID                        *string
			rErrCode                         *string
			rAttempts                        int
			rNextAttemptAt                   *time.Time
			rAmount                          int64
			rCreatedAt, rUpdatedAt           time.Time
		)

		err := rows.Scan(
			// pending
			&pID, &pOrigin, &pExternalID, &pProviderID, &pIdemKey, &pPayloadHash,
			&pWalletID, &pPlayerID, &pRoundID, &pGameID, &pTxType, &pAmount, &pCurrency,
			&pStatus, &pRefID, &pRefExtID, &pErrCode,
			&pAttempts, &pNextAttemptAt, &pCreatedAt, &pUpdatedAt,
			// resolved
			&rID, &rOrigin, &rExternalID, &rProviderID, &rIdemKey, &rPayloadHash,
			&rWalletID, &rPlayerID, &rRoundID, &rGameID, &rTxType, &rAmount, &rCurrency,
			&rStatus, &rRefID, &rRefExtID, &rErrCode,
			&rAttempts, &rNextAttemptAt, &rCreatedAt, &rUpdatedAt,
		)
		if err != nil {
			return nil, err
		}

		pMoney, err := money.New(pAmount, pCurrency)
		if err != nil {
			return nil, err
		}
		rMoney, err := money.New(rAmount, rCurrency)
		if err != nil {
			return nil, err
		}

		pending := domain.RehydrateWagerTransaction(
			pID, domain.Origin(pOrigin), pWalletID, pPlayerID,
			pProviderID, pExternalID, pIdemKey, pPayloadHash,
			pRoundID, pGameID, pExternalID, pRefExtID, pRefID,
			domain.TransactionType(pTxType), pMoney, pCurrency,
			domain.TransactionStatus(pStatus), pErrCode,
			pAttempts, pNextAttemptAt, pCreatedAt, pUpdatedAt,
		)

		resolved := domain.RehydrateWagerTransaction(
			rID, domain.Origin(rOrigin), rWalletID, rPlayerID,
			rProviderID, rExternalID, rIdemKey, rPayloadHash,
			rRoundID, rGameID, rExternalID, rRefExtID, rRefID,
			domain.TransactionType(rTxType), rMoney, rCurrency,
			domain.TransactionStatus(rStatus), rErrCode,
			rAttempts, rNextAttemptAt, rCreatedAt, rUpdatedAt,
		)

		results = append(results, &PendingWithResolved{
			Pending:    &pending,
			ResolvedBy: &resolved,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Caso 2: pendências expiradas por TTL (60 min) ou max tentativas (10) sem referência encontrada
	expiredQuery := `SELECT ` + wagerTxSelectColumns + `
		FROM wager_transactions
		WHERE status = 'PENDING_REFERENCE'
		  AND (attempts >= 10 OR created_at < now() - interval '60 minutes')
		FOR UPDATE SKIP LOCKED
		LIMIT 100
	`
	expiredRows, err := tx.Query(ctx, expiredQuery)
	if err != nil {
		return nil, err
	}
	expiredTxs, err := scanWagerTransactions(expiredRows)
	if err != nil {
		return nil, err
	}
	for _, expired := range expiredTxs {
		// Verificar se já não foi incluído no primeiro resultado (JOIN pode ter pego)
		alreadyIncluded := false
		for _, r := range results {
			if r.Pending.ID == expired.ID {
				alreadyIncluded = true
				break
			}
		}
		if !alreadyIncluded {
			results = append(results, &PendingWithResolved{
				Pending:    expired,
				ResolvedBy: nil, // TTL expirado, sem referência encontrada
			})
		}
	}

	return results, nil
}

// HasProcessedReversal verifica se já existe uma transação PROCESSED
// de tipo reverso (REFUND ou ROLLBACK) para a mesma referência.
// Previne dupla reversão conforme Seção 7 do desafio.
func (r *pgxWagerTransactionRepository) HasProcessedReversal(ctx context.Context, tx pgx.Tx, referenceID uuid.UUID, txType domain.TransactionType) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT 1 FROM wager_transactions
			WHERE reference_id = $1
			  AND type = $2
			  AND status = 'PROCESSED'
		)
	`
	var exists bool
	err := tx.QueryRow(ctx, query, referenceID, txType).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}
