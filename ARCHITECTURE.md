# Arquitetura — Jungle Gaming Backend Challenge

## 1. Visão Geral
Serviço distribuído de movimentação financeira e wagering em conformidade com o desafio de backend da Jungle Gaming.

---

## 2. Invariantes Arquiteturais (Cross-Phase Invariants)
1. **Money sem Float:** `Money` é representado internamente sempre em centavos (`int64`), sem nunca utilizar tipos `float32` ou `float64` para valores monetários.
2. **Outbox Post-Commit:** Registros do Transactional Outbox são gerados na mesma transação SQL da mutação financeira e a publicação/despacho externo só ocorre após o commit confirmado no banco de dados.
3. **Ledger Append-Only:** A tabela `wallet_ledger_entries` é estritamente append-only. Nunca executar `UPDATE` ou `DELETE` em lançamentos do ledger (garantido por trigger de imutabilidade no PostgreSQL).
4. **Idempotent Replay é Consulta:** O replay idempotente consulta o resultado originalmente persistido e o devolve (`idempotentReplay: true`), sem reprocessar regras de negócio, sem debitar/creditar saldo e sem gerar novas entradas de ledger.
5. **Isolamento de Lock por Carteira:** A coordenação de concorrência é granular por carteira (`wallet_id`). O lock em uma carteira nunca deve bloquear ou degradar requisições para carteiras diferentes.

---

## 3. Estratégia de Concorrência e Integridade Financeira
- **Locking Granular por Linha:** As transações financeiras adquirem lock pessimista na carteira alvo via `SELECT ... WHERE id = $1 FOR UPDATE` dentro da transação SQL (`pgx.Tx` com nível `ReadCommitted`).
- **Prevenção de Lost Updates e Deadlocks:** Como a ordenação de locks por carteira é unitária e delimitada à linha específica, não há deadlocks cruzados entre carteiras diferentes. Carteiras distintas executam simultaneamente com vazão máxima.
- **Defesa em Profundidade:** A cláusula `UPDATE wallets SET balance = $1, version = $2 WHERE id = $4 AND version = $5` valida adicionalmente a versão antes de confirmar.

---

## 4. Contrato de Resposta HTTP (`POST /wagering/transactions`)

| Cenário | HTTP Status | Corpo / Envelope de Resposta | Descrição |
| --- | --- | --- | --- |
| **Sucesso (`PROCESSED`)** | `200 OK` | `{"transactionId": "...", "status": "PROCESSED", "balance": {"amount": "...", "currency": "..."}, "idempotentReplay": false}` | Aposta/ganho executado com sucesso e saldo atualizado. |
| **Rejeição de Negócio (`REJECTED`)** | `200 OK` | `{"transactionId": "...", "status": "REJECTED", "balance": {"amount": "...", "currency": "..."}, "idempotentReplay": false}` | Decisão de negócio válida (ex: saldo insuficiente). A transação é registrada para auditoria, sem debitar saldo e sem criar ledger. |
| **Replay Idempotente** | `200 OK` | Envelope idêntico ao original persistido, com `"idempotentReplay": true` | Mesma chave de idempotência e payload idêntico. Retorna o resultado exato original. |
| **Entrada Inválida** | `400 Bad Request` | `{"error": "..."}` | Payload malformado, falta de header `Idempotency-Key`, `walletId` ausente ou formato de moeda inválido. |
| **Conflito de Idempotência** | `409 Conflict` | `{"error": "idempotency key conflict: ..."}` | Chave de idempotência reutilizada com payload de negócio divergente, ou `(providerId, externalTransactionId)` duplicado sob outra chave. |
| **Processamento Pendente** | `202 Accepted` | `{"transactionId": "...", "status": "PENDING_REFERENCE", ...}` | Operação depende de referência ainda não chegada (Fase 8). |
| **Indisponibilidade Transitória** | `503 Service Unavailable` ou `500 Internal Server Error` | `{"error": "..."}` | Falhas de conexão temporária de banco ou rede antes de estabelecer transação. |

---

## 5. Transações e Gerenciamento de Conexões
- As transações SQL (`pgx.Tx`) são abertas e comitadas explicitamente no `WagerService`.
- Toda mutação (carteira, transação de aposta, ledger e registro de idempotência) ocorre dentro do mesmo bloco atômico.
- Caso ocorra violação de unicidade de chave concorrente (`23505`), o serviço executa rollback imediato da transação atual, consulta o registro gravado pelo processo concorrente vencedor e retorna o replay de forma transparente.
