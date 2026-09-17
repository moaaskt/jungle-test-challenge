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

---

## 6. Transactional Outbox Pattern e Relayer Multi-Publisher

### 6.1. Garantia de Atomicidade e Invariante Post-Commit
- Todo evento de integração (`WagerTransactionProcessed`, `WagerTransactionRejected`, `WalletBalanceChanged`, `WagerTransactionPendingReference`) é gerado no domínio e persistido na tabela `outbox_events` rigorosamente dentro da **mesma transação SQL** (`pgx.Tx`) que atualiza a carteira, gera a entrada append-only no ledger e persiste o registro de idempotência.
- Se a transação sofrer `ROLLBACK`, absolutamente nenhum registro de outbox é gravado, eliminando mensagens-fantasma.
- A publicação externa nunca ocorre dentro da transação HTTP/SQL: é sempre delegada ao background worker `OutboxRelayer`, que despacha eventos exclusivamente após o commit bem-sucedido no banco de dados.

### 6.2. Estrutura do Envelope e Imutabilidade dos Eventos
O envelope do evento segue estritamente as diretrizes da Seção 11 da especificação:
- `eventId`: UUID v4 estável gerado na emissão do evento. Mantém-se imutável em qualquer republicação ou recuperação de falha.
- `eventType`: Nome do evento (`WagerTransactionProcessed`, `WagerTransactionRejected`, `WalletBalanceChanged`, `WagerTransactionPendingReference`).
- `aggregateId`: Identificador do agregado raiz (`walletId` para `WalletBalanceChanged` ou `transactionId` para transações de aposta).
- `correlationId`: Mapeado para o header `Idempotency-Key` da requisição HTTP (ou `messageId` do SQS na Fase 7). Para aberturas internas sem chave externa, adota o ID da transação.
- `causationId`: Permanece `nil` nesta Fase 6, pois as ações derivam de chamadas primárias de API. Fica reservado para eventos desencadeados por outros eventos (ex: resolução de referências na Fase 8).
- `occurredAt`: Timestamp no padrão UTC RFC 3339 Nano (`time.RFC3339Nano`).
- `version`: Inteiro tipado (versão 1 do schema de evento).
- `data`: Snapshot JSONB imutável contendo os dados concretos da operação, com valores monetários expressos em strings decimais formatadas (ex: `"100.00"`, `"0.00"`).

### 6.3. Disputa Multi-Publisher via `SELECT ... FOR UPDATE SKIP LOCKED`
Para viabilizar escala horizontal sem duplicação de mensagens e sem deadlocks:
- O relayer consulta os registros pendentes através de uma CTE atômica:
  ```sql
  WITH claimed AS (
      SELECT id
      FROM outbox_events
      WHERE status = 'PENDING'
        AND next_attempt_at <= NOW()
        AND (locked_until IS NULL OR locked_until < NOW())
      ORDER BY created_at ASC
      LIMIT $1
      FOR UPDATE SKIP LOCKED
  )
  UPDATE outbox_events o
  SET locked_until = NOW() + ($2 || ' seconds')::interval,
      retry_count = retry_count + 1
  FROM claimed
  WHERE o.id = claimed.id
  RETURNING o.id, o.aggregate_type, o.aggregate_id, o.event_type, o.payload, o.status,
            o.retry_count, o.next_attempt_at, o.locked_until, o.last_error, o.created_at, o.processed_at;
  ```
- O uso de `SKIP LOCKED` assegura que N workers em execução simultânea não bloqueiem uns aos outros e nunca reivindiquem o mesmo registro.

### 6.4. Recuperação de Trabalho Abandonado e Backoff Exponencial
- **Recuperação de Crash**: Cada reivindicação define uma locação temporal (`locked_until = NOW() + leaseDuration`). Se o worker que reivindicou um lote sofrer falha de hardware, restart do pod ou interrupção de rede antes de confirmar, o lease expira e outro publisher assume o processamento do evento mantendo seu `eventId` inalterado.
- **Backoff Exponencial com Jitter/Capping**: Em caso de falha no publisher, o próximo envio é agendado via `next_attempt_at = NOW() + baseBackoff * 2^(retry_count-1)` (limitado a `maxBackoff`).
- **Terminalidade `FAILED`**: Ao atingir `maxRetries` tentativas sem sucesso, o status transiciona para `FAILED` para fins de auditoria e inspeção de DLQ.

### 6.5. Observabilidade e Lag da Outbox
- O método `GetLag` da camada de persistência expõe o total de eventos pendentes (`pendingCount`) e a idade do evento mais antigo aguardando despacho (`oldestPendingAge`), provendo a métrica de "atraso da outbox" exigida pela Seção 12 da especificação do desafio.

---

## 7. Mensageria Assíncrona AWS SQS e Padrão Inbox

### 7.1. Topologia de Filas e Configurações SQS FIFO
O serviço integra com a infraestrutura AWS SQS (LocalStack local e AWS em produção) provisionando automaticamente as seguintes filas FIFO:
1. `wager-transactions.fifo`: Fila principal de entrada de requisições de apostas assíncronas (`WagerTransactionRequested`). Configurada com `FifoQueue: true`, `ContentBasedDeduplication: false` e `RedrivePolicy` apontando para a DLQ com `maxReceiveCount: 5`.
2. `wager-transactions-dlq.fifo`: Fila de Dead Letter (DLQ) FIFO para retenção e quarentena de mensagens envenenadas ou erros permanentes após esgotamento de tentativas.
3. `wager-events.fifo`: Fila de saída FIFO para onde o `OutboxRelayer` despacha os eventos transacionais de integração gerados pelo sistema (`WagerTransactionProcessed`, `WagerTransactionRejected`, `WalletBalanceChanged`, `WagerTransactionPendingReference`).

### 7.2. Semântica de Particionamento e Deduplicação Broker-Level
- `MessageGroupId`: Mapeado para o `playerId` (ou `aggregateId` na saída), garantindo preservação estrita de ordem sequencial por jogador/carteira, ao mesmo tempo em que permite consumo concorrente paralelo entre jogadores distintos.
- `MessageDeduplicationId`: Na publicação de saída da Outbox, é atribuído o `eventId` imutável. Na entrada, o envelope porta o `messageId` durável.

### 7.3. Padrão Inbox e Atomicidade Transacional
- Toda mensagem recebida é registrada na tabela `inbox_messages` (`id`, `message_id`, `consumer_name`, `source`, `payload_hash`, `received_at`, `processed_at`) rigorosamente dentro da **mesma transação SQL** (`pgx.Tx`) que atualiza a carteira (`wallets`), insere o registro da aposta (`wager_transactions`), gera a entrada append-only no ledger (`wallet_ledger_entries`) e enfileira os eventos da Outbox (`outbox_events`).
- Se houver falha ou rollback durante a execução, nada é comitado e a mensagem não entra na Inbox, permitindo retry natural pelo broker.

### 7.4. Interrupção Pós-Commit e Reentrega Segura (Item 5 da Seção 13 do Desafio)
- Se a instância sofrer interrupção após o commit bem-sucedido no PostgreSQL, mas antes da chamada a `sqs.DeleteMessage`, a mensagem será reentregue pelo SQS após o vencimento do `VisibilityTimeout`.
- Ao receber a mensagem reentregue, o consumidor consulta a Inbox. Como o registro já foi comitado, o caso de uso detecta `IdempotentReplay: true`.
- O consumidor **não retorna erro** e executa imediatamente a chamada `sqs.DeleteMessage(ctx, queueURL, receiptHandle)`, expurgando a duplicata da fila de forma silenciosa e idempotente, sem degradar métricas nem reenviar para DLQ.

### 7.5. Mensagens com Dependência Futura (`PENDING_REFERENCE`)
- Conforme as Seções 6.3, 6.5 e 10 do Desafio, quando uma mensagem de `REFUND` ou `ROLLBACK` chega antes da aposta original, ela é persistida com status `PENDING_REFERENCE` e o evento `WagerTransactionPendingReference` é gravado na Outbox, confirmando a Inbox na mesma transação atômica.
- O consumidor SQS considera o registro `PENDING_REFERENCE` como um processamento com sucesso no broker e remove a mensagem da fila (`sqs.DeleteMessage`), liberando a partição FIFO e delegando a reconciliação assíncrona ao worker de resolução de referências (Fase 8).

### 7.6. Rejeições de Negócio Terminais
- Rejeições de negócio confirmadas (ex: saldo insuficiente para uma aposta) são decisões válidas e finais. O registro é persistido com status `REJECTED`, evento `WagerTransactionRejected` é emitido na Outbox e a mensagem é removida do broker (`sqs.DeleteMessage`), impedindo loops desnecessários e evitando poluição da DLQ com regras de negócio esperadas.

### 7.7. Graceful Shutdown e Liberação Imediata de Visibilidade (0s)
- Em sinal de desligamento (`SIGTERM` / `Stop`), o consumidor encerra o polling cancelando o contexto do leitor e aguarda as mensagens em voo finalizarem até o prazo do shutdown.
- Se o prazo limite for atingido com mensagens ainda não comitadas, o consumidor aciona `sqs.ChangeMessageVisibility` com `VisibilityTimeout: 0` para cada mensagem pendente em voo.
- As mensagens são imediatamente liberadas de volta ao broker para redistribuição instantânea por outros pods/instâncias, sem reter os 30 segundos do visibility timeout padrão. Caso a goroutine retardada termine após a liberação, ela ignora a deleção (`Skipping delete for message whose visibility was released to 0s`), preservando a integridade da fila.


