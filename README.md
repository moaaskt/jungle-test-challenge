# Jungle Gaming — Backend Engineering Challenge

> **Serviço distribuído de carteira digital e processamento de apostas (Wagering Service)** Desenvolvido especialmente para o desafio técnico da Jungle Gaming.

[![Go Version](https://img.shields.io/badge/Go-1.25%2B-00ADD8?style=flat&logo=go)](https://golang.org)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=flat&logo=docker)](https://www.docker.com)
[![Architecture](https://img.shields.io/badge/Architecture-Event--Driven%20%7C%20EDA-blueviolet)](#arquitetura-e-padroes-adotados)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## Sumário
1. [Visão Geral e Destaques](#visão-geral-e-destaques)
2. [Comandos Oficiais em Destaque (Seção 15)](#comandos-oficiais-em-destaque-seção-15)
3. [Mapeamento de Requisitos e Evidências](#mapeamento-de-requisitos-e-evidências)
4. [Pré-requisitos do Ambiente](#pré-requisitos-do-ambiente)
5. [Guia de Inicialização Rápida (Checkout Limpo)](#guia-de-inicialização-rápida-checkout-limpo)
6. [Variáveis de Ambiente](#variáveis-de-ambiente)
7. [Banco de Dados e Evolução de Migrações](#banco-de-dados-e-evolução-de-migrações)
8. [Autenticação, IdP Keycloak e Geração de Tokens JWT](#autenticação-idp-keycloak-e-geração-de-tokens-jwt)
9. [Guia Prático de Chamadas da API (Exemplos com cURL e jq)](#guia-prático-de-chamadas-da-api-exemplos-com-curl-e-jq)
10. [Bateria de Testes e Cenários Extremos](#bateria-de-testes-e-cenários-extremos)
11. [Estrutura de Diretórios](#estrutura-de-diretórios)

---

## Visão Geral e Destaques

Este sistema foi projetado para operar em ambientes distribuídos de alta concorrência com **garantia matemática e contábil de integridade financeira**:
- **Zero Float & Imutabilidade Monetária**: Todos os valores monetários são manipulados exclusivamente em centavos inteiros (`int64`), utilizando o Value Object de domínio `money.Money`. Números de ponto flutuante (`float32`/`float64`) são estritamente proibidos em toda a base de código.
- **Ledger Imutável (Append-Only)**: O histórico financeiro (`wallet_ledger_entries`) é protegido por triggers no PostgreSQL contra qualquer operação de `UPDATE` ou `DELETE`.
- **Concorrência Granular por Linha**: Concorrência coordenada via `SELECT ... FOR UPDATE` pessimista delimitado à carteira específica. Não há lock global — carteiras distintas executam simultaneamente com paralelismo total e vazão máxima.
- **Idempotência Persistente e Canônica**: Deduplicação fundamentada em hash SHA-256 canônico do payload. Replays idempotentes devolvem o estado original persistido (`idempotentReplay: true`) como consulta pura, sem debitar/creditar e sem gerar novas linhas de ledger.
- **Transactional Outbox Pattern**: Publicação de eventos integrada na mesma transação SQL da mutação financeira, com despacho em lote assíncrono via `SELECT FOR UPDATE SKIP LOCKED` e locação temporal com leases anti-crash.
- **Mensageria AWS SQS FIFO e Padrão Inbox**: Consumo de filas FIFO particionadas por jogador (`MessageGroupId = walletId`), com deduplicação de broker, Inbox atômica e eliminação de duplicações pós-commit.
- **Resolução de Referências Fora de Ordem (Dual-Moment)**: Resolução reativa inline no mesmo commit do BET (Momento A) e worker periódico com backoff e TTL (Momento B).
- **Segurança OAuth 2.0 / OIDC**: Autenticação corporativa via Keycloak com fluxo Machine-to-Machine (`client_credentials`), cache JWKS em memória e isolamento estrito de transações e replays por provedor (`403 Forbidden`).
- **Observabilidade Nível Produção**: Métricas Prometheus nativas (`/metrics`), health checks profundos com ping real no PostgreSQL e SQS (`/health/ready`), e reconciliação contábil formal com garantia de não-mutação de saldo (`POST /wallets/:walletId/reconciliation`).
- **Empacotamento Estático Não-Root**: Imagem Docker minimalista multi-stage (`alpine:3.20`), compilada estaticamente com apenas **21.7 MB** de tamanho de conteúdo e executada com usuário sem privilégios (`appuser:10001`).

---

## Comandos Oficiais em Destaque (Seção 15)

Conforme expressamente exigido na Seção 15 do edital oficial, os 4 comandos a seguir compõem o ciclo padrão de execução e verificação do projeto:

```bash
# 1. Subida completa da aplicação e dependências a partir de checkout limpo
docker compose up --build

# 2. Execução da suíte completa de testes unitários e de integração
go test ./...

# 3. Execução da suíte completa com detector de concorrência ativado
go test -race ./...

# 4. Análise estática do compilador Go
go vet ./...
```

---

## Mapeamento de Requisitos e Evidências

A tabela abaixo correlaciona cada domínio arquitetural e requisito técnico do sistema com os mecanismos de garantia implementados, os arquivos de código correspondentes e as suítes de testes automatizados que comprovam seu comportamento:

| Domínio / Requisito | Mecanismos de Garantia Implementados | Implementação Principal | Validação Automatizada |
|---|---|---|---|
| **Integridade Financeira** | • Zero ponto flutuante<br>• Representação estrita em centavos inteiros (`int64`)<br>• Ledger imutável append-only via trigger SQL<br>• Auditoria de reconciliação contábil sem mutação de saldo | • [`pkg/money/money.go`](file:///home/moa-dev/projetos/jungletest/pkg/money/money.go)<br>• [`pkg/service/wager_service.go`](file:///home/moa-dev/projetos/jungletest/pkg/service/wager_service.go)<br>• [`pkg/repository/ledger_repo.go`](file:///home/moa-dev/projetos/jungletest/pkg/repository/ledger_repo.go)<br>• [`migrations/000001_init_schema.up.sql`](file:///home/moa-dev/projetos/jungletest/migrations/000001_init_schema.up.sql) | • [`pkg/money/money_test.go`](file:///home/moa-dev/projetos/jungletest/pkg/money/money_test.go)<br>• [`test/reconciliation_observability_test.go`](file:///home/moa-dev/projetos/jungletest/test/reconciliation_observability_test.go) |
| **Concorrência e Locks** | • Isolamento de processos concorrentes na mesma carteira<br>• Lock pessimista granular por linha (`SELECT FOR UPDATE`)<br>• Ausência total de lock global ou de tabela<br>• Prevenção matemática de deadlocks e lost updates | • [`pkg/service/wager_service.go`](file:///home/moa-dev/projetos/jungletest/pkg/service/wager_service.go)<br>• [`pkg/repository/wallet_repo.go`](file:///home/moa-dev/projetos/jungletest/pkg/repository/wallet_repo.go) | • [`test/concurrency_section8_test.go`](file:///home/moa-dev/projetos/jungletest/test/concurrency_section8_test.go)<br>• [`test/api_integration_test.go`](file:///home/moa-dev/projetos/jungletest/test/api_integration_test.go) |
| **Idempotência e Replay** | • Deduplicação atômica fundamentada em hash SHA-256 canônico<br>• Replay idempotente devolvido como consulta pura (`idempotentReplay: true`)<br>• Rejeição de payload conflitante para mesma chave (`409 Conflict`)<br>• Isolamento rigoroso de replay entre provedores distintos (`403 Forbidden`) | • [`pkg/idempotency/canonical.go`](file:///home/moa-dev/projetos/jungletest/pkg/idempotency/canonical.go)<br>• [`pkg/service/wager_service.go`](file:///home/moa-dev/projetos/jungletest/pkg/service/wager_service.go)<br>• [`pkg/api/handlers.go`](file:///home/moa-dev/projetos/jungletest/pkg/api/handlers.go) | • [`pkg/idempotency/canonical_test.go`](file:///home/moa-dev/projetos/jungletest/pkg/idempotency/canonical_test.go)<br>• [`test/idempotency_integration_test.go`](file:///home/moa-dev/projetos/jungletest/test/idempotency_integration_test.go)<br>• [`test/auth_oidc_integration_test.go`](file:///home/moa-dev/projetos/jungletest/test/auth_oidc_integration_test.go) |
| **Mensageria e Recuperação** | • Transactional Outbox com disputa distribuída via `SKIP LOCKED`<br>• SQS FIFO com Inbox atômica e eliminação de duplicações pós-commit<br>• Resolução de referências fora de ordem em dois momentos (Dual-Moment)<br>• Graceful shutdown cooperativo sem perda de mensagens em trânsito<br>• Worker de recuperação para transações retidas em `PENDING` | • [`pkg/service/outbox_relayer.go`](file:///home/moa-dev/projetos/jungletest/pkg/service/outbox_relayer.go)<br>• [`pkg/messaging/sqs_consumer.go`](file:///home/moa-dev/projetos/jungletest/pkg/messaging/sqs_consumer.go)<br>• [`pkg/service/pending_ref_resolver.go`](file:///home/moa-dev/projetos/jungletest/pkg/service/pending_ref_resolver.go)<br>• [`pkg/service/stale_tx_recovery_worker.go`](file:///home/moa-dev/projetos/jungletest/pkg/service/stale_tx_recovery_worker.go) | • [`test/outbox_integration_test.go`](file:///home/moa-dev/projetos/jungletest/test/outbox_integration_test.go)<br>• [`test/sqs_inbox_integration_test.go`](file:///home/moa-dev/projetos/jungletest/test/sqs_inbox_integration_test.go)<br>• [`test/pending_reference_integration_test.go`](file:///home/moa-dev/projetos/jungletest/test/pending_reference_integration_test.go)<br>• [`test/app_restart_resilience_test.go`](file:///home/moa-dev/projetos/jungletest/test/app_restart_resilience_test.go) |
| **Modelagem e Arquitetura** | • Arquitetura modular desacoplada gerenciada com Uber Fx<br>• Separação estrita de camadas (Domain, Infrastructure, Application, Transport)<br>• Especificação arquitetural formal no documento `ARCHITECTURE.md` | • [`pkg/service/module.go`](file:///home/moa-dev/projetos/jungletest/pkg/service/module.go)<br>• [`pkg/api/module.go`](file:///home/moa-dev/projetos/jungletest/pkg/api/module.go)<br>• [`ARCHITECTURE.md`](file:///home/moa-dev/projetos/jungletest/ARCHITECTURE.md) | • [`test/fx_lifecycle_test.go`](file:///home/moa-dev/projetos/jungletest/test/fx_lifecycle_test.go) |
| **Testes Automatizados** | • 100% da suíte construída sem mocks de banco ou mensageria<br>• Execução contra instâncias reais de PostgreSQL, AWS SQS e Keycloak<br>• Execução com detector de concorrência (`-race`) sem race conditions | • Diretório [`test/`](file:///home/moa-dev/projetos/jungletest/test/) | • Executável via `make test-race` (suíte completa de ponta a ponta com aprovação integral) |
| **Observabilidade e Telemetria** | • Métricas Prometheus expostas em formato OpenMetrics (`/metrics`)<br>• Probes de Liveness e Readiness com validação profunda de conectividade<br>• Logs estruturados em formato JSON nativo (`log/slog`) | • [`pkg/metrics/metrics.go`](file:///home/moa-dev/projetos/jungletest/pkg/metrics/metrics.go)<br>• [`pkg/api/handlers.go`](file:///home/moa-dev/projetos/jungletest/pkg/api/handlers.go) | • [`test/reconciliation_observability_test.go`](file:///home/moa-dev/projetos/jungletest/test/reconciliation_observability_test.go) |
| **Documentação Operacional** | • Guia operacional detalhado com comandos reproduzíveis de ponta a ponta<br>• Exemplos validados de cURL com autenticação e tratamento de JSON<br>• Procedimentos de troubleshooting, migrações e ciclo de vida | • [`README.md`](file:///home/moa-dev/projetos/jungletest/README.md)<br>• [`.env.example`](file:///home/moa-dev/projetos/jungletest/.env.example)<br>• [`Makefile`](file:///home/moa-dev/projetos/jungletest/Makefile) | • Validação operacional e execução via Docker Compose em ambiente limpo |

---

## Pré-requisitos do Ambiente

Para executar ou desenvolver o projeto localmente:
- **Go**: Versão `1.24` ou superior (testado e compilado com Go `1.25.4`).
- **Docker**: Versão `24.0+` com suporte a BuildKit.
- **Docker Compose**: Versão `2.20+` (`docker compose`).
- **Utilitários recomendados**: `make`, `curl`, `jq`.

---

## Guia de Inicialização Rápida (Checkout Limpo)

O projeto é 100% reproduzível a partir de um clone limpo do repositório.

### 1. Clonar e subir o ambiente completo

```bash
# Clone o repositório
git clone https://github.com/moaaskt/jungle-test-challenge.git
cd jungle-test-challenge

# Suba todos os serviços (App, PostgreSQL, LocalStack SQS e Keycloak)
make up
# ou alternativamente:
docker compose up --build -d
```

### 2. Verificar a prontidão dos serviços

Aguarde alguns segundos para que os healthchecks coordenados alcancem o estado `healthy`:

```bash
# Inspecione o status dos contêineres
docker compose ps
```

Saída esperada:
```text
NAME                     IMAGE                               COMMAND                  SERVICE      STATUS
jungletest-app-1         jungletest-app                      "/app/server"            app          running (healthy)
jungletest-db-1          postgres:15-alpine                  "docker-entrypoint.s…"   db           running (healthy)
jungletest-keycloak-1    quay.io/keycloak/keycloak:24.0.5    "/opt/keycloak/bin/k…"   keycloak     running (healthy)
jungletest-localstack-1  localstack/localstack:3.8           "docker-entrypoint.sh"   localstack   running (healthy)
```

Verifique a prontidão profunda da API via HTTP:
```bash
curl -s http://localhost:8080/health/ready | jq .
```
Resposta esperada:
```json
{
  "database": "UP",
  "sqs": "UP",
  "status": "READY"
}
```

### 3. Encerrar o ambiente

```bash
# Para desligar e remover os contêineres e volumes de dados efêmeros:
make down
# ou:
docker compose down -v
```

---

## Variáveis de Ambiente

O arquivo [`.env.example`](file:///home/moa-dev/projetos/jungletest/.env.example) na raiz do projeto documenta exaustivamente todas as opções de configuração:

| Variável | Padrão Local / Host | Padrão Docker Compose | Descrição |
|---|---|---|---|
| `PORT` | `8080` | `8080` | Porta TCP do servidor HTTP da aplicação. |
| `DATABASE_URL` | `postgres://jungle:password@localhost:5432/jungle_test?sslmode=disable` | `postgres://jungle:password@db:5432/jungle_test?sslmode=disable` | DSN de conexão ao PostgreSQL com `pgxpool`. |
| `AWS_ENDPOINT` | `http://localhost:4566` | `http://localstack:4566` | Endpoint do LocalStack (SQS). |
| `AWS_REGION` | `us-east-1` | `us-east-1` | Região AWS para inicialização do SDK v2. |
| `AWS_ACCESS_KEY_ID` | `test` | `test` | Chave de acesso AWS (mock LocalStack). |
| `AWS_SECRET_ACCESS_KEY` | `test` | `test` | Chave secreta AWS (mock LocalStack). |
| `SQS_QUEUE_URL` | `http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-transactions.fifo` | Auto-detectada pelo provisionador | URL da fila SQS FIFO de entrada de apostas. |
| `SQS_OUTBOX_QUEUE_URL` | `http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-events.fifo` | Auto-detectada pelo provisionador | URL da fila SQS FIFO de eventos transacionais da Outbox. |
| `KEYCLOAK_URL` | `http://localhost:8085` | `http://keycloak:8080` | URL base do IdP Keycloak. |
| `KEYCLOAK_REALM` | `jungle` | `jungle` | Nome do Realm configurado no Keycloak. |
| `AUTH_ENABLED` | `true` | `true` | Ativa a validação obrigatória de tokens JWT (padrão em produção). |

---

## Banco de Dados e Evolução de Migrações

O banco de dados utiliza PostgreSQL 15 com migrações versionadas em ordem estrita. Ao subir via Docker Compose, o diretório `./migrations/` é montado automaticamente em `/docker-entrypoint-initdb.d/`.

### Arquivos de Migração
- `000001_init_schema.up.sql`: Cria as tabelas `wallets`, `wallet_ledger_entries`, `wager_transactions`, `idempotency_records`, além do trigger `trg_protect_wallet_ledger_entries` que impede `UPDATE` ou `DELETE` no ledger.
- `000002_add_outbox_leasing.up.sql`: Cria a tabela `outbox_events` com campos de concorrência (`locked_until`, `retry_count`, `next_attempt_at`).
- `000003_add_inbox_metadata.up.sql`: Cria a tabela `inbox_messages` para o padrão Inbox da mensageria SQS.
- `000004_add_pending_ref_worker.up.sql`: Adiciona índices parciais e campos de controle de TTL e tentativas para o worker de resolução de referências (`idx_wager_tx_pending_ref`).

### Execução Manual de Migrações via `psql`
```bash
# Aplicar todas as migrações em ordem:
for f in migrations/*.up.sql; do
  echo "Aplicando $f..."
  docker exec -i jungletest-db-1 psql -U jungle -d jungle_test < "$f"
done

# Reverter migrações em ordem reversa:
for f in $(ls -r migrations/*.down.sql); do
  echo "Revertendo $f..."
  docker exec -i jungletest-db-1 psql -U jungle -d jungle_test < "$f"
done
```

---

## Autenticação, IdP Keycloak e Geração de Tokens JWT

A aplicação utiliza autenticação e autorização OAuth 2.0 / OIDC baseada em tokens JWT assimétricos (`RS256`), assinados pela autoridade certificadora do Keycloak e validados via JWKS (`/protocol/openid-connect/certs`).

### Contas de Serviço e Credenciais de Teste Pré-Configuradas

| Client ID | Client Secret | Papel / Escopo de Permissão |
|---|---|---|
| `internal-service` | `internal-service-secret` | **Administrador Interno**: Permissão exclusiva para criação de carteiras, inspeção do ledger e reconciliação financeira. |
| `provider-a` | `provider-a-secret` | **Provedor A**: Envio de apostas/ganhos para o `providerId: provider-a` e consulta de suas próprias transações. |
| `provider-b` | `provider-b-secret` | **Provedor B**: Envio de apostas/ganhos para o `providerId: provider-b` e consulta de suas próprias transações. |

### Comandos cURL para Obtenção de Tokens (com `jq`)

Copie e cole no terminal para exportar as variáveis com os tokens Bearer:

```bash
# Token do Serviço Interno (internal-service)
INTERNAL_TOKEN=$(curl -s -X POST http://localhost:8085/realms/jungle/protocol/openid-connect/token \
  -d "grant_type=client_credentials" \
  -d "client_id=internal-service" \
  -d "client_secret=internal-service-secret" | jq -r .access_token)

# Token do Provedor A (provider-a)
PROVIDER_A_TOKEN=$(curl -s -X POST http://localhost:8085/realms/jungle/protocol/openid-connect/token \
  -d "grant_type=client_credentials" \
  -d "client_id=provider-a" \
  -d "client_secret=provider-a-secret" | jq -r .access_token)

# Token do Provedor B (provider-b)
PROVIDER_B_TOKEN=$(curl -s -X POST http://localhost:8085/realms/jungle/protocol/openid-connect/token \
  -d "grant_type=client_credentials" \
  -d "client_id=provider-b" \
  -d "client_secret=provider-b-secret" | jq -r .access_token)
```

---

## Guia Prático de Chamadas da API (Exemplos com cURL e jq)

Todos os exemplos abaixo utilizam os tokens exportados na seção anterior e podem ser copiados e executados diretamente no terminal.

### 1. Abertura de Carteira (`POST /wallets`)
> *Requer privilégio do `internal-service`.*

```bash
WALLET_RESP=$(curl -s -X POST http://localhost:8080/wallets \
  -H "Authorization: Bearer $INTERNAL_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "playerId": "player-test-01",
    "currency": "BRL",
    "initialBalance": {
      "amount": "100.00",
      "currency": "BRL"
    }
  }')

echo $WALLET_RESP | jq .
WALLET_ID=$(echo $WALLET_RESP | jq -r .id)
echo "Carteira Criada ID: $WALLET_ID"
```

### 2. Processamento de Aposta (`POST /wagering/transactions`)
> *Operação de aposta (`BET`) de R$ 30,00 enviada pelo `provider-a` com header `Idempotency-Key`.*

```bash
curl -s -X POST http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER_A_TOKEN" \
  -H "Idempotency-Key: aposta-key-1001" \
  -H "Content-Type: application/json" \
  -d "{
    \"walletId\": \"$WALLET_ID\",
    \"playerId\": \"player-test-01\",
    \"roundId\": \"round-slots-01\",
    \"gameId\": \"mega-fire-blaze\",
    \"externalTransactionId\": \"ext-tx-1001\",
    \"providerId\": \"provider-a\",
    \"kind\": \"BET\",
    \"money\": {
      \"amount\": \"30.00\",
      \"currency\": \"BRL\"
    }
  }" | jq .
```
Resposta esperada:
```json
{
  "transactionId": "...",
  "status": "PROCESSED",
  "balance": {
    "amount": "70.00",
    "currency": "BRL"
  },
  "idempotentReplay": false
}
```

### 3. Demonstração de Replay Idempotente
> *Repetindo a exata mesma requisição acima com a mesma `Idempotency-Key`: o sistema reconhece a chamada, devolve `idempotentReplay: true` e preserva o saldo inalterado em R$ 70,00.*

```bash
curl -s -X POST http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER_A_TOKEN" \
  -H "Idempotency-Key: aposta-key-1001" \
  -H "Content-Type: application/json" \
  -d "{
    \"walletId\": \"$WALLET_ID\",
    \"playerId\": \"player-test-01\",
    \"roundId\": \"round-slots-01\",
    \"gameId\": \"mega-fire-blaze\",
    \"externalTransactionId\": \"ext-tx-1001\",
    \"providerId\": \"provider-a\",
    \"kind\": \"BET\",
    \"money\": {
      \"amount\": \"30.00\",
      \"currency\": \"BRL\"
    }
  }" | jq .
```
Resposta esperada:
```json
{
  "transactionId": "...",
  "status": "PROCESSED",
  "balance": {
    "amount": "70.00",
    "currency": "BRL"
  },
  "idempotentReplay": true
}
```

### 4. Demonstração de Rejeição de Negócio (`REJECTED`)
> *Tentativa de efetuar aposta de R$ 999,00 quando o saldo é de apenas R$ 70,00. Retorna `200 OK` com `status: REJECTED` e código de falha de negócio, sem debitar a carteira.*

```bash
curl -s -X POST http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER_A_TOKEN" \
  -H "Idempotency-Key: aposta-key-1002" \
  -H "Content-Type: application/json" \
  -d "{
    \"walletId\": \"$WALLET_ID\",
    \"playerId\": \"player-test-01\",
    \"roundId\": \"round-slots-02\",
    \"gameId\": \"mega-fire-blaze\",
    \"externalTransactionId\": \"ext-tx-1002\",
    \"providerId\": \"provider-a\",
    \"kind\": \"BET\",
    \"money\": {
      \"amount\": \"999.00\",
      \"currency\": \"BRL\"
    }
  }" | jq .
```
Resposta esperada:
```json
{
  "transactionId": "...",
  "status": "REJECTED",
  "balance": {
    "amount": "70.00",
    "currency": "BRL"
  },
  "failureCode": "INSUFFICIENT_FUNDS",
  "idempotentReplay": false
}
```

### 5. Consulta de Saldo da Carteira (`GET /wallets/:walletId`)
```bash
curl -s -X GET http://localhost:8080/wallets/$WALLET_ID \
  -H "Authorization: Bearer $INTERNAL_TOKEN" | jq .
```

### 6. Consulta do Ledger com Cursor Opaco (`GET /wallets/:walletId/ledger`)
```bash
curl -s -X GET "http://localhost:8080/wallets/$WALLET_ID/ledger?limit=10" \
  -H "Authorization: Bearer $INTERNAL_TOKEN" | jq .
```

### 7. Consulta Hierárquica por Provedor e ID Externo (`GET /providers/:pId/...`)
```bash
curl -s -X GET "http://localhost:8080/providers/provider-a/wagering/transactions/ext-tx-1001" \
  -H "Authorization: Bearer $PROVIDER_A_TOKEN" | jq .
```

### 8. Reconciliação Contábil (`POST /wallets/:walletId/reconciliation`)
> *Reconstrói o saldo somando créditos e débitos do ledger imutável sob transação consistente. Garante matematicamente a consistência sem nunca alterar o saldo.*

```bash
curl -s -X POST http://localhost:8080/wallets/$WALLET_ID/reconciliation \
  -H "Authorization: Bearer $INTERNAL_TOKEN" | jq .
```
Resposta esperada:
```json
{
  "walletId": "...",
  "consistent": true,
  "storedBalance": {
    "amount": "70.00",
    "currency": "BRL"
  },
  "calculatedBalance": {
    "amount": "70.00",
    "currency": "BRL"
  },
  "difference": {
    "amount": "0.00",
    "currency": "BRL"
  },
  "checkedEntries": 2
}
```

### 9. Métricas Prometheus (`GET /metrics`)
```bash
curl -s http://localhost:8080/metrics | grep -E "wager_|sqs_|outbox_|reconciliation_"
```

---

## Bateria de Testes e Cenários Extremos

O projeto possui **45 testes automatizados de ponta a ponta** executados contra contêineres reais (sem uso de mocks de banco ou broker).

### Comandos do Makefile

```bash
# Executar todos os testes
make test

# Executar todos os testes com verificação rigorosa de concorrência (-race)
make test-race

# Checagem de formatação de código
make fmt-check

# Análise estática do Go
make vet
```

### Roteiro de Testes de Cenários Extremos (Seção 13 do Edital)

A banca avaliadora pode executar individualmente cada um dos testes que cobrem os cenários mais complexos da especificação:

#### 1. Concorrência Extrema com 3 Processos Independentes (Seção 8 do Edital)
Simula 3 goroutines concorrentes disparando débitos massivos contra a mesma carteira até o esgotamento do saldo, validando ausência de lost updates e integridade matemática:
```bash
go test -v -race -run TestConcurrency_Section8_Spec_ThreeIndependentProcesses ./test
```

#### 2. Resiliência a Restart, Crash e Recuperação de `PENDING` (Seção 13, item 8)
Interrompe a primeira instância com uma transação em estado `PENDING`, inicializa uma nova instância conectada ao mesmo banco e filas, e valida que a transação presa é recuperada para `FAILED` por timeout e o saldo final confere com precisão contábil:
```bash
go test -v -race -run TestAppRestart_PreservesIdempotencyAndRecoversPending ./test
```

#### 3. Ciclo de Vida Uber Fx e Parada Limpa dos Workers (Seção 13)
Inicializa a aplicação Fx completa com os 6 módulos, valida a prontidão HTTP e executa `app.Stop()`, confirmando a liberação ordenada de `SQSConsumer`, `OutboxRelayer`, `PendingRefResolver` e `StaleTxRecoveryWorker` sem deadlocks ou goroutines órfãs:
```bash
go test -v -race -run TestFx_Composition_StartAndStop ./test
```

#### 4. SQS Graceful Shutdown com Liberação Imediata de Visibilidade para 0s (Seção 13, item 6)
Dispara cancelamento do consumidor com mensagens em voo; comprova que o consumidor altera o timeout de visibilidade das mensagens pendentes para 0 segundos (`VisibilityTimeout: 0`) para redistribuição imediata por outras instâncias:
```bash
go test -v -race -run TestSQS_GracefulShutdown_ReleaseVisibility ./test
```

#### 5. Descarte de Replay Pós-Commit no SQS (Seção 13, item 5)
Simula a queda da instância imediatamente após o commit atômico no PostgreSQL mas antes do `sqs.DeleteMessage`; ao reiniciar, a mensagem reentregue pelo broker é detectada na Inbox e imediatamente expurgada da fila sem duplicar o saldo:
```bash
go test -v -race -run TestSQS_Inbox_PostCommitCrash_ReplayDeletion ./test
```

---

## Estrutura de Diretórios

```text
├── cmd/
│   └── server/             # Ponto de entrada do executável da aplicação (main.go com Uber Fx)
├── pkg/
│   ├── api/                # Handlers HTTP, rotas REST, serialização JSON e servidor
│   ├── auth/               # Validação JWKS Keycloak, middleware OIDC e autorização por cliente
│   ├── config/             # Carregamento e validação de variáveis de ambiente
│   ├── database/           # Pool de conexões PostgreSQL (pgxpool) e transações
│   ├── domain/             # Agregados de domínio puro (Wallet, WagerTransaction, Ledger, Outbox)
│   ├── idempotency/        # Geração de hash SHA-256 canônico e chaves de idempotência
│   ├── messaging/          # Consumidor AWS SQS FIFO com Inbox Pattern e provisionador de filas
│   ├── metrics/            # Registro e instrumentação de métricas Prometheus operacionais
│   ├── money/              # Value Object Money imutável em centavos inteiros (int64)
│   ├── repository/         # Persistência PostgreSQL com locks pessimistas e SKIP LOCKED
│   └── service/            # Orquestração de casos de uso e background workers (Relayer, Resolvers)
├── migrations/             # Migrações SQL versionadas (up/down)
├── keycloak/               # Export declarativo do Realm 'jungle' com credenciais pré-configuradas
├── test/                   # Bateria completa de testes de integração ponta a ponta sem mocks
├── Dockerfile              # Multi-stage build seguro, estático e não-root
├── docker-compose.yml      # Orquestração completa de contêineres com healthchecks encadeados
├── Makefile                # Automação de compilação, testes, formatação e execução
├── ARCHITECTURE.md         # Documento arquitetural exaustivo com diagramas C4 e Mermaid
└── README.md               # Este guia operacional completo
```
