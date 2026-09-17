.PHONY: all build test test-race vet fmt fmt-check infra-up up down logs clean

# Alvo padrão: formatação, linting e testes
all: fmt-check vet test

# Compilação local do binário
build:
	@mkdir -p bin
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/server ./cmd/server

# Testes automatizados unitários e de integração
test:
	go test ./...

# Testes automatizados com detector de concorrência (-race)
test-race:
	go test -v -race ./...

# Análise estática de código com go vet
vet:
	go vet ./...

# Formatação do código com gofmt
fmt:
	gofmt -s -w .

# Checagem de formatação sem modificar arquivos
fmt-check:
	@test -z "$$(gofmt -s -l .)" || (echo "Arquivos não formatados com gofmt:" && gofmt -s -l . && exit 1)

# Sobe apenas a infraestrutura (PostgreSQL, LocalStack, Keycloak) para testes no host
infra-up:
	docker compose up -d db localstack keycloak

# Sobe a stack completa incluindo a aplicação compilada via Docker Compose
up:
	docker compose up --build -d

# Encerra todos os containers e remove volumes
down:
	docker compose down -v

# Acompanha logs do serviço da aplicação
logs:
	docker compose logs -f app

# Limpeza de binários gerados
clean:
	rm -rf bin/
