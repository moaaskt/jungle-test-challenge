# Stage 1: Build estático otimizado
FROM golang:alpine AS builder

WORKDIR /build

# Instala dependências de build e certificados CA
RUN apk add --no-cache ca-certificates tzdata

# Cache de dependências de módulos Go
COPY go.mod go.sum ./
RUN go mod download

# Copia o código-fonte da aplicação
COPY . .

# Compilação do binário estático sem CGO com redução de tamanho (-s -w)
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -extldflags '-static'" \
    -o /build/bin/server ./cmd/server

# Stage 2: Imagem final mínima e segura para produção
FROM alpine:3.20

# Criação de usuário e grupo não-privilegiados (appuser:10001)
RUN addgroup -g 10001 appgroup && \
    adduser -u 10001 -G appgroup -D -s /sbin/nologin -h /app appuser

# Instalação de certificados CA atualizados e tzdata
RUN apk --no-cache add ca-certificates tzdata curl

WORKDIR /app

# Cópia do binário a partir do stage builder
COPY --from=builder /build/bin/server /app/server
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Garantia de permissões estritas
RUN chown -R appuser:appgroup /app

# Execução estrita sob usuário sem privilégios de root
USER appuser:appgroup

# Exposição da porta padrão da API
EXPOSE 8080

ENTRYPOINT ["/app/server"]
