# -------- builder --------
FROM golang:1.25-alpine AS builder

WORKDIR /app
ENV CGO_ENABLED=0 GOOS=linux

# системные зависимости
RUN apk add --no-cache git ca-certificates

# кеш модулей и сборки
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

# копируем исходники
COPY . .

# сборка только main-пакета (предположим, что main в корне)
# -trimpath, -s -w для меньшего бинарника
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -buildvcs=false -trimpath -ldflags="-s -w" -o /out/app ./cmd/api

# -------- runtime --------
FROM alpine:3.20

# неблокирующий юзер + сертификаты
RUN adduser -D -H -u 10001 appuser && \
    apk --no-cache add ca-certificates

WORKDIR /home/app
COPY --from=builder /out/app /usr/local/bin/app

EXPOSE 8080
USER appuser

HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8080/health >/dev/null 2>&1 || exit 1

CMD ["app"]
