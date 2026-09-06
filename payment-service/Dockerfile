FROM golang:1.25.6-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o payment-service ./cmd/server

FROM alpine:3.20

WORKDIR /app

COPY --from=builder /app/payment-service .

EXPOSE 8082

ENTRYPOINT ["./payment-service"]