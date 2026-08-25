# our base image
FROM golang:1.25.6-alpine AS builder

# setting up a working directory
WORKDIR /app

# Docker is known for caching layers, so the dependencies only re-download when go.mod/go.sum change, not every time we edit the source
COPY go.mod go.sum ./

RUN go mod download

COPY . .

# this will create a binary of the name inventory-service with zero C dependencies
RUN CGO_ENABLED=0 GOOS=linux go build -o inventory-service ./cmd/server

# now we create a small container that can run the binary we are creating right above
FROM alpine:3.20

# create a working directory for our container
WORKDIR /app

# copy the details from our builder
COPY --from=builder /app/inventory-service .

EXPOSE 8080

ENTRYPOINT ["./inventory-service"]