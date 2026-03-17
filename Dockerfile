FROM golang:1.26.1 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download -x

COPY ./cmd ./cmd

RUN CGO_ENABLED=0 go build -v ./cmd/socat

FROM scratch

COPY --from=builder /app/socat /socat

ENTRYPOINT ["/socat"]
