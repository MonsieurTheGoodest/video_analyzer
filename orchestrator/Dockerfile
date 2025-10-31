FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    golang-go\
    ca-certificates

COPY ./ca.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates

WORKDIR /orchestrator

COPY ./cmd/ ./cmd/
COPY ./db/ ./db/
COPY ./orchestrator/ ./orchestrator/
COPY ./worker/ ./worker/

RUN go mod init orchestrator
RUN go get github.com/twmb/franz-go
RUN go get github.com/jackc/pgx/v5/pgxpool
RUN go mod tidy

ENTRYPOINT ["go", "run", "cmd/main.go"]
