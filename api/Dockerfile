FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    golang-go \
    ca-certificates

COPY ./ca.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates

WORKDIR /api

COPY ./cmd/ ./cmd/
COPY ./api/ ./api/

RUN go mod init api
RUN go get github.com/twmb/franz-go
RUN go mod tidy

ENTRYPOINT ["go", "run", "cmd/main.go"]
