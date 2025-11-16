FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    golang-go \
    ca-certificates

COPY ./ca.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates

WORKDIR /inference

RUN go mod init inference
RUN go get github.com/twmb/franz-go

COPY ./cmd/ ./cmd/
COPY ./inference/ ./inference/

RUN go mod tidy

ENTRYPOINT ["go", "run", "cmd/main.go"]
