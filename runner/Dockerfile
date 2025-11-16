FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    golang-go \
    ffmpeg \
    ca-certificates

COPY ./ca.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates

WORKDIR /runner

RUN go mod init runner
RUN go get github.com/twmb/franz-go
RUN go get github.com/AlexEidt/Vidio
RUN mkdir video

COPY ./cmd/ ./cmd/
COPY ./runner/ ./runner/

RUN go mod tidy

ENTRYPOINT ["go", "run", "cmd/main.go"]
