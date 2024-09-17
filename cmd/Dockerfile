FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN cd cmd && go build -o main .

#WORKDIR /app/workers
#RUN go build -o worker .

FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/cmd .
#COPY --from=builder /app/workers/worker .

VOLUME /root/logs

EXPOSE 8088

# Start both the main application and the workers
ENTRYPOINT ["sh", "-c", "./main"]