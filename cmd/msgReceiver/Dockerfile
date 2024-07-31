FROM golang:1.22-alpine AS builder

WORKDIR /usr/local/src

RUN apk --no-cache add bash gcc gettext

# dep
COPY ../../go.mod ../../go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o ./bin/app ./cmd/msgReceiver/main.go

RUN chmod +x ./bin/app

FROM alpine

COPY --from=builder /usr/local/src/bin/app .

COPY ../../src/sql/MessageTable.sql /usr/local/src/db/sql/
COPY ../../.env  /

CMD ["./app"]