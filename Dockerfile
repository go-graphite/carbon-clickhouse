FROM golang:alpine as builder

WORKDIR /go/src/github.com/lomik/carbon-clickhouse

COPY . .

RUN apk --no-cache add make git
RUN make submodules

RUN make

FROM alpine:latest

RUN apk --no-cache add ca-certificates
WORKDIR /

COPY --from=builder /go/src/github.com/lomik/carbon-clickhouse/carbon-clickhouse ./usr/bin/carbon-clickhouse

CMD ["carbon-clickhouse"]

