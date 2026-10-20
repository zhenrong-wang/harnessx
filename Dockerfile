# syntax=docker/dockerfile:1.7
FROM golang:1.22-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG PACKAGE
RUN test -n "$PACKAGE" && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/harnessx "$PACKAGE"

FROM alpine:3.20
RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 harnessx \
    && adduser -S -D -H -u 10001 -G harnessx harnessx \
    && mkdir -p /var/lib/harnessx/volumes \
    && chown -R harnessx:harnessx /var/lib/harnessx
WORKDIR /app
COPY --from=build /out/harnessx /app/harnessx
COPY --from=build /src/db/migrations /app/db/migrations
USER harnessx
ENTRYPOINT ["/app/harnessx"]
