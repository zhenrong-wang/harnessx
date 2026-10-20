SHELL := /usr/bin/env bash

.PHONY: proto-tools proto-generate fmt test build compose-validate check

proto-tools:
	GOBIN="$${HOME}/go/bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5
	GOBIN="$${HOME}/go/bin" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

proto-generate:
	scripts/generate-proto.sh

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './gen/*')

test:
	go test ./...

build:
	go build ./...

compose-validate:
	docker compose -f deployments/docker-compose/compose.yaml config --quiet

check: proto-generate fmt test build compose-validate
