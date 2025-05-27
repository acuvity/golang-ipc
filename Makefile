GOPRIVATE=go.acuvity.ai,github.com/acuvity

GIT_SHA=$(shell git rev-parse --short HEAD)
GIT_BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
GIT_TAG=$(shell git describe --tags --abbrev=0 --match='v[0-9]*.[0-9]*.[0-9]*' 2> /dev/null | sed 's/^.//')
BUILD_DATE=$(shell date)

ifeq ($(ARCH), x86)
	GOOS = linux
	GOARCH = amd64
else ifeq ($(ARCH), arm)
	GOOS = linux
	GOARCH = arm64
else ifeq ($(ARCH), native)
	# let the os decide
endif

export GOOS GOARCH GOPRIVATE

.PHONY: default
default: lint vuln test

## Env
.PHONY: install-tools
install-tools:
	@echo "--> install-tools ..."
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	go install golang.org/x/vuln/cmd/govulncheck@latest
	go install github.com/securego/gosec/v2/cmd/gosec@latest

## Tests
.PHONY: lint
lint:
	@echo "--> lint ..."
	@echo "--> GOOS: $(GOOS)"
	@echo "--> GOARCH: $(GOARCH)"
	@echo "--> golangci-lint: $(shell golangci-lint --version)"
	@echo "--> go: $(shell go version)"
	golangci-lint run \
		--timeout=5m \
		--disable=govet  \
		--disable=staticcheck \
		--disable=revive \
		--enable=errcheck \
		--enable=ineffassign \
		--enable=unused \
		--enable=unconvert \
		--enable=misspell \
		--enable=prealloc \
		--enable=nakedret \
		--enable=unparam \
		--enable=nilerr \
		--enable=bodyclose \
		--enable=errorlint \
		./...

.PHONY: test
test:
	@echo "--> test ..."
	@mkdir -p dist
	go test -vet=off ./... -race -cover -covermode=atomic -coverprofile=unit_coverage.out

.PHONY: sec
sec:
	@echo "--> sec ..."
	gosec -exclude=G103,G115,G304 -quiet ./...

vuln:
	@echo "--> vulncheck ..."
	govulncheck -show verbose ./...

.PHONY: init
init: install-tools
	@echo "--> init ..."
