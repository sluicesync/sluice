.PHONY: all build test test-it test-all bench lint vet fmt fmt-check pre-commit tidy clean help

GO         ?= go
PKG        := sluicesync.dev/sluice
BINARY     := sluice
BUILD_DIR  := bin

VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE    := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w \
	-X main.version=$(VERSION) \
	-X main.commit=$(COMMIT) \
	-X main.date=$(DATE)

all: lint test build ## Run lint, tests, and build

build: ## Build the sluice binary into ./bin
	@mkdir -p $(BUILD_DIR)
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BUILD_DIR)/$(BINARY) ./cmd/sluice

test: ## Run unit tests (Layer 1) — fast, no databases
	$(GO) test -race -count=1 ./...

test-it: ## Run unit + integration tests (Layers 1-2) — requires Docker for testcontainers
	$(GO) test -race -count=1 -tags=integration ./...

test-all: ## Run unit + integration + sqllogic + property tests (Layers 1-4)
	$(GO) test -race -count=1 -tags='integration sqllogic property' ./...

bench: ## Run Go benchmarks
	$(GO) test -bench=. -run='^$$' -count=3 ./...

lint: vet ## Run go vet plus golangci-lint
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not found. Install: https://golangci-lint.run/welcome/install/"; \
		exit 1; \
	}
	golangci-lint run ./...

vet: ## Run go vet
	$(GO) vet ./...

fmt: ## Apply gofumpt (preferred) or gofmt to all .go files
	@if command -v gofumpt >/dev/null 2>&1; then \
		echo "gofumpt -l -w ."; \
		gofumpt -l -w .; \
	else \
		echo "gofumpt not installed; falling back to go fmt"; \
		echo "Install: go install mvdan.cc/gofumpt@latest"; \
		$(GO) fmt ./...; \
	fi

fmt-check: ## Verify formatting without writing changes (exits non-zero if any file would change)
	@if ! command -v gofumpt >/dev/null 2>&1; then \
		echo "gofumpt not installed. Install: go install mvdan.cc/gofumpt@latest"; \
		exit 1; \
	fi
	@out=$$(gofumpt -l .); \
	if [ -n "$$out" ]; then \
		echo "gofumpt would reformat the following files:"; \
		echo "$$out"; \
		echo ""; \
		echo "Run 'make fmt' to apply."; \
		exit 1; \
	fi

pre-commit: fmt-check vet test ## Run the pre-commit suite locally (formatting, vet, fast tests)
	@echo "OK — ready to commit."

tidy: ## go mod tidy
	$(GO) mod tidy

clean: ## Remove build artefacts
	rm -rf $(BUILD_DIR)

help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
