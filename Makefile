.PHONY: all build test test-it test-all bench lint vet vet-tags fmt fmt-check pre-commit tidy clean help

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

# -race needs the CGO/TSan runtime; Windows toolchains commonly run
# with CGO_ENABLED=0, where -race fails to build. Mirror the
# pre-commit hook's conditional: race on when CGO is on, plain
# otherwise (CI's Linux runners remain the authoritative -race gate).
RACE := $(shell [ "$$($(GO) env CGO_ENABLED 2>/dev/null)" = "1" ] && echo -race)

all: lint test build ## Run lint, tests, and build

build: ## Build the sluice binary into ./bin
	@mkdir -p $(BUILD_DIR)
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BUILD_DIR)/$(BINARY) ./cmd/sluice

test: ## Run unit tests (Layer 1) — fast, no databases
	$(GO) test $(RACE) -count=1 ./...

test-it: ## Run unit + integration tests (Layers 1-2) — requires Docker for testcontainers
	$(GO) test $(RACE) -count=1 -tags=integration ./...

test-all: ## Today: same as test-it (the sqllogic/property tags match no files yet — docs/testing.md Layers 3-4 are design targets)
	$(GO) test $(RACE) -count=1 -tags='integration sqllogic property' ./...

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

vet-tags: ## Type-check every build-tag combination in use (incl. tagged test files)
	sh scripts/vet-tags.sh

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

coverage-guards: ## Run CI Lint's test-coverage guards (shard + -run-filter)
	sh scripts/check-shard-coverage.sh
	sh scripts/check-run-filter-coverage.sh

pre-commit: fmt-check vet vet-tags coverage-guards lint test ## Run the full local gate (mirrors CI: format, vet, tags-vet, coverage guards, lint, fast tests)
	@echo "OK — ready to commit."

tidy: ## go mod tidy
	$(GO) mod tidy

clean: ## Remove build artefacts
	rm -rf $(BUILD_DIR)

help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
