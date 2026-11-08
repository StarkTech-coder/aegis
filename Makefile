.PHONY: test lint bench run-tcp

# Run all unit tests with the race detector
test:
	@echo "==> Running unit tests with race detector..."
	go test -race -v ./...

# Run static code analysis and linting
lint:
	@echo "==> Checking golangci-lint..."
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "ERROR: golangci-lint is not installed."; \
		echo "Install: https://golangci-lint.run/welcome/install/"; \
		exit 1; \
	}
	@echo "==> Running static code analysis..."
	golangci-lint run ./...

# Run performance and memory allocation benchmarks
bench:
	@echo "==> Running benchmarks..."
	go test -bench=. -benchmem ./...

# Run the TCP example
run-tcp:
	@echo "==> Running TCP example..."
	go run ./examples/tcp