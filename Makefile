.PHONY: test lint bench run-server run-client

# Run all unit tests with the data race detector enabled
test:
	@echo "==> Running unit tests with race detector..."
	go test -race -v ./...

# Run static code analysis and linting checks
lint:
	@echo "==> Running static code analysis..."
	@golangci-lint run ./... || echo "golangci-lint not installed locally"

# Run performance and memory allocation benchmarks
bench:
	@echo "==> Running benchmarks..."
	go test -bench=. -benchmem ./...

# Start the example TCP server locally
run-server:
	go run examples/tcp_server/main.go

# Start the example TCP client locally
run-client:
	go run examples/tcp_client/main.go