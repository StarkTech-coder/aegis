.PHONY: test lint bench run-server run-client

# Tüm testleri veri yarışı (data race) dedektörü ile çalıştırır
test:
	@echo "==> Running unit tests with race detector..."
	go test -race -v ./...

# Kod kalitesini ve standartları kontrol eder
lint:
	@echo "==> Running static code analysis..."
	@golangci-lint run ./... || echo "golangci-lint not installed locally"

# İleride yazacağımız performans testleri için hazır altyapı
bench:
	@echo "==> Running benchmarks..."
	go test -bench=. -benchmem ./...

# Hazırlayacağımız örnek sunucuyu lokalde tek komutla başlatır
run-server:
	go run examples/tcp_server/main.go

# Hazırlayacağımız örnek istemciyi lokalde tek komutla başlatır
run-client:
	go run examples/tcp_client/main.go