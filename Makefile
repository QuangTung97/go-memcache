.PHONY: lint test test-race benchmark install-tools coverage

lint:
	$(foreach f,$(shell go fmt ./...),@echo "Forgot to format file: ${f}"; exit 1;)
	go vet ./...
	revive -config revive.toml -formatter friendly ./...

test:
	go test -p 1 -count=1 -covermode=count -coverprofile=coverage.out ./...

test-race:
	go test -p 1 -race -count=1 ./...

benchmark:
	go test -run "^Benchmark" -bench=. ./...

install-tools:
	go install github.com/matryer/moq
	go install github.com/mgechev/revive

coverage:
	go tool cover -func coverage.out | grep ^total
