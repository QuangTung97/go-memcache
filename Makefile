.PHONY: lint test install-tools

lint:
	$(foreach f,$(shell go fmt ./...),@echo "Forgot to format file: ${f}"; exit 1;)
	go vet ./...
	revive -config revive.toml -formatter friendly ./...

test:
	go test -v ./...

install-tools:
	go install github.com/matryer/moq
	go install github.com/mgechev/revive
