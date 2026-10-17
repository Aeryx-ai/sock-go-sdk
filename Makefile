.DEFAULT_GOAL := build
.PHONY: build test install redeploy check lint fmt vet cross fuzz

build:
	go build ./...

test:
	go test -race ./...

install:
	@echo "sock-go-sdk is a library; nothing to install"

redeploy:
	@echo "sock-go-sdk is a library; nothing to deploy"

lint:
	golangci-lint run

fmt:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	go vet ./...

# cross vets every platform the SDK's consumers ship to
cross:
	@for os in darwin linux windows; do GOOS=$$os go vet ./... || exit 1; done

# fuzz runs each fuzz target briefly; FUZZTIME=1m for a longer pass
FUZZTIME ?= 10s
fuzz:
	@for f in $$(go test -list '^Fuzz' . | grep '^Fuzz'); do go test -run '^$$' -fuzz "^$$f$$" -fuzztime $(FUZZTIME) . || exit 1; done

check: fmt vet cross lint test
