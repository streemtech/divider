SHELL:=/bin/bash

.PHONY: docker compose import export deploy main


go-test:
	go test ./...
	go run golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow -strict $$(go list ./... | grep -v "api$$")
	go run honnef.co/go/tools/cmd/staticcheck@latest $$(go list ./... | grep -v "api$$")
	golangci-lint run
	
newman-tests:
	# newman run ./end-to-end/end-to-end.postman_collection.json -e ./end-to-end/end-to-end.postman_environment.json

test: go-test newman-tests


