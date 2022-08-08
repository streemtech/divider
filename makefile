SHELL:=/bin/bash

.PHONY: docker compose import export deploy main


go-test:
	go test ./...
	shadow -strict $$(go list ./... | grep -v "api$$")
	staticcheck $$(go list ./... | grep -v "api$$")
	golangci-lint run
	
newman-tests:
	# newman run ./end-to-end/end-to-end.postman_collection.json -e ./end-to-end/end-to-end.postman_environment.json

test: go-test newman-tests


