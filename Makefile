.PHONY: test lint

test:
	go test -race ./...

lint:
	# https://golangci-lint.run/usage/install/#local-installation
	docker run --rm -v `pwd`:/app -w /app mirror.gcr.io/golangci/golangci-lint:v2.15.0 golangci-lint run -v
