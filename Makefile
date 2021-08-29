PROJECT_NAME := "data-pipe"
PKG := "github.com/weak-head/$(PROJECT_NAME)"
PKG_LIST := $(shell go list ${PKG}/... | grep -v /vendor/)
GO_FILES := $(shell find . -name '*.go' | grep -v /vendor/ | grep -v _test.go)

# Docker image
IMAGE_ID ?= ${PKG}
IMAGE_TAG ?= 0.0.1

.PHONY: all dep compile build clean test coverage coverhtml
.PHONY: lint race msan install uninstall docker

dep: ## Get the dependencies
	@go get -v -d ./...
	@go get -d golang.org/x/lint/golint

compile: ## Generate protobuf APIs
	./scripts/compile.sh

lint: ## Lint the files
	@golint -set_exit_status ${PKG_LIST}

test: ## Run unittests
	@go test -short ${PKG_LIST}

race: dep ## Run data race detector
	@go test -race -short ${PKG_LIST}

msan: dep ## Run memory sanitizer
	@go test -msan -short ${PKG_LIST}

coverage: ## Generate global code coverage report
	./scripts/coverage.sh;

coverhtml: ## Generate global code coverage report in HTML
	./scripts/coverage.sh html;

docker: compile ## Build docker image
	@docker build -t $(IMAGE_ID):$(IMAGE_TAG) .

build: dep compile ## Build the binary file
	@go build -v ./cmd/"${PROJECT_NAME}"

clean: ## Remove previous build
	@rm -f $(PROJECT_NAME)

install: docker ## Install helm chart
	@kind load docker-image $(IMAGE_ID):$(IMAGE_TAG)
	@helm install ${PROJECT_NAME} ./deploy/${PROJECT_NAME}/

uninstall: ## Uninstall helm chart
	@helm uninstall ${PROJECT_NAME}

help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
