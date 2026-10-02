# Build targets used by `sam build` (BuildMethod: makefile). Lambda runs the
# Go binaries as "bootstrap" on the provided.al2023 runtime, arm64 (Graviton).
GOBUILD = GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -trimpath -ldflags="-s -w"

build-OrderApiFunction:
	$(GOBUILD) -o $(ARTIFACTS_DIR)/bootstrap ./cmd/lambda-api

build-ActivateFunction:
	$(GOBUILD) -o $(ARTIFACTS_DIR)/bootstrap ./cmd/lambda-activate
