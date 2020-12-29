SHELL := /bin/bash
PROJECT_DIR = $(shell pwd)

start-ms:
	ROCKET_PORT=8080 cargo run --manifest-path ./metadata-server/Cargo.toml

start-cs:
	METADATA_URL=http://localhost:8080 cargo run --manifest-path ./chunk-server/Cargo.toml

ccfs-cli:
	cargo run --manifest-path $(PROJECT_DIR)/cli/Cargo.toml -- -c cli/cli_config.yaml $(filter-out $@,$(MAKECMDGOALS))
