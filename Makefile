SHELL := /bin/bash
PROJECT_DIR = $(shell pwd)

start-ms:
	ROCKET_PORT=8080 cargo run --release --manifest-path ./metadata-server/Cargo.toml

start-cs:
	METADATA_URL=http://localhost:8080 cargo run --release --manifest-path ./chunk-server/Cargo.toml

ccfs-cli:
	cargo run --release --manifest-path ./cli/Cargo.toml $(filter-out $@,$(MAKECMDGOALS))
