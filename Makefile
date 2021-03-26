.PHONY: dev.run
PROJECT_DIR = $(shell pwd)

#
# Installs the dependencies and prepares the development environment.
#
dev.setup:
	docker-compose build

#
# Spins up a development environment with a metadata server and 3 chunk servers.
# Skips running cli container.
#
dev.up:
	BOOTSTRAP_SIZE=3 BOOTSTRAP_URL=http://meta_server_001:4001 docker-compose up \
		--scale cli=0

#
# Spins up a single container.
#
dev.run:
	docker-compose up -d $(filter-out $@,$(MAKECMDGOALS))

#
# Stops all containers that match the provided names.
#
dev.stop:
	docker-compose stop $(filter-out $@,$(MAKECMDGOALS))

#
# Stops all containers in the development environment.
#
dev.down:
	docker-compose down

#
# Runs the CLI environment entrypoint and forwards the command arguments to it.
#
dev.cli:
	docker-compose run --rm cli $(filter-out $@,$(MAKECMDGOALS))

test:
	bats tests