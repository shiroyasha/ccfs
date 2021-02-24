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
dev.run:
	./rebuild-dev.sh
	docker-compose up --scale cli=0

#
# Runs the CLI environment entrypoint and forwards the command arguments to it.
#
dev.cli:
	./rebuild-dev.sh
	docker-compose run cli $(filter-out $@,$(MAKECMDGOALS))

test:
	bats tests