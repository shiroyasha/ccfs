.PHONY: dev.run

#
# Installs the dependencies and prepares the development environment.
#
dev.setup:
	docker-compose build
	docker-compose run meta_server      bash -c 'cd /app/metadata-server && cargo install --path .'
	docker-compose run chunk_server_001 bash -c 'cd /app/chunk-server && cargo install --path .'
	docker-compose run chunk_server_002 bash -c 'cd /app/chunk-server && cargo install --path .'
	docker-compose run chunk_server_003 bash -c 'cd /app/chunk-server && cargo install --path .'
	docker-compose run cli              bash -c 'cd /app/cli && cargo install --path .'

#
# Spins up a development environment with a metadata server and 3 chunk servers.
#
dev.run:
	docker-compose up

#
# Enters the environment that has a running CLI.
#
dev.cli:
	docker-compose exec cli
