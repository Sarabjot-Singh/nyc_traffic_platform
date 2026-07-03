build: # syntax make build
# Builds the Docker image for the project.
	docker compose build

up: # syntax make up
# Starts the Docker containers for the project.
	docker compose up -d

down: # syntax make down
# Stops and removes the Docker containers for the project.
	docker compose down

list_containers: # syntax make list_containers
# Lists all running Docker containers.
	docker ps -a

exec: # syntax make exec
# Executes a command in a running Docker container.
# Usage: make exec CONTAINER=<container_name>
	docker exec -it $(CONTAINER) bash