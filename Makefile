# Makefile for Docker Compose

# Default target
.DEFAULT_GOAL := help

# Docker Compose file (adjust if your file is named differently)
DOCKER_COMPOSE_FILE := docker-compose.yml
POSTGRES_CONTAINER := postgres
POSTGRES_USER := postgres

# Targets
.PHONY: up down restart logs enter-postgres enter-shell help

# Start the Docker Compose services
up: ## Bring up the Docker Compose services. Add -d to run in detached mode (background)
	@echo "Starting services..."
	@docker-compose -f $(DOCKER_COMPOSE_FILE) up

# Stop the Docker Compose services
down: ## Stop and remove the Docker Compose services
	@echo "Stopping services..."
	@docker-compose -f $(DOCKER_COMPOSE_FILE) down

# Restart the Docker Compose services
restart: ## Restart the Docker Compose services
	@echo "Restarting services..."
	@make down
	@make up

# View logs for the Docker Compose services
logs: ## View logs for Docker Compose services
	@echo "Showing logs..."
	@docker-compose -f $(DOCKER_COMPOSE_FILE) logs -f

# Enter PostgreSQL psql prompt
enter-postgres: ## Access the PostgreSQL database inside the container
	@echo "Entering PostgreSQL..."
	@docker exec -it $(POSTGRES_CONTAINER) psql -U $(POSTGRES_USER)

# Enter the container's shell
enter-shell: ## Access the container's shell
	@echo "Entering the container shell..."
	@docker exec -it $(POSTGRES_CONTAINER) bash

# Display help message
help: ## Display this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'