###############################################################################
# ChangeDataCapture Project Makefile
# 
# This Makefile provides commands to control the CDC environment and access
# the various admin UIs available in the project.
###############################################################################

.PHONY: help up down ui minio-ui debezium-ui control-center-ui

# ANSI color codes for better terminal output
BLUE=\033[0;34m
GREEN=\033[0;32m
NC=\033[0m # No Color

help:
	@echo "$(BLUE)Available commands:$(NC)"
	@echo "  make up               - Start all containers with docker-compose"
	@echo "  make down             - Stop and remove all containers"
	@echo "  make ui               - Display URLs for all available UIs"
	@echo "  make minio-ui         - Display MinIO UI URL"
	@echo "  make debezium-ui      - Display Debezium UI URL"
	@echo "  make control-center-ui - Display Control Center UI URL"

up:
	@echo "$(BLUE)Starting all containers...$(NC)"
	@docker-compose up -d
	@echo "$(GREEN)All containers started successfully$(NC)"

down:
	@echo "$(BLUE)Stopping all containers...$(NC)"
	@docker-compose down
	@echo "$(GREEN)All containers stopped successfully$(NC)"

minio-ui:
	@echo "$(BLUE)MinIO UI:$(NC) http://localhost:9000"

debezium-ui:
	@echo "$(BLUE)Debezium Control Center:$(NC) http://localhost:8085"

control-center-ui:
	@echo "$(BLUE)Control Center:$(NC) http://localhost:9021"

ui: minio-ui debezium-ui control-center-ui
	@echo "$(GREEN)All administrative UIs are now accessible via the URLs above$(NC)"