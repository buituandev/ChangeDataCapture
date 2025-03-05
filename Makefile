# Makefile for MinIO, Debezium, and Control Center project

.PHONY: up down minio-ui debezium-ui control-center-ui ui

# Start the services defined in docker-compose.yml
up:
	docker-compose up -d

# Stop the services
down:
	docker-compose down

# Run the UI for MinIO
minio-ui:
	@echo "MinIO UI is available at: http://localhost:9000"
	@echo "Please open the above URL in your browser"

# Run the UI for Debezium Control Center
debezium-ui:
	@echo "Debezium Control Center is available at: http://localhost:8085"
	@echo "Please open the above URL in your browser"

# Run the UI for Control Center
control-center-ui:
	@echo "Control Center is available at: http://localhost:9021"
	@echo "Please open the above URL in your browser"

# Combined command to run all UIs
ui: minio-ui debezium-ui control-center-ui
	@echo "All UIs are now running."