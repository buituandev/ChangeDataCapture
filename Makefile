
.PHONY: up down minio-ui debezium-ui control-center-ui ui

up:
	docker-compose up -d

down:
	docker-compose down

minio-ui:
	@echo "MinIO UI is available at: http://localhost:9000"
	@echo "Please open the above URL in your browser"

debezium-ui:
	@echo "Debezium Control Center is available at: http://localhost:8085"
	@echo "Please open the above URL in your browser"

control-center-ui:
	@echo "Control Center is available at: http://localhost:9021"
	@echo "Please open the above URL in your browser"

ui: minio-ui debezium-ui control-center-ui
	@echo "All UIs are now running."