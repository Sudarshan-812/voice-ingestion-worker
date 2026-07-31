.DEFAULT_GOAL := image

.PHONY: image run

image:
	docker build -t voice-ingestion-worker .

run:
	docker run -p 8000:8000 voice-ingestion-worker
