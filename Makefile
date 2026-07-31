.DEFAULT_GOAL := image
IMAGE_NAME := voice-ingestion-worker

.PHONY: image run clean

image:
	docker build -t $(IMAGE_NAME) .

run: image
	docker run --rm -p 8000:8000 $(IMAGE_NAME)

clean:
	docker rmi -f $(IMAGE_NAME)
