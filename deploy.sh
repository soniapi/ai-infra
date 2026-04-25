#!/bin/bash
gcloud run deploy grpc-schema-service \
  --source . \
  --region us-central1 \
  --project ai-infra-grpc \
  --no-allow-unauthenticated \
  --use-http2 \
  --port=8080
