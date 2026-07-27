#!/bin/sh
# Creates the SQS queue in LocalStack on container start. VisibilityTimeout
# matches VISIBILITY_TIMEOUT_SECONDS in app/worker/sqs_consumer.py so local
# behavior matches what dev/prod would see against real SQS.
set -eu

aws --endpoint-url="${AWS_ENDPOINT_URL:-http://localstack:4566}" sqs create-queue \
  --queue-name jobs-queue \
  --attributes VisibilityTimeout=3600

echo "jobs-queue created in LocalStack."
