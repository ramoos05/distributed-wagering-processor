#!/bin/sh
set -eu
dead=$(awslocal sqs create-queue --queue-name wager-transactions-dlq.fifo --attributes FifoQueue=true --query QueueUrl --output text)
arn=$(awslocal sqs get-queue-attributes --queue-url "$dead" --attribute-names QueueArn --query Attributes.QueueArn --output text)
printf '{"FifoQueue":"true","RedrivePolicy":"{\"deadLetterTargetArn\":\"%s\",\"maxReceiveCount\":5}"}' "$arn" > /tmp/wager-queue.json
awslocal sqs create-queue --queue-name wager-transactions.fifo --attributes file:///tmp/wager-queue.json
awslocal sqs create-queue --queue-name wager-events.fifo --attributes FifoQueue=true
