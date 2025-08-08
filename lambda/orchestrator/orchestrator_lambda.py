import json
import logging
import os
from datetime import datetime, timezone
import boto3

# import the scripts
from scripts.fetch_data_from_api import fetch_and_store_population_data
from scripts.publish_open_dataset import main as publish_main

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#set up sqs
sqs = boto3.client("sqs")


def send_sqs_notification(queue_url: str, payload: dict) -> None:
    """Send a JSON message to the specified SQS queue."""
    if not queue_url:
        logger.warning("SQS_QUEUE_URL not set; skipping SQS notification.")
        return

    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(payload),
        MessageAttributes={
            "source": {"DataType": "String", "StringValue": "data-pipeline"},
            "event": {"DataType": "String", "StringValue": "publish_completed"},
        },
    )
    logger.info("SQS notification sent to %s", queue_url)


def lambda_handler(event, context):
    # 1) Fetch and store data
    fetch_result = fetch_and_store_population_data()

    # 2) Publish dataset
    publish_result = publish_main()

    # 3) Notify via SQS
    queue_url = os.environ.get("SQS_QUEUE_URL", "")
    message = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "requestId": getattr(context, "aws_request_id", None),
        "fetch": fetch_result,
        "publish": publish_result,
    }

    try:
        send_sqs_notification(queue_url, message)
        notified = bool(queue_url)
    except Exception as exc:
        logger.exception("Failed to send SQS notification: %s", exc)
        notified = False

    # Return a concise summary for observability
    return {
        "fetch": fetch_result,
        "publish": publish_result,
        "sqsNotified": notified,
    }