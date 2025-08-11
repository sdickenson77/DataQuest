import json
import logging
import os
from datetime import datetime, timezone
import tempfile
from urllib.parse import unquote_plus
from pathlib import Path

import boto3
import papermill as pm
import nbformat

# import the scripts
from scripts.publish_open_dataset import main as publish_main
from scripts.fetch_data_from_api import fetch_and_store_population_data


logger = logging.getLogger(__name__)
s3 = boto3.client("s3")


def send_sqs_notification(queue_url: str, payload: dict) -> None:
    """Send a JSON message to the specified SQS queue."""
    if not queue_url:
        logger.warning("SQS_QUEUE_URL not set; skipping SQS notification.")
        return

    sqs = boto3.client("sqs")
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(payload),
        MessageAttributes={
            "source": {"DataType": "String", "StringValue": "data-pipeline"},
            "event": {"DataType": "String", "StringValue": payload.get("event", "publish_completed")},
        },
    )
    logger.info("SQS notification sent to %s", queue_url)


def _execute_notebook_from_s3(
    notebook_bucket: str,
    notebook_key: str,
    output_bucket: str | None = None,
    output_prefix: str | None = None,
    parameters: dict | None = None,
) -> dict:
    """
    Download the source notebook from S3, execute it with Papermill, and:
      - If output_bucket is None: only log cell outputs to CloudWatch (no upload).
      - Otherwise: upload executed notebook to the specified S3 location.
    Returns execution metadata.
    """
    # 1) Download input notebook to /tmp
    with tempfile.TemporaryDirectory() as td:
        in_path = os.path.join(td, "input.ipynb")
        s3.download_file(notebook_bucket, notebook_key, in_path)

        # 2) Execute with log_output=True so outputs go to Lambda logs (CloudWatch)
        out_path = os.path.join(
            td, f"executed_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}.ipynb"
        )
        logger.info("Executing notebook %s/%s with parameters=%s", notebook_bucket, notebook_key, parameters or {})
        pm.execute_notebook(
            input_path=in_path,
            output_path=out_path,      # write locally only
            parameters=parameters or {},
            log_output=True,           # key: stream cell outputs to logs
            kernel_name=None,          # let Papermill choose default
        )

        # 3) Optionally upload the executed notebook
        uploaded_uri = None
        if output_bucket:
            key = f"{(output_prefix or '').rstrip('/')}/" + os.path.basename(out_path) if output_prefix else os.path.basename(out_path)
            s3.upload_file(out_path, output_bucket, key)
            uploaded_uri = f"s3://{output_bucket}/{key}"
            logger.info("Executed notebook uploaded to %s", uploaded_uri)
        else:
            logger.info("Notebook outputs logged to CloudWatch only; no S3 upload performed.")

    return {
        "outputUploaded": bool(uploaded_uri),
        "outputS3Uri": uploaded_uri,
        "cloudwatchLogging": True,
        "executedAt": datetime.now(timezone.utc).isoformat(),
    }


def _is_s3_put_event(event: dict) -> bool:
    """Detect if the Lambda event is an S3 ObjectCreated event."""
    if not isinstance(event, dict) or "Records" not in event:
        return False
    for rec in event["Records"]:
        if rec.get("eventSource") == "aws:s3" and "ObjectCreated" in rec.get("eventName", ""):
            return True
    return False


def _handle_s3_event(event: dict, context) -> dict:
    """
    Process S3 ObjectCreated events for JSON files under population_data/.
    For each matching object, execute the configured notebook and log outputs to CloudWatch.
    """
    processed = []
    skipped = []

    nb_bucket = os.environ["NOTEBOOK_S3_BUCKET"]
    nb_key = os.environ["NOTEBOOK_S3_KEY"]

    #for jupyter
    os.environ.setdefault("JUPYTER_PATH", "/var/task/share/jupyter")

    # Force log-only mode (no S3 output)
    out_bucket = None
    out_prefix = None

    for rec in event.get("Records", []):
        if rec.get("eventSource") != "aws:s3":
            continue

        bkt = rec["s3"]["bucket"]["name"]
        key = unquote_plus(rec["s3"]["object"]["key"])

        if not key.startswith("population_data/") or not key.endswith(".json"):
            skipped.append({"bucket": bkt, "key": key, "reason": "not population_data/*.json"})
            continue

        json_uri = f"s3://{bkt}/{key}"
        logger.info("Triggering notebook for new object: %s", json_uri)

        exec_meta = _execute_notebook_from_s3(
            notebook_bucket=nb_bucket,
            notebook_key=nb_key,
            output_bucket=out_bucket,     # None => log-only
            output_prefix=out_prefix,     # None => log-only
            parameters={
                "input_json_s3_uri": json_uri,
                "trigger_bucket": bkt,
                "trigger_key": key,
                "run_timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )
        processed.append({"inputJsonUri": json_uri, **exec_meta})

    # Notify via SQS (optional; unchanged)
    queue_url = os.environ.get("SQS_QUEUE_URL", "")
    message = {
        "event": "notebook_executed",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "requestId": getattr(context, "aws_request_id", None),
        "processed": processed,
        "skipped": skipped,
    }
    try:
        send_sqs_notification(queue_url, message)
        notified = bool(queue_url)
    except Exception as exc:
        logger.exception("Failed to send SQS notification after notebook execution: %s", exc)
        notified = False

    return {"processed": processed, "skipped": skipped, "sqsNotified": notified}


def lambda_handler(event, context):
    # If invoked by S3 event, run the notebook workflow for new JSONs
    if _is_s3_put_event(event):
        return _handle_s3_event(event, context)

    # Default behavior: API fetch -> publish -> SQS notify
    fetch_result = fetch_and_store_population_data()
    publish_result = publish_main()

    queue_url = os.environ.get("SQS_QUEUE_URL", "")
    message = {
        "event": "publish_completed",
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

    return {
        "fetch": fetch_result,
        "publish": publish_result,
        "sqsNotified": notified,
    }