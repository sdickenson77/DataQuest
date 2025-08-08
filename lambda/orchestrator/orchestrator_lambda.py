import json
import logging
import os
from datetime import datetime, timezone
import tempfile
from urllib.parse import unquote_plus
from pathlib import Path

import boto3
import papermill as pm

# import the scripts
from scripts.fetch_data_from_api import fetch_and_store_population_data
from scripts.publish_open_dataset import main as publish_main

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# set up aws clients
sqs = boto3.client("sqs")
s3 = boto3.client("s3")


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
            "event": {"DataType": "String", "StringValue": payload.get("event", "publish_completed")},
        },
    )
    logger.info("SQS notification sent to %s", queue_url)


def _execute_notebook_from_s3(
    notebook_bucket: str,
    notebook_key: str,
    output_bucket: str | None,
    output_prefix: str | None,
    parameters: dict | None = None,
) -> dict:
    """
    Download a notebook from S3, execute with papermill, and upload executed notebook back to S3.
    Returns metadata about the execution and output location.
    """
    if not notebook_bucket or not notebook_key:
        raise ValueError("Notebook S3 location is required (NOTEBOOK_S3_BUCKET, NOTEBOOK_S3_KEY).")

    output_bucket = output_bucket or notebook_bucket
    output_prefix = (output_prefix or "executed_notebooks/").strip("/")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base_name = Path(notebook_key).name
    name_wo_ext = base_name[:-6] if base_name.endswith(".ipynb") else base_name
    output_key = f"{output_prefix}/{name_wo_ext}__executed__{ts}.ipynb"

    # Local temp files
    with tempfile.TemporaryDirectory() as tmpdir:
        local_in = os.path.join(tmpdir, "in.ipynb")
        local_out = os.path.join(tmpdir, "out.ipynb")

        logger.info("Downloading notebook s3://%s/%s to %s", notebook_bucket, notebook_key, local_in)
        s3.download_file(notebook_bucket, notebook_key, local_in)

        pm_params = parameters or {}
        logger.info("Executing notebook with parameters: %s", pm_params)

        # Execute the notebook
        pm.execute_notebook(
            input_path=local_in,
            output_path=local_out,
            parameters=pm_params,
            progress_bar=False,
            log_output=True,
        )

        logger.info("Uploading executed notebook to s3://%s/%s", output_bucket, output_key)
        s3.upload_file(local_out, output_bucket, output_key)

    return {
        "outputNotebookBucket": output_bucket,
        "outputNotebookKey": output_key,
        "outputNotebookUri": f"s3://{output_bucket}/{output_key}",
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
    For each matching object, execute the configured notebook, passing the JSON S3 URI as a parameter.
    """
    processed = []
    skipped = []

    nb_bucket = os.environ.get("NOTEBOOK_S3_BUCKET", "")
    nb_key = os.environ.get("NOTEBOOK_S3_KEY", "")
    out_bucket = os.environ.get("NOTEBOOK_OUTPUT_BUCKET", "") or None
    out_prefix = os.environ.get("NOTEBOOK_OUTPUT_PREFIX", "") or None

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
            output_bucket=out_bucket,
            output_prefix=out_prefix,
            parameters={
                "input_json_s3_uri": json_uri,
                "trigger_bucket": bkt,
                "trigger_key": key,
                "run_timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )
        processed.append({"inputJsonUri": json_uri, **exec_meta})

    # Notify via SQS (optional)
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