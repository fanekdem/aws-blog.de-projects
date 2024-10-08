import datetime
import hashlib
import os
import sys
import io
import json
import pathlib
import urllib

import boto3

s3_client = boto3.client("s3")


SQLITE_DB_S3_BUCKET = os.getenv("SQLITE_DB_S3_BUCKET")
SQLITE_DB_S3_KEY = os.getenv("SQLITE_DB_S3_KEY")

src_dir_path = str(pathlib.Path(__file__).parent.parent)
sys.path.append(src_dir_path)

from common.helpers import (
    compute_documents_information,
    get_file_text,
    get_s3_file_locally,
    upload_file,
)
from common.db import (
    save_documents_to_db,
    initialize_db,
)

# NOTE: There is no need to re-download the sqlite db file, as the file can not be concurrently updated with the setup
# so that the local within a lambda instance is the most up-to-date-one
# This is why we can use the same local file for all lambda instances

LOCAL_DB_URI = get_s3_file_locally(SQLITE_DB_S3_BUCKET, SQLITE_DB_S3_KEY)
DB_CONNECTION = initialize_db(LOCAL_DB_URI)


def process_single_s3_record(s3_record: dict):
    bucket_input = s3_record["s3"]["bucket"]["name"]
    object_key_input = urllib.parse.unquote_plus(s3_record["s3"]["object"]["key"])
    filetype = object_key_input.split(".")[-1]

    object_s3_uri = f"s3://{bucket_input}/{object_key_input}"
    object_content = s3_client.get_object(Bucket=bucket_input, Key=object_key_input)["Body"].read()
    file_text = get_file_text(document_blob=object_content, filetype=filetype)

    documents = compute_documents_information(file_text, object_s3_uri)
    save_documents_to_db(documents, connection=DB_CONNECTION)


def lambda_handler(event: dict[str, object], context: dict[str, object]):
    sqs_batch_response: dict = {"batch_item_failures": []}

    silenced_errors = []
    if event:
        records = event["Records"]
        batch_item_failures = []

        for record in records:
            record_body_reconstructed: dict[str, str | list | dict] = json.loads(record["body"])
            try:
                sqs_records: list[dict] = record_body_reconstructed["Records"]
                for sqs_record in sqs_records:
                    s3_records: list[dict] = json.loads(sqs_record["body"])["Records"]
                    for s3_record in s3_records:
                        process_single_s3_record(s3_record)
            except Exception as e:
                batch_item_failures.append({"itemIdentifier": record["messageId"]})
                silenced_errors.append(str(e))

        sqs_batch_response["batchItemFailures"] = batch_item_failures
        upload_file(SQLITE_DB_S3_BUCKET, SQLITE_DB_S3_KEY)

    if silenced_errors:
        print("SILENCED ERRORS: ", silenced_errors)

    return sqs_batch_response


if __name__ == "__main__":
    sample_object_created_path = pathlib.Path(__file__).parent.joinpath("events", "sqs_event.json")
    with open(sample_object_created_path) as f_in:
        event = json.load(f_in)
    lambda_handler(event, None)
