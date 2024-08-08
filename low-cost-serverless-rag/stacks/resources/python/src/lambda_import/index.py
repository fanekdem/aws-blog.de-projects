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


OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX")
DOCUMENTS_OUTPUT_PREFIX = os.environ.get("DOCUMENTS_OUTPUT_PREFIX")

src_dir_path = str(pathlib.Path(__file__).parent.parent)
sys.path.append(src_dir_path)

from common.helpers import (
    export_chunks_information_to_parquet,
    compute_chunks_information,
    get_document_text,
)


def lambda_handler(event: dict[str, object], context: dict[str, object]):
    print(json.dumps(event))

    records = event["Records"]

    for record in records:
        bucket_input = record["s3"]["bucket"]["name"]
        object_key_input = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        filetype = object_key_input.split(".")[-1]

        object_s3_uri = f"s3://{bucket_input}/{object_key_input}"
        object_content = s3_client.get_object(Bucket=bucket_input, Key=object_key_input)["Body"].read()
        document_text = get_document_text(document_blob=object_content, filetype=filetype)

        chunks_information = compute_chunks_information(document_text, object_s3_uri)

        input_file_basename = os.path.basename(object_s3_uri)
        input_file_path_hexdigest = hashlib.sha1(object_s3_uri.encode()).hexdigest()
        output_basename = f"{input_file_basename}-{input_file_path_hexdigest}.parquet"

        csv_file_buffer = io.BytesIO()
        export_chunks_information_to_parquet(chunks_information, csv_file_buffer)
        csv_file_buffer.seek(0)

        bucket_output = OUTPUT_BUCKET

        object_key_output = os.path.join(DOCUMENTS_OUTPUT_PREFIX, str(output_basename))
        s3_client.put_object(Body=csv_file_buffer.getvalue(), Bucket=bucket_output, Key=object_key_output)


if __name__ == "__main__":
    sample_object_created_path = pathlib.Path(__file__).parent.joinpath("sample", "object_created.json")
    with open(sample_object_created_path) as f_in:
        event = json.load(f_in)
    lambda_handler(event, None)
