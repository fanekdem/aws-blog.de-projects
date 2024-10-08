import datetime
import io
import boto3
import json
import re

import botocore
import fitz


from .config import (
    ACCEPT,
    AWS_REGION_BEDROCK,
    CHAT_MODEL_ID,
    CHUNK_OVERLAP_SIZE,
    CHUNK_SIZE,
    CONTENT_TYPE,
    EMBEDDING_MODEL_ID,
)

DEFAULT_LOCAL_DB_PATH = "/tmp/db.sqlite3"

s3_resource = boto3.resource("s3")
s3_client = boto3.client("s3")

bedrock_runtime = boto3.client(service_name="bedrock-runtime", region_name=AWS_REGION_BEDROCK)


def get_cleaned_text(text):
    """
    Clean text - currently just reduces consecutive new lines.
    """
    return re.sub("(\n\n *)( *\n)*", "\n\n", text)


def get_file_text(document_blob: bytes, filetype: str = None) -> str:
    """
    Extract text from the passed document bytes
    """
    if filetype in {"txt", "md"}:
        document_text = document_blob.decode()
    else:
        document = fitz.open(stream=io.BytesIO(document_blob), filetype=filetype)
        document_text = "\n\n".join(page.get_text() for page in document.pages())
    return document_text


def get_embedding(
    text: str,
    model_id: str = EMBEDDING_MODEL_ID,
    content_type: str = CONTENT_TYPE,
    accept: str = ACCEPT,
) -> list[float]:
    """
    Compute an embedding for the given text and return a list of floats.
    The size of the list depends on model_id.
    """
    body = json.dumps({"inputText": text})
    response = bedrock_runtime.invoke_model(body=body, modelId=model_id, accept=accept, contentType=content_type)
    response_body = json.loads(response["body"].read())
    embedding = response_body.get("embedding")
    return embedding


def get_llm_query_response(
    body: dict,
    model_id: str = CHAT_MODEL_ID,
    content_type: str = CONTENT_TYPE,
    accept: str = ACCEPT,
) -> dict:
    response = bedrock_runtime.invoke_model(
        body=json.dumps(body), modelId=model_id, accept=accept, contentType=content_type
    )
    response_body = json.loads(response["body"].read())
    return response_body


def get_llm_query_response_text(
    body: dict, model_id: str = CHAT_MODEL_ID, content_type: str = CONTENT_TYPE, accept: str = ACCEPT
) -> str:
    answer: dict = get_llm_query_response(body, model_id, content_type, accept)
    text = " ".join(item["outputText"] for item in answer["results"])
    return text


def compute_text_chunks(
    text: str, chunk_size: int = CHUNK_SIZE, chunk_overlap_size: int = CHUNK_OVERLAP_SIZE
) -> tuple[tuple[int], tuple[int], str]:
    """
    Split the text in chunks with overlapping and return chunks with position information:
    text_start_pos and text_end_pos are relative to the whole text passed
    unique_text_start_pos and unique_text_end_pos and relative to text_chunk and can be used to rebuild the
        the initial text without overlapping
    [(text_start_pos, text_end_pos), (unique_text_start_pos, unique_text_end_pos), text_chunk]

    """
    chunks = []
    single_side_overlap_size = (chunk_overlap_size + 1) // 2

    if len(text) < chunk_overlap_size:
        chunks.append(((0, len(text)), (0, len(text)), text))
    else:
        for idx in range(single_side_overlap_size, len(text), chunk_size - chunk_overlap_size):
            chunk_start_idx = idx - single_side_overlap_size
            chunk_end_idx = idx + chunk_size - single_side_overlap_size
            chunk = text[chunk_start_idx:chunk_end_idx]
            unique_chunk_start_idx = 0
            unique_chunk_end_idx = len(chunk) - single_side_overlap_size * 2
            if chunk_end_idx >= len(text):
                unique_chunk_end_idx = len(chunk)
            chunks.append(((chunk_start_idx, chunk_end_idx), (unique_chunk_start_idx, unique_chunk_end_idx), chunk))
    return chunks


def compute_documents_information(
    text: str, document_id: str | None = None, timestamp: str | None = None
) -> list[dict[str, str | int]]:
    """
    Given a text, split it in chunks and return chunks with corresponding embedding information
    [{
        "timestamp": timestamp,
        "start": start,
        "end": end,
        "start_unique": start_unique,
        "end_unique": end_unique,
        "embedding": text embedding,
        "document_id": document_id,
        "text": text,
    }]
    """

    if timestamp is None:
        timestamp = datetime.datetime.now().isoformat(" ", timespec="seconds")
    items = []
    for pos, unique_pos, text in compute_text_chunks(text):
        start, end = pos
        start_unique, end_unique = unique_pos
        cleaned_text = get_cleaned_text(text)
        embedding = get_embedding(cleaned_text)
        item = {
            "timestamp": timestamp,
            "start": start,
            "end": end,
            "start_unique": start_unique,
            "end_unique": end_unique,
            "embedding": embedding,
            "document_id": document_id,
            "text": text,
        }
        items.append(item)
    return items


def get_s3_file_locally(bucket: str, key: str, local_path: str = DEFAULT_LOCAL_DB_PATH):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        with open(local_path, "wb") as file_out:
            s3_client.download_fileobj(bucket, key, file_out)
    except botocore.exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "404":
            # The file does not exists yet, so we will just create a new one locally
            print("File not existing yet, but it is fine!")
            pass
        else:
            raise

    return local_path


def upload_file(bucket: str, key: str, local_path: str = DEFAULT_LOCAL_DB_PATH):
    s3_client.upload_file(local_path, bucket, key)
