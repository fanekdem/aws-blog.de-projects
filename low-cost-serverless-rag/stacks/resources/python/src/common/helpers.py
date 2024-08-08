import datetime
import io
import uuid
import boto3
import json
from lshashpy3 import LSHash

import pandas as pd
import numpy as np
import fitz


from .config import (
    ACCEPT,
    AWS_REGION_BEDROCK,
    CHAT_MODEL_ID,
    CHUNK_OVERLAP_SIZE,
    CHUNK_SIZE,
    CONTENT_TYPE,
    EMBEDDING_LSH_SEED,
    EMBEDDING_LSH_SIZE,
    EMBEDDING_MODEL_ID,
    EMBEDDING_SIZE,
)

SupportsWrite = object

s3_client = boto3.client("s3")
athena_client = boto3.client("athena")

bedrock_runtime = boto3.client(service_name="bedrock-runtime", region_name=AWS_REGION_BEDROCK)


def get_document_text(document_blob: bytes, filetype: str = None) -> str:
    """
    Extract text from the passed document bytes
    """
    if filetype in {"txt", "md"}:
        document_text = document_blob.decode()
    else:
        document = fitz.open(stream=io.BytesIO(document_blob), filetype=filetype)
        document_text = "\n\n".join(page.get_text() for page in document.pages())
    return document_text


class LSHashCustom(LSHash):
    def __init__(self, hash_size, input_dim, seed=None, **kwargs):
        self.seed = seed
        super().__init__(hash_size, input_dim, **kwargs)

    def hash(self, input_point):
        """
        hash a single input point by adding it to the selected storage.

        If `extra_data` is provided, it will become the value of the dictionary
        {input_point: extra_data}, which in turn will become the value of the
        hash table. `extra_data` needs to be JSON serializable if in-memory
        dict is not used as storage.

        :param input_point:
            A list, or tuple, or numpy ndarray object that contains numbers
            only. The dimension needs to be 1 * `input_dim`.
            This object will be converted to Python tuple and its hash value returned.

        Note: we always only consider a single table hash
        """

        value = (tuple(input_point), None)

        hashes = []
        for i, table in enumerate(self.hash_tables):
            h = self._hash(self.uniform_planes[i], input_point)
            table.append_val(h, value)
            hashes.append(h)
        return hashes[0]

    def _generate_uniform_planes(self):
        """
        Generate uniformly distributed hyperplanes and return it as a 2D numpy array.
        ---
        @override:
            Allow seeded LSH
        """
        rnd = np.random.RandomState(self.seed)
        return rnd.randn(self.hash_size, self.input_dim)

    @classmethod
    def get_hasher(
        cls, hash_size: int = EMBEDDING_LSH_SIZE, input_dim: int = EMBEDDING_SIZE, seed: int = EMBEDDING_LSH_SEED
    ):
        hasher = LSHashCustom(hash_size=hash_size, input_dim=input_dim, seed=seed)
        return hasher


embedding_lsh_hasher = LSHashCustom.get_hasher()


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


def compute_embedding_lsh(text: str) -> str:
    """
    Given a text, compute and return a fixed length locality-sensitive-hash
    """
    embedding = get_embedding(text)
    return embedding_lsh_hasher.hash(embedding)


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


def compute_chunks_information(
    text: str, document_id: str | None = None, timestamp: str | None = None
) -> list[dict[str, str | int]]:
    """
    Given a text, split it in chunks and return chunks with corresponding embedding information as lsh hash
    [{
        "uuid": uuid,
        "timestamp": timestamp,
        "start": start,
        "end": end,
        "start_unique": start_unique,
        "end_unique": end_unique,
        "lsh": stringified_embedding,
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
        embedding_lsh = compute_embedding_lsh(text)
        item = {
            "uuid": str(uuid.uuid4()),
            "timestamp": timestamp,
            "start": start,
            "end": end,
            "start_unique": start_unique,
            "end_unique": end_unique,
            "lsh": embedding_lsh,
            "document_id": document_id,
            "text": text,
        }
        items.append(item)
    return items


def export_chunks_information_to_csv(items: list[dict[str, int | str]], csv_file_or_buffer: SupportsWrite):
    df = pd.DataFrame(items)
    df.to_csv(csv_file_or_buffer, index=False)


def export_chunks_information_to_parquet(items: list[dict[str, int | str]], csv_file_or_buffer: SupportsWrite):
    df = pd.DataFrame(items)
    df.to_parquet(csv_file_or_buffer, compression="snappy", index=False)


if __name__ == "__main__":
    chunks = compute_chunks_information("Hello world everyone!")
    print(chunks)
