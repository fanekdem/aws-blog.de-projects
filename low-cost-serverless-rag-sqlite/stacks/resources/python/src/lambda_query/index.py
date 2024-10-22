import os
import boto3
import sys
import time
import re
import pathlib

src_dir_path = str(pathlib.Path(__file__).parent.parent)
sys.path.append(src_dir_path)

from common.config import MAX_TOKEN_OUTPUT, AWS_REGION_BEDROCK
from common.db import (
    initialize_db,
    query_db_documents,
)
from common.helpers import (
    get_cleaned_text,
    get_embedding,
    get_s3_file_locally,
    get_llm_query_response_text,
)


TOP_N_DOCUMENTS = int(os.environ.get("TOP_N_DOCUMENTS", "6"))


# NOTE: possible distances ranges from 0 to ~2X embedding-size
MAX_DISTANCE_THRESHOLD = int(os.environ.get("MAX_DISTANCE_THRESHOLD", "350"))

SQLITE_DB_S3_BUCKET = os.getenv("SQLITE_DB_S3_BUCKET")
SQLITE_DB_S3_KEY = os.getenv("SQLITE_DB_S3_KEY")


def log_time(function):
    def logging_time_function(*args, **kwargs):
        start = time.time()
        result = function(*args, **kwargs)
        end = time.time()
        time_ms = int((end - start) * 1000)
        print("*" * 40, "START", f"<{function.__name__}>", "*" * 40)
        print(f"Timed {time_ms} ms for function <{function.__name__}>")
        print("*" * 40, " END ", "*" * 40)
        return result

    return logging_time_function


LOCAL_DB_URI = log_time(get_s3_file_locally)(SQLITE_DB_S3_BUCKET, SQLITE_DB_S3_KEY)
DB_CONNECTION = log_time(initialize_db)(LOCAL_DB_URI)


SupportsWrite = object

s3_client = boto3.client("s3")


# Create the connection to Bedrock
bedrock = boto3.client(service_name="bedrock", region_name=AWS_REGION_BEDROCK)

bedrock_runtime = boto3.client(service_name="bedrock-runtime", region_name=AWS_REGION_BEDROCK)


LLM_RAG_QUERY_TEMPLATE = """
You are a friendly AI-Bot and answer queries about any topic within your knowledge and particularly within your context.
Your answers are as exact and brief as possible.
In case you are not able to answer a query, you clearly state that you do not know the answer.

Answer the following query by summarizing information within your context:
{{{query}}}

You can use the following information to answer the query:
{{{documents}}}

"""


def build_llm_query(documents_text: str, query: str):
    return LLM_RAG_QUERY_TEMPLATE.format(documents=documents_text, query=query)


def get_text_from_documents(documents: list[dict]):
    """
    Extract, preprocess and combine text from chunks
    """

    # NOTE: for a real use case we would use a proper strategy to clean and put the text together
    # e.g. by avoiding overlaps, making sure the final text would fit within the LLM context etc.
    documents_text = "\n".join(document["text"] for document in documents)

    return get_cleaned_text(documents_text)


def lambda_handler(event: dict[str, object], context: dict[str, object]):
    query = event["query"]

    embedding = log_time(get_embedding)(query)

    matching_documents = log_time(query_db_documents)(
        embedding=embedding,
        connection=DB_CONNECTION,
        top_n_documents=TOP_N_DOCUMENTS,
        distance_threshold=MAX_DISTANCE_THRESHOLD,
    )

    documents_text = get_text_from_documents(matching_documents)

    if not documents_text:
        documents_text = "Not enough information available."

    llm_query = build_llm_query(
        documents_text=documents_text,
        query=query,
    )

    llm_request_body = {"inputText": llm_query, "textGenerationConfig": {"maxTokenCount": MAX_TOKEN_OUTPUT}}
    answer_text = log_time(get_llm_query_response_text)(llm_request_body)

    response = {"text": answer_text}
    return response


if __name__ == "__main__":
    # lambda_handler({"query": "Tell me the story of the ugly prince."}, None)
    res = query_db_documents(
        embedding=[0.1] * 1536,
        connection=DB_CONNECTION,
        top_n_documents=TOP_N_DOCUMENTS,
        distance_threshold=MAX_DISTANCE_THRESHOLD,
    )
    print(res)
