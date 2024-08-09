import os
import boto3
import sys
import time

import pathlib
import awswrangler as wr
import pandas as pd

src_dir_path = str(pathlib.Path(__file__).parent.parent)
sys.path.append(src_dir_path)


def log_time(function):
    def logging_time_function(*args, **kwargs):
        start = time.time()
        result = function(*args, **kwargs)
        end = time.time()
        time_ms = int((end - start) * 1000)
        print("<time>" * 10, "START", "<time>" * 10)
        print(f"Timed {time_ms} ms for function <{function.__name__}>")
        print("<time>" * 10, "END", "<time>" * 10)
        return result

    return logging_time_function


SupportsWrite = object

s3_client = boto3.client("s3")
athena_client = boto3.client("athena")


# Leave to None in case use want to query endpoints within the same region as the lambda function
AWS_REGION = "eu-central-1"
# Create the connection to Bedrock
bedrock = boto3.client(service_name="bedrock", region_name=AWS_REGION)

bedrock_runtime = boto3.client(service_name="bedrock-runtime", region_name=AWS_REGION)

from common.helpers import (
    compute_embedding_lsh,
    get_llm_query_response_text,
)
from common.config import MAX_TOKEN_OUTPUT

ATHENA_TABLE = os.environ.get("ATHENA_TABLE", "documents")
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP")

TOP_N_DOCUMENTS = int(os.environ.get("TOP_N_DOCUMENTS", "10"))


# NOTE: score ranges from 0.0 to 100.0
QUERY_SCORE_THRESHOLD = int(os.environ.get("QUERY_SCORE_THRESHOLD", "60"))

ATHENA_DOCUMENTS_QUERY_TEMPLATE = """
WITH scored_documents AS (
    SELECT
        "uuid", "start", "end", start_unique, end_unique, lsh, document_id, "text",
        (length(lsh) - hamming_distance(lsh, '{query_lsh}')) * 100.0 / length(lsh) score
    FROM
        "awsdatacatalog"."{athena_database}"."{athena_table}"
)

SELECT * FROM scored_documents
WHERE
    score >= {score_threshold}
ORDER BY score DESC
LIMIT {top_n_documents}
"""


def get_athena_documents_query(
    query_lsh: str,
    score_threshold: float = QUERY_SCORE_THRESHOLD,
    top_n_documents: int = TOP_N_DOCUMENTS,
    athena_database: str = ATHENA_DATABASE,
    athena_table: str = ATHENA_TABLE,
    template: str = ATHENA_DOCUMENTS_QUERY_TEMPLATE,
):
    return template.format(
        query_lsh=query_lsh,
        score_threshold=score_threshold,
        top_n_documents=top_n_documents,
        athena_table=athena_table,
        athena_database=athena_database,
    )


LLM_RAG_QUERY_TEMPLATE = """
You are a friendly AI-Bot and answer queries about any topic within your knowledge and particularly within your context.
Your answers are as exact and brief as possible.
In case you are not able to answer a query, you clearly state that you do not know the answer.

Answer the following query by summarizing information within your context:
{{{query}}}

You can use the following information to answer the query:
{{{documents}}}

"""


def build_llm_query(documents: str, query: str):
    return LLM_RAG_QUERY_TEMPLATE.format(documents=documents, query=query)


@log_time
def get_chunks_df(sql: str, database: str = ATHENA_DATABASE, workgroup: str = ATHENA_WORKGROUP, **kwargs):
    if "ctas_approach" in kwargs:
        kwargs.pop("ctas_approach")
    return wr.athena.read_sql_query(sql=sql, database=database, ctas_approach=False, workgroup=workgroup, **kwargs)


def get_text_from_chunks(chunks_df: pd.DataFrame):
    """)
    Extract, preprocess and combine text from chunks
    """

    # NOTE: for a real use case we would use a proper strategy to clean and put the text together
    # e.g. by avoiding overlaps, making sure the final text would fit within the LLM context etc.
    documents_text = "\n".join(chunks_df["text"].to_list())
    return documents_text


def lambda_handler(event: dict[str, object], context: dict[str, object]):
    print(event)

    query = event["query"]

    query_lsh = log_time(compute_embedding_lsh)(query)

    documents_sql_query = get_athena_documents_query(query_lsh=query_lsh)

    print("-" * 100)
    print(documents_sql_query)
    print("-" * 50, "REPR")
    print(repr(documents_sql_query))

    chunks_df = get_chunks_df(
        sql=documents_sql_query, database=ATHENA_DATABASE, ctas_approach=False, workgroup=ATHENA_WORKGROUP
    )
    print(chunks_df)

    chunks_text = get_text_from_chunks(chunks_df)

    if not chunks_text:
        chunks_text = "not enough information available"

    llm_query = build_llm_query(
        documents=chunks_text,
        query=query,
    )

    llm_request_body = {"inputText": llm_query, "textGenerationConfig": {"maxTokenCount": MAX_TOKEN_OUTPUT}}
    answer_text = log_time(get_llm_query_response_text)(llm_request_body)
    print("*" * 50, "LLM_ANSWER")
    print(answer_text)
    response = {"text": answer_text}
    return response


if __name__ == "__main__":
    lambda_handler({"query": "Tell me the story of the ugly prince."}, None)
