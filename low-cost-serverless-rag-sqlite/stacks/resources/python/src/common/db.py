import sqlite3
import sqlite_vss
import json
import datetime
import numpy as np

from .config import EMBEDDING_SIZE, ON_DISK_DATABASE, IN_MEMORY_DATABASE

SQL_CREATE_VSS_DOCUMENTS_TABLE_TEMPLATE = """
CREATE virtual table IF NOT EXISTS vss_documents using vss0(
  text_embedding({embedding_size})
);
"""

SQL_CREATE_DOCUMENTS_TABLE = """
CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    text TEXT,
    timestamp DATETIME
);
"""

SQL_INSERT_DOCUMENT = """
INSERT INTO documents(text, timestamp)
VALUES(:text, :timestamp)
"""

SQL_INSERT_VSS_DOCUMENT = """
INSERT INTO vss_documents(rowid, text_embedding)
VALUES(:rowid, :text_embedding)
"""

SQL_QUERY_TEMPLATE_PRIOR_TO_V3_41_0 = """
WITH matched_documents (rowid, distance) AS (
    SELECT
        rowid,
        distance
    FROM vss_documents
    WHERE
        vss_search(
            text_embedding,
            vss_search_params(
                ?,
                {top_n_documents}
            )
        )
    ORDER BY distance ASC
)

SELECT
    d.*,
    m.distance
FROM matched_documents m
LEFT JOIN documents d ON m.rowid=d.rowid
WHERE
    m.distance <= {distance_threshold}
ORDER BY m.distance ASC
"""


SQL_QUERY_TEMPLATE_STARTING_FROM_V3_41_0 = """
WITH matched_documents AS (
    SELECT
        rowid,
        distance
    FROM vss_documents
    WHERE
        vss_search(text_embedding, ?)
    ORDER BY distance ASC
    LIMIT {top_n_documents}
)

SELECT
    d.*,
    m.distance
FROM matched_documents m
LEFT JOIN documents d ON m.rowid=d.rowid
WHERE
    m.distance <= {distance_threshold}
ORDER BY m.distance ASC
"""


MAX_DISTANCE_THRESHOLD = 999999999
TOP_N_DOCUMENTS = 10
EMBEDDING_SAMPLE = np.random.random(EMBEDDING_SIZE).tolist()
EMBEDDING_SAMPLE_TEST = [idx % 2 for idx in range(EMBEDDING_SIZE)]

SQLITE_VERSION_TUPLE = tuple(int(value) for value in sqlite3.sqlite_version.split("."))

print("SQLITE_VERSION: ", SQLITE_VERSION_TUPLE)


def initialize_db(database: str | bytes = ON_DISK_DATABASE, embedding_size: int = EMBEDDING_SIZE):
    db: sqlite3.Connection = sqlite3.connect(database)
    db.enable_load_extension(True)
    sqlite_vss.load(db)
    db.enable_load_extension(False)
    db.execute(SQL_CREATE_DOCUMENTS_TABLE)
    db.execute(SQL_CREATE_VSS_DOCUMENTS_TABLE_TEMPLATE.format(embedding_size=embedding_size))
    (version,) = db.execute("select vss_version()").fetchone()
    print("VSS_VERSION: ", version)
    return db


def get_serialized_embedding(embedding: list[float] | np.ndarray) -> str:
    if isinstance(embedding, np.ndarray):
        embedding = embedding.tolist()

    return json.dumps(embedding)


def _query_db_sqlite_prior_to_v3_41_0(
    embedding: list[float] | np.ndarray,
    connection: sqlite3.Connection,
    top_n_documents: int = TOP_N_DOCUMENTS,
    distance_threshold: float = MAX_DISTANCE_THRESHOLD,
) -> list[dict]:
    serialized_embedding = get_serialized_embedding(embedding)

    SQL_QUERY_VSS_DOCUMENTS_COUNT = """SELECT count(*) FROM vss_documents"""

    query = SQL_QUERY_TEMPLATE_STARTING_FROM_V3_41_0.format(
        top_n_documents=top_n_documents, distance_threshold=distance_threshold
    )

    cursor: sqlite3.Cursor = connection.cursor()
    cursor.row_factory = sqlite3.Row

    vss_documents_count: int = cursor.execute(SQL_QUERY_VSS_DOCUMENTS_COUNT).fetchone()[0]

    result: list[dict] = []

    # NOTE: The database extension generates an exception for sqlite < 3.41.0 in case the virtual table is empty
    # see: https://github.com/asg017/sqlite-vss/issues/129
    if vss_documents_count > 0:
        result_rows = cursor.execute(query, [serialized_embedding])
        result = [dict(item) for item in result_rows]
    return result


def _query_db_sqlite_starting_from_v3_41_0(
    embedding: list[float] | np.ndarray,
    connection: sqlite3.Connection,
    top_n_documents: int = TOP_N_DOCUMENTS,
    distance_threshold: float = MAX_DISTANCE_THRESHOLD,
) -> list[dict]:
    serialized_embedding = get_serialized_embedding(embedding)

    query = SQL_QUERY_TEMPLATE_STARTING_FROM_V3_41_0.format(
        top_n_documents=top_n_documents, distance_threshold=distance_threshold
    )

    cursor: sqlite3.Cursor = connection.cursor()
    cursor.row_factory = sqlite3.Row
    result_rows = cursor.execute(query, [serialized_embedding])
    result = [dict(item) for item in result_rows]
    return result


def query_db(
    embedding: list[float] | np.ndarray,
    connection: sqlite3.Connection,
    top_n_documents: int = TOP_N_DOCUMENTS,
    distance_threshold: float = MAX_DISTANCE_THRESHOLD,
) -> list[dict]:
    if SQLITE_VERSION_TUPLE < (3, 41, 0):
        return _query_db_sqlite_prior_to_v3_41_0(
            embedding=embedding,
            connection=connection,
            top_n_documents=top_n_documents,
            distance_threshold=distance_threshold,
        )
    else:
        return _query_db_sqlite_starting_from_v3_41_0(
            embedding=embedding,
            connection=connection,
            top_n_documents=top_n_documents,
            distance_threshold=distance_threshold,
        )


def save_document_into_db(
    document: dict,
    connection: sqlite3.Connection,
    sql_insert_document_query: str = SQL_INSERT_DOCUMENT,
    sql_insert_vss_document_query: str = SQL_INSERT_VSS_DOCUMENT,
):

    document_clone = document.copy()
    if "timestamp" not in document_clone:
        document_clone["timestamp"] = datetime.datetime.now()
    embedding: list[float] | np.ndarray = document_clone.pop("embedding")
    serialized_embedding = get_serialized_embedding(embedding)

    document_lastrowid = connection.execute(sql_insert_document_query, document_clone).lastrowid
    vss_document_lastrowid = connection.execute(
        sql_insert_vss_document_query, [document_lastrowid, serialized_embedding]
    ).lastrowid

    return document_lastrowid == vss_document_lastrowid


def save_documents_to_db(
    documents: list[dict],
    connection: sqlite3.Connection,
    sql_insert_document_query: str = SQL_INSERT_DOCUMENT,
    sql_insert_vss_document_query: str = SQL_INSERT_VSS_DOCUMENT,
):
    with connection:
        for document in documents:
            save_document_into_db(
                document=document,
                connection=connection,
                sql_insert_document_query=sql_insert_document_query,
                sql_insert_vss_document_query=sql_insert_vss_document_query,
            )


if __name__ == "__main__":
    db = initialize_db("./db.sqlite3")

    # res = save_documents_to_db([{"text": "Hello!", "embedding": EMBEDDING_SAMPLE}], connection=db)
    res = query_db(embedding=EMBEDDING_SAMPLE_TEST, connection=db)
    print(res)
