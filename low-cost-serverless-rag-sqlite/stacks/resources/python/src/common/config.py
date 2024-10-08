ACCEPT = "application/json"
CONTENT_TYPE = "application/json"

# NOTE: Leave to None in case you want to query bedrock endpoints within the same region as the lambda function
AWS_REGION_BEDROCK = "eu-central-1"

# The model we use to generate embeddings
EMBEDDING_MODEL_ID = "amazon.titan-embed-text-v1"

# The model we use to answer queries based on retrieved documents
CHAT_MODEL_ID = "amazon.titan-text-express-v1"

# How much text the LLM generate is allowed to generate in response to a query (max value specific to CHAT_MODEL_ID)
MAX_TOKEN_OUTPUT = 1024

# The dimension of embedding generated by the model configure using EMBEDDING_MODEL_ID
EMBEDDING_SIZE = 1024 + 512

# How much text we want to import as a single item
CHUNK_SIZE = 512

# How much text (chars) should overlap between chunks (50% before, 50% after)
CHUNK_OVERLAP_SIZE = 128


ON_DISK_DATABASE = "/tmp/db.sqlite"
IN_MEMORY_DATABASE = ":memory:"