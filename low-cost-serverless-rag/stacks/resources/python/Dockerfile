# Use a NodeJS 20.x runtime
FROM public.ecr.aws/lambda/python:3.11

# Copy the function code to the LAMBDA_TASK_ROOT directory
# This environment variable is provided by the lambda base image
COPY requirements.txt ${LAMBDA_TASK_ROOT}

RUN python -m pip install -r requirements.txt

COPY src ${LAMBDA_TASK_ROOT}/
# Set the CMD to the function handler
# CMD [ "lambda_import.index.lambda_handler" ]