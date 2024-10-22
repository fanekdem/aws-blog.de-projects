# see: https://docs.aws.amazon.com/lambda/latest/dg/python-image.html#python-image-clients

# Define custom function directory
ARG FUNCTION_DIR="/function"

FROM python:3.12 as build-image

# Include global arg in this stage of the build
ARG FUNCTION_DIR

# Copy function code
RUN mkdir -p ${FUNCTION_DIR}
COPY . ${FUNCTION_DIR}

# Install the function's dependencies
RUN pip install \
    --target ${FUNCTION_DIR} \
        awslambdaric

# Use a slim version of the base Python image to reduce the final image size
FROM python:3.12-slim

# Include global arg in this stage of the build
ARG FUNCTION_DIR
# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}

# Copy in the built dependencies
COPY --from=build-image ${FUNCTION_DIR} ${FUNCTION_DIR}

# ### Customization start ###
# Install sqlite-vss dependencies
RUN apt update && apt install libgomp1 libatlas3-base -y

# Copy python requirements file
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install python requirements
RUN python3 -m pip install -r requirements.txt --target ${FUNCTION_DIR}

# Copy python code to target dir
COPY src ${FUNCTION_DIR}/

# ### Customization end ###

# Set runtime interface client as default command for the container runtime
ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]

# Pass the name of the function handler as an argument to the runtime
CMD [ "lambda_function.handler" ]