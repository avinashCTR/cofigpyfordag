#!/bin/bash

# Variables for the arguments
W2R_PATH="value1"
IPA_META_DATA_PATH="value2"
W2R_SCORED_PATH="value3"
IPA_META_DATA_OUTPUT_PATH="value4"
MAX_IPA_COMPUTATIONS=10000

# Path to the Python file on your local machine
LOCAL_PYTHON_FILE="/path/to/your/python_script.py"

# Destination path for the Python file inside the Docker container
CONTAINER_PYTHON_FILE="/app/python_script.py"

# Start the container with a dummy process to keep it alive
docker run -dit --name my_container my_image tail -f /dev/null

# Copy the Python file to the Docker container
docker cp "$LOCAL_PYTHON_FILE" my_container:"$CONTAINER_PYTHON_FILE"

# Execute the Python file with the arguments inside the Docker container
docker exec my_container python3 "$CONTAINER_PYTHON_FILE" \
    --w2r_path "$W2R_PATH" \
    --ipa_meta_data_path "$IPA_META_DATA_PATH" \
    --w2r_scored_path "$W2R_SCORED_PATH" \
    --ipa_meta_data_output_path "$IPA_META_DATA_OUTPUT_PATH" \
    --max_ipa_computations "$MAX_IPA_COMPUTATIONS"

# (Optional) Stop and remove the container after execution
docker stop my_container
docker rm my_container
