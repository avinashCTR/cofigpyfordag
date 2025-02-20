#!/bin/bash

# Variables for the arguments
W2R_PATH="value1"
IPA_META_DATA_PATH="value2"
W2R_SCORED_PATH="value3"
IPA_META_DATA_OUTPUT_PATH="value4"
MAX_IPA_COMPUTATIONS=10000

#make a temp folder
mkdir temp

# copy the folders from the hdfs to the local
hdfs dfs -get /app/notebooks/avinash/IPA-TASK/temp/w2r $W2R_PATH
hdfs dfs -get /app/notebooks/avinash/IPA-TASK/temp/ipa_meta_data $IPA_META_DATA_PATH

# Path to the Python file on your local machine
LOCAL_PYTHON_FILE="/absolute/path/to/your/python_script.py"

# Destination directory and file inside the Docker container
CONTAINER_DIR="/app"
CONTAINER_PYTHON_FILE="$CONTAINER_DIR/python_script.py"

# Start the container with a dummy process to keep it alive
docker run -dit --name avinash my_image tail -f /dev/null

# Ensure the destination directory exists inside the container
docker exec avinash mkdir -p "$CONTAINER_DIR"

# Copy the Python file to the Docker container
docker cp "$LOCAL_PYTHON_FILE" avinash:"$CONTAINER_PYTHON_FILE"

# copy the folders from the local to the container
docker cp $W2R_PATH avinash:/app
docker cp $IPA_META_DATA_PATH avinash:/app

# Verify that the Python file exists inside the container
docker exec avinash ls -l "$CONTAINER_PYTHON_FILE"

# Execute the Python file with the arguments inside the Docker container
docker exec avinash python3 "$CONTAINER_PYTHON_FILE" \
    --w2r_path "$W2R_PATH" \
    --ipa_meta_data_path "$IPA_META_DATA_PATH" \
    --w2r_scored_path "$W2R_SCORED_PATH" \
    --ipa_meta_data_output_path "$IPA_META_DATA_OUTPUT_PATH" \
    --max_ipa_computations "$MAX_IPA_COMPUTATIONS"

# copy the folders from the container to the local
docker cp avinash:/app/w2r_scored $W2R_SCORED_PATH
docker cp avinash:/app/ipa_meta_data_output $IPA_META_DATA_OUTPUT_PATH

# send the folders to the hdfs
hdfs dfs -put $W2R_SCORED_PATH /app/notebooks/avinash/IPA-TASK/temp/w2r_scored
hdfs dfs -put $IPA_META_DATA_OUTPUT_PATH /app/notebooks/avinash/IPA-TASK/temp/ipa_meta_data_output

# clean up temp folders
rm -rf temp

# (Optional) Stop and remove the container after execution
docker stop avinash
docker rm avinash
