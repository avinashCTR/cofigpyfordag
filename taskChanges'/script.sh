#!/bin/bash

# Variables for the arguments
W2R_PATH="value1"
IPA_META_DATA_PATH="value2"
W2R_SCORED_PATH="value3"
IPA_META_DATA_OUTPUT_PATH="value4"
MAX_IPA_COMPUTATIONS=10000

# Path to the Python file
PYTHON_FILE="/path/to/your/python_script.py"

# Execute the Python file with the arguments
python3 $PYTHON_FILE \
    --w2r_path $W2R_PATH \
    --ipa_meta_data_path $IPA_META_DATA_PATH \
    --w2r_scored_path $W2R_SCORED_PATH \
    --ipa_meta_data_output_path $IPA_META_DATA_OUTPUT_PATH \
    --max_ipa_computations $MAX_IPA_COMPUTATIONS \