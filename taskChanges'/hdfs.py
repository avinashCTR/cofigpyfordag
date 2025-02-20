"""
This script will help to get the HDFS files to and from the local filesystem.
"""

import os
import pyarrow.parquet as pq
import pyarrow.fs as fs
import argparse

os.environ["LIBHDFS_OPTS"] = (
            "-Djava.security.krb5.conf=/home/jioapp/aditya/jiomart_cluster/krb5.conf"
        )
hdfs = pa.hdfs.connect(host='10.144.96.170', port=8020, kerb_ticket="/home/jioapp/aditya/jiomart_cluster/krb5cc_154046")


import pyarrow as pa
import os

# Connect to HDFS
def connect_to_hdfs(host='your-hdfs-host', port=9000, user='your-username'):
    return pa.hdfs.connect(host=host, port=port, user=user)

# Function to upload local directory to HDFS
def upload_directory(hdfs, local_dir, hdfs_dir):
    """
    Recursively upload a local directory to HDFS.
    """
    for root, dirs, files in os.walk(local_dir):
        # Construct the relative path for HDFS
        rel_path = os.path.relpath(root, local_dir)
        hdfs_path = os.path.join(hdfs_dir, rel_path).replace("\\", "/")  # Handle Windows paths

        # Create directory in HDFS if it doesn't exist
        if not hdfs.exists(hdfs_path):
            hdfs.mkdir(hdfs_path)
            print(f"Created HDFS directory: {hdfs_path}")

        # Upload files
        for file in files:
            local_file = os.path.join(root, file)
            hdfs_file = os.path.join(hdfs_path, file).replace("\\", "/")
            with hdfs.open(hdfs_file, 'wb') as hdfs_f, open(local_file, 'rb') as local_f:
                hdfs_f.write(local_f.read())
            print(f"Uploaded: {local_file} → {hdfs_file}")

# Function to download HDFS directory to local
def download_directory(hdfs, hdfs_dir, local_dir):
    """
    Recursively download an HDFS directory to local filesystem.
    """
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    for file_info in hdfs.ls(hdfs_dir, detail=True):
        hdfs_path = file_info['name']
        local_path = os.path.join(local_dir, os.path.relpath(hdfs_path, hdfs_dir))

        if file_info['kind'] == 'directory':
            os.makedirs(local_path, exist_ok=True)
            download_directory(hdfs, hdfs_path, local_path)
        else:
            with hdfs.open(hdfs_path, 'rb') as hdfs_f, open(local_path, 'wb') as local_f:
                local_f.write(hdfs_f.read())
            print(f"Downloaded: {hdfs_path} → {local_path}")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Copy files to and from HDFS')
    parser.add_argument('--direction', type=str, help='Direction of the copy: local_to_hdfs or hdfs_to_local')
    parser.add_argument('--local_path', type=str, help='Local path of the file')
    parser.add_argument('--hdfs_path', type=str, help='HDFS path of the file')
    args = parser.parse_args()

    direction = args.direction
    local_path = args.local_path
    hdfs_path = args.hdfs_path

    if direction == "local_to_hdfs":
        local_to_hdfs(local_path,hdfs_path)

    elif direction == "hdfs_to_local":
        hdfs_to_local(hdfs_path,local_path)

    else:
        print("Invalid direction. Please choose local_to_hdfs or hdfs_to_local")