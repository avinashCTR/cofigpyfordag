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


def local_to_hdfs(local_path,hdfs_path):
    
    with open(local_path, 'rb') as f:
        with hdfs.open(hdfs_path, 'wb') as hdfs_f:
            hdfs_f.write(f.read())

def hdfs_to_local(hdfs_path,local_path):
    
    with hdfs.open(hdfs_path, 'rb') as f:
        with open(local_path, 'wb') as local_f:
            local_f.write(f.read())

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