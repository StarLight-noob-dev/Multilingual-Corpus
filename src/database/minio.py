import logging

import urllib3
from minio import Minio
from urllib3 import Retry

from src.database.config import MINIO_CONFIG

log = logging.getLogger("MinIOClient")

def get_minio_client(
        endpoint=MINIO_CONFIG["endpoint"],
        access_key=MINIO_CONFIG["access_key"],
        secret_key=MINIO_CONFIG["secret_key"],
        secure=MINIO_CONFIG["secure"],
        max_connections=MINIO_CONFIG["max_connections"],
        retry=MINIO_CONFIG["retries"],
        timeout=MINIO_CONFIG["timeout"]
) -> Minio:
    retry_strategy = Retry(
        total=retry,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504]
    )

    http_client = urllib3.PoolManager(
        num_pools=max_connections,  # Number of connection pools
        maxsize=max_connections,  # Maximum connections per pool
        retries=retry_strategy,
        timeout=timeout
    )

    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
        http_client=http_client  # pass the session here
    )

    log.info(f"MinIO client created with connection pool size {max_connections}")
    return client

def initialize_minio():
    print("Initializing buckets...")
    client = get_minio_client()
    if not client.bucket_exists("books"):
        client.make_bucket("books")
        print("Created 'books' bucket.")
    else:
        print("'books' bucket already exists.")
    print("Buckets initialized.")


