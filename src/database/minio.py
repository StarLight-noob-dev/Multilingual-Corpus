from minio import Minio

from src.database.config import MINIO_CONFIG


def get_minio_client():
    return Minio(**MINIO_CONFIG)


def initialize_minio():
    print("Initializing buckets...")
    client = get_minio_client()
    if not client.bucket_exists("books"):
        client.make_bucket("books")
        print("Created 'books' bucket.")
    else:
        print("'books' bucket already exists.")
    print("Buckets initialized.")


