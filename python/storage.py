# Task 2

from minio import Minio
from minio.error import S3Error
import msgpack
import io
from datetime import datetime
from typing import Dict, Any
import hashlib

# Task 3
from tokenization import DocumentTokenizer

class ObjectStore:
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket_name: str, tokenizer: DocumentTokenizer):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False  # Set to True for production with HTTPS
        )
        self.bucket_name = bucket_name
        self.tokenizer = tokenizer # Task 3
        self.ensure_bucket_exists()

    def ensure_bucket_exists(self):
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
        except S3Error as e:
            raise Exception(f"Failed to initialize bucket: {e}")

    def generate_object_key(self, url: str, timestamp: str) -> str:
        """Generate a unique and organized object key."""
        url_hash = hashlib.sha256(url.encode()).hexdigest()[:12]
        date = datetime.now().strftime("%Y/%m/%d")
        return f"{date}/{url_hash}_{timestamp}.msgpack"

    def store_document(self, document: Dict[str, Any]) -> str:
        """Store a document and return its object key."""
        try:
            # Tokenize the content - Task 3
            tokenized_content = self.tokenizer.tokenize(document['content'])

            # Add tokenization to document
            document['tokenization'] = {
                'token_ids': tokenized_content['ids'],
                'tokens': tokenized_content['tokens'],
                'attention_mask': tokenized_content['attention_mask']
            }

            # Pack the document using msgpack - Task 2
            packed_data = msgpack.packb(document, use_bin_type=True)
            data_stream = io.BytesIO(packed_data)

            # Generate object key
            object_key = self.generate_object_key(
                document['metadata']['url'],
                document['timestamp']
            )

            # Upload to MinIO
            self.client.put_object(
                self.bucket_name,
                object_key,
                data_stream,
                length=len(packed_data),
                content_type='application/msgpack'
            )

            return object_key
        except S3Error as e:
            raise Exception(f"Failed to store document: {e}")