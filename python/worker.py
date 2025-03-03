import io
import json
from prometheus_client import start_http_server, Counter, Histogram
import trafilatura
from warcio.archiveiterator import WARCIterator

import argparse
from typing import Optional, Dict, Any
from datetime import datetime

from commoncrawl import BASE_URL, CCDownloader, Downloader
from rabbitmq import QUEUE_NAME, rabbitmq_channel
from storage import ObjectStore
from tokenization import DocumentTokenizer
import os

MIN_DOCUMENT_LENGTH = 500
MAX_DOCUMENT_LENGTH = 1000000


WORKER_METRICS = {
    'processed_batches': Counter('worker_processed_batches_total', 'Number of batches processed'),
    'processed_documents': Counter('worker_processed_documents_total', 'Number of documents processed'),
    'failed_documents': Counter('worker_failed_documents_total', 'Number of documents that failed processing'),
    'download_size': Counter('worker_download_bytes_total', 'Total bytes downloaded'),
    'processing_time': Histogram('worker_processing_seconds', 'Time spent processing documents',
                               buckets=[.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0]),
    # Task 2
    'storage_errors': Counter('worker_storage_errors_total', 'Number of storage errors'),
    'stored_documents': Counter('worker_stored_documents_total', 'Number of documents stored successfully'),
    # Task 3
    'tokenization_errors': Counter('worker_tokenization_errors_total', 'Number of tokenization errors'),
    'tokens_processed': Counter('worker_tokens_processed_total', 'Total number of tokens processed'),
    # Task 5
    'documents_filtered_too_short': Counter('worker_documents_filtered_too_short_total',
                                          'Documents filtered due to being too short'),
    'documents_filtered_too_long': Counter('worker_documents_filtered_too_long_total',
                                         'Documents filtered due to being too long'),
    'document_length': Histogram('worker_document_length', 'Length of processed documents',
                               buckets=[100, 500, 1000, 10000, 100000, 1000000, 5000000])
}

batch_counter = Counter("worker_batches", "Number of consumed batches")

# Task 2
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Common Crawl Worker")
    parser.add_argument("--minio-endpoint", type=str, required=True,
                       help="MinIO endpoint (e.g., localhost:9000)")
    parser.add_argument("--minio-access-key", type=str, required=True,
                       help="MinIO access key")
    parser.add_argument("--minio-secret-key", type=str, required=True,
                       help="MinIO secret key")
    parser.add_argument("--minio-bucket", type=str, required=True,
                       help="MinIO bucket name")
    
    # Task 3
    parser.add_argument("--tokenizer-path", type=str, default="tokenizer.json",
                       help="Path to tokenizer model file")
    parser.add_argument("--train-tokenizer", action="store_true",
                       help="Train tokenizer if no model exists")
    
    # Task 5
    parser.add_argument("--min-length", type=int, default=MIN_DOCUMENT_LENGTH,
                       help="Minimum document length in characters")
    parser.add_argument("--max-length", type=int, default=MAX_DOCUMENT_LENGTH,
                       help="Maximum document length in characters")

    return parser.parse_args()

# Task 5
def check_document_length(text: str) -> bool:
    """Check if document length is within acceptable range."""
    length = len(text)
    WORKER_METRICS['document_length'].observe(length)

    if length < MIN_DOCUMENT_LENGTH:
        WORKER_METRICS['documents_filtered_too_short'].inc()
        print(f"Document filtered: too short ({length} chars)")
        return False

    if length > MAX_DOCUMENT_LENGTH:
        WORKER_METRICS['documents_filtered_too_long'].inc()
        print(f"Document filtered: too long ({length} chars)")
        return False

    return True

# Task 2
def process_document(text: str, metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Process and validate document content."""
    if not text or len(text.strip()) == 0:
        return None

    # Add length filtering - Task 5
    if not check_document_length(text.strip()):
        return None

    return {
        'content': text,
        'metadata': metadata,
        'timestamp': datetime.utcnow().isoformat(),
        'processing_info': {
            'processor_version': '1.0',
            'processing_date': datetime.utcnow().isoformat(),
            'content_length': len(text)  # Add content length to metadata - Task 5
        }
    }

def process_batch(downloader: Downloader, object_store: ObjectStore, ch, method, _properties, body):
    print("Received batch of size", len(body))
    batch = json.loads(body)
    WORKER_METRICS['processed_batches'].inc()

    for item in batch:
        try:
            data = downloader.download_and_unzip(
                item["metadata"]["filename"],
                int(item["metadata"]["offset"]),
                int(item["metadata"]["length"]),
            )
            WORKER_METRICS['download_size'].inc(len(data))

            for record in WARCIterator(io.BytesIO(data)):
                if record.rec_type == "response":
                    with WORKER_METRICS['processing_time'].time():
                        try:
                            text = trafilatura.extract(record.content_stream().read())

                            if text:
                                document = process_document(text, item["metadata"])
                                if document:
                                    object_key = object_store.store_document(document)
                                    WORKER_METRICS['stored_documents'].inc()
                                    print(f"Stored document: {object_key}")

                            WORKER_METRICS['processed_documents'].inc()
                        except Exception as e:
                            print(f"Failed to process document: {e}")
                            WORKER_METRICS['failed_documents'].inc()

        except Exception as e:
            print(f"Failed to process batch item: {e}")
            WORKER_METRICS['failed_documents'].inc()

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    args = parse_args()

    # Update length constants if provided via command line - Task 5
    global MIN_DOCUMENT_LENGTH, MAX_DOCUMENT_LENGTH
    MIN_DOCUMENT_LENGTH = args.min_length
    MAX_DOCUMENT_LENGTH = args.max_length

    print(f"Document length filters: min={MIN_DOCUMENT_LENGTH}, max={MAX_DOCUMENT_LENGTH}")

    # Initialize tokenizer - Task 3
    tokenizer = DocumentTokenizer()
    if args.train_tokenizer and not os.path.exists(args.tokenizer_path):
        print("Training new tokenizer...")
        # Collect some initial texts for training
        sample_texts = [
            "This is a sample text for tokenizer training.",
            "Another example of text that will help train the tokenizer.",
            "The more diverse the training data, the better the tokenizer.",
        ]
        tokenizer.train(sample_texts, args.tokenizer_path)
    else:
        print(f"Loading tokenizer from {args.tokenizer_path}")
        tokenizer.load(args.tokenizer_path)

    # Initialize MinIO client - Task 2
    object_store = ObjectStore(
        args.minio_endpoint,
        args.minio_access_key,
        args.minio_secret_key,
        args.minio_bucket,
        tokenizer,
    )
    start_http_server(9001)
    downloader = CCDownloader(BASE_URL)
    channel = rabbitmq_channel()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=lambda ch, method, properties, body: process_batch(
            downloader, object_store, ch, method, properties, body
        ),
    )

    print("Worker started. Waiting for messages...")
    channel.start_consuming()


if __name__ == "__main__":
    main()
