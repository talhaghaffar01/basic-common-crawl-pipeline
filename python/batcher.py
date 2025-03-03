from abc import ABC, abstractmethod
import json
import argparse
from typing import Any, Mapping, Sequence
from prometheus_client import Counter, start_http_server, Gauge

from commoncrawl import (
    BASE_URL,
    get_crawl_path,
    CCDownloader,
    CSVIndexReader,
    Downloader,
    IndexReader,
)
from rabbitmq import QUEUE_NAME, MessageQueueChannel, RabbitMQChannel

# Metrics for batcher (Task 1)
BATCHER_METRICS = {
    'processed_lines': Counter('batcher_processed_lines_total', 'Total number of lines processed'),
    'english_filtered': Counter('batcher_english_filtered_total', 'Number of non-English documents filtered'),
    'status_filtered': Counter('batcher_status_filtered_total', 'Number of non-200 status documents filtered'),
    'valid_urls': Counter('batcher_valid_urls_total', 'Number of valid URLs found'),
    'batches_published': Counter('batcher_batches_published_total', 'Number of batches published to RabbitMQ'),
    'processing_progress': Gauge('batcher_processing_progress_bytes', 'Current processing progress in bytes'),
    #Task 7
    'failed_publishes': Counter('batcher_failed_publishes_total', 'Number of failed batch publishes to RabbitMQ')
}


BATCH_SIZE = 50

batch_counter = Counter("batcher_batches", "Number of published batches")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batcher")
    parser.add_argument(
        "--cluster-idx-filename", type=str, help="Input file path", required=True
    )
    parser.add_argument(
        "--crawl-version", type=str, default="CC-MAIN-2024-30",
        help="Common Crawl version (e.g., CC-MAIN-2024-30)"
    )
    return parser.parse_args()


def publish_batch(
    channel: MessageQueueChannel,
    batch: Sequence[Mapping[str, Any]],
) -> None:
    try:
        print("Pushing batch of size", len(batch))
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(batch),
        )
        batch_counter.inc()
        BATCHER_METRICS['batches_published'].inc()
    except Exception as e:
        print(f"Failed to publish batch after all retries: {e}")
        # Store failed batch for later retry or logging
        BATCHER_METRICS['failed_publishes'].inc()
        raise


# (task 1)
def process_index(
    index: IndexReader,
    channel: MessageQueueChannel,
    downloader: Downloader,
    batch_size: int,
) -> None:
    found_urls = []
    for cdx_chunk in index:
        data = downloader.download_and_unzip(
            cdx_chunk[1], int(cdx_chunk[2]), int(cdx_chunk[3])
        ).decode("utf-8")
        BATCHER_METRICS['processing_progress'].set(int(cdx_chunk[2]))

        for line in data.split("\n"):
            if line == "":
                continue
            BATCHER_METRICS['processed_lines'].inc()

            values = line.split(" ")
            metadata = json.loads("".join(values[2:]))

            # Track filtering metrics
            if "languages" not in metadata or "eng" not in metadata["languages"]:
                BATCHER_METRICS['english_filtered'].inc()
                continue

            if metadata["status"] != "200":
                BATCHER_METRICS['status_filtered'].inc()
                continue

            BATCHER_METRICS['valid_urls'].inc()
            found_urls.append(
                {
                    "surt_url": values[0],
                    "timestamp": values[1],
                    "metadata": metadata,
                }
            )

            if len(found_urls) >= batch_size:
                publish_batch(channel, found_urls)
                found_urls = []

    if len(found_urls) > 0:
        publish_batch(channel, found_urls)


def main() -> None:
    args = parse_args()
    start_http_server(9000)
    channel = RabbitMQChannel()
    crawl_path = get_crawl_path(args.crawl_version)
    downloader = CCDownloader(f"{BASE_URL}/{crawl_path}")
    index_reader = CSVIndexReader(args.cluster_idx_filename)

    process_index(index_reader, channel, downloader, BATCH_SIZE)


if __name__ == "__main__":
    main()
