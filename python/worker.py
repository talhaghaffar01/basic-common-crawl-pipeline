import io
import json
from prometheus_client import start_http_server, Counter, Histogram
import trafilatura
from warcio.archiveiterator import WARCIterator

from commoncrawl import BASE_URL, CCDownloader, Downloader
from rabbitmq import QUEUE_NAME, rabbitmq_channel


WORKER_METRICS = {
    'processed_batches': Counter('worker_processed_batches_total', 'Number of batches processed'),
    'processed_documents': Counter('worker_processed_documents_total', 'Number of documents processed'),
    'failed_documents': Counter('worker_failed_documents_total', 'Number of documents that failed processing'),
    'download_size': Counter('worker_download_bytes_total', 'Total bytes downloaded'),
    'processing_time': Histogram('worker_processing_seconds', 'Time spent processing documents',
                               buckets=[.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0]),
}

batch_counter = Counter("worker_batches", "Number of consumed batches")


def process_batch(downloader: Downloader, ch, method, _properties, body):
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
                            _text = trafilatura.extract(record.content_stream().read())
                            WORKER_METRICS['processed_documents'].inc()
                        except Exception:
                            WORKER_METRICS['failed_documents'].inc()

        except Exception:
            WORKER_METRICS['failed_documents'].inc()

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    start_http_server(9001)
    downloader = CCDownloader(BASE_URL)
    channel = rabbitmq_channel()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=lambda ch, method, properties, body: process_batch(
            downloader, ch, method, properties, body
        ),
    )
    channel.start_consuming()


if __name__ == "__main__":
    main()
