# Task 1

import time
import requests
from datetime import datetime
import os

def clear_terminal():
    os.system('cls' if os.name == 'nt' else 'clear')

def get_metrics(port):
    try:
        response = requests.get(f'http://localhost:{port}/metrics')
        return response.text
    except:
        return "Cannot connect to metrics endpoint"

def parse_metric_value(metrics_text, metric_name):
    for line in metrics_text.split('\n'):
        if line.startswith(metric_name):
            return float(line.split()[1])
    return 0

def display_metrics():
    while True:
        clear_terminal()

        # Get metrics from both services
        batcher_metrics = get_metrics(9000)
        worker_metrics = get_metrics(9001)

        print("=" * 50)
        print(f"Common Crawl Pipeline Metrics - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)

        # Batcher metrics
        print("\n🔄 BATCHER METRICS:")
        print("-" * 30)
        processed_lines = parse_metric_value(batcher_metrics, 'batcher_processed_lines_total')
        english_filtered = parse_metric_value(batcher_metrics, 'batcher_english_filtered_total')
        status_filtered = parse_metric_value(batcher_metrics, 'batcher_status_filtered_total')
        valid_urls = parse_metric_value(batcher_metrics, 'batcher_valid_urls_total')
        batches_published = parse_metric_value(batcher_metrics, 'batcher_batches_published_total')

        print(f"Processed Lines: {int(processed_lines):,}")
        print(f"English Filtered: {int(english_filtered):,}")
        print(f"Status Filtered: {int(status_filtered):,}")
        print(f"Valid URLs: {int(valid_urls):,}")
        print(f"Batches Published: {int(batches_published):,}")

        if processed_lines > 0:
            success_rate = (valid_urls / processed_lines) * 100
            print(f"Success Rate: {success_rate:.2f}%")

        # Worker metrics
        print("\n👷 WORKER METRICS:")
        print("-" * 30)
        processed_batches = parse_metric_value(worker_metrics, 'worker_processed_batches_total')
        processed_docs = parse_metric_value(worker_metrics, 'worker_processed_documents_total')
        failed_docs = parse_metric_value(worker_metrics, 'worker_failed_documents_total')
        download_bytes = parse_metric_value(worker_metrics, 'worker_download_bytes_total')

        print(f"Processed Batches: {int(processed_batches):,}")
        print(f"Processed Documents: {int(processed_docs):,}")
        print(f"Failed Documents: {int(failed_docs):,}")
        print(f"Downloaded Data: {download_bytes/1024/1024:.2f} MB")

        if processed_docs > 0:
            error_rate = (failed_docs / processed_docs) * 100
            print(f"Error Rate: {error_rate:.2f}%")

        print("\nPress Ctrl+C to exit")
        time.sleep(2)  # Update every 2 seconds

if __name__ == "__main__":
    try:
        display_metrics()
    except KeyboardInterrupt:
        print("\nMonitoring stopped")