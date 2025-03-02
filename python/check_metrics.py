# Task 1

import requests

def print_metric_summary():
    # Check batcher metrics
    try:
        batcher_response = requests.get('http://localhost:9000/metrics')
        batcher_metrics = batcher_response.text

        print("BATCHER METRICS SUMMARY:")
        for line in batcher_metrics.split('\n'):
            if line.startswith('batcher_') and not line.startswith('#'):
                print(line)
    except:
        print("Could not connect to batcher metrics endpoint")

    print("\n" + "="*50 + "\n")

    # Check worker metrics
    try:
        worker_response = requests.get('http://localhost:9001/metrics')
        worker_metrics = worker_response.text

        print("WORKER METRICS SUMMARY:")
        for line in worker_metrics.split('\n'):
            if line.startswith('worker_') and not line.startswith('#'):
                print(line)
    except:
        print("Could not connect to worker metrics endpoint")

if __name__ == "__main__":
    print_metric_summary()