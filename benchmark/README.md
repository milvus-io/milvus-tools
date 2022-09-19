Procedures

    Deploy a Milvus standalone or cluster. In this case, the IP address of the Milvus server is 10.100.31.105.

    Deploy a client. In this case, we use Ubuntu 18.04 and Python 3.8.13 for the deployment. Run the following code to install PyMilvus 2.1.1.

pip install pymilvus==2.1.1

    Download and copy the following files to the same working directory as the client. In this case, the working directory is /go_ben.

        collection_prepare.py

        go_benchmark.py

        benchmark (for Ubuntu) or benchmark-mac (for macOS)

    Note:

        benchmark and benchmark-mac are executable files developed and compiled using Go SDK 2.1.1. They are only used to conduct a concurrent search.

        For Ubuntu users, please download benchmark; for macOS users, please download benchmark-mac.

        Executable permissions are required to access benchmark or benchmark-mac.

        Mac users need to trust the benchmark-mac file by configuring Security & Privacy in System Preferences.

        Settings on concurrent search can be found and modified in the go_benchmark.py source code.

    Create a collection and insert vector data.

root@milvus-pytest:/go_ben# python collection_prepare.py 10.100.31.105 

    Open /tmp/collection_prepare.log to check the running result.

...
08/11/2022 17:33:34 PM - INFO - Build index costs 263.626
08/11/2022 17:33:54 PM - INFO - Collection prepared completed

    Call benchmark (or benchmark-mac on macOS) to conduct a concurrent search.

root@milvus-pytest:/go_ben# python go_benchmark.py 10.100.31.105 ./benchmark
[write_json_file] Remove file(search_vector_file.json).
[write_json_file] Write json file:search_vector_file.json done.
Params of go_benchmark: ['./benchmark', 'locust', '-u', '10.100.31.105:19530', '-q', 'search_vector_file.json', '-s', '{\n  "collection_name": "random_1m",\n  "partition_names": [],\n  "fieldName": "embedding",\n  "index_type": "HNSW",\n  "metric_type": "L2",\n  "params": {\n    "sp_value": 64,\n    "dim": 128\n  },\n  "limit": 1,\n  "expr": null,\n  "output_fields": [],\n  "timeout": 600\n}', '-p', '10', '-f', 'json', '-t', '60', '-i', '20', '-l', 'go_log_file.log']
[2022-08-11 11:37:39.811][    INFO] - Name      #   reqs      # fails  |       Avg       Min       Max    Median  |     req/s  failures/s (benchmark_run.go:212:sample)
[2022-08-11 11:37:39.811][    INFO] - go search     9665     0(0.00%)  |    20.679     6.499    81.761    12.810  |    483.25        0.00 (benchmark_run.go:213:sample)
[2022-08-11 11:37:59.811][    INFO] - Name      #   reqs      # fails  |       Avg       Min       Max    Median  |     req/s  failures/s (benchmark_run.go:212:sample)
[2022-08-11 11:37:59.811][    INFO] - go search    19448     0(0.00%)  |    20.443     6.549    78.121    13.401  |    489.22        0.00 (benchmark_run.go:213:sample)
[2022-08-11 11:38:19.811][    INFO] - Name      #   reqs      # fails  |       Avg       Min       Max    Median  |     req/s  failures/s (benchmark_run.go:212:sample)
[2022-08-11 11:38:19.811][    INFO] - go search    29170     0(0.00%)  |    20.568     6.398    76.887    12.828  |    486.15        0.00 (benchmark_run.go:213:sample)
[2022-08-11 11:38:19.811][   DEBUG] - go search run finished, parallel: 10(benchmark_run.go:95:benchmark)
[2022-08-11 11:38:19.811][    INFO] - Name      #   reqs      # fails  |       Avg       Min       Max    Median  |     req/s  failures/s (benchmark_run.go:159:samplingLoop)
[2022-08-11 11:38:19.811][    INFO] - go search    29180     0(0.00%)  |    20.560     6.398    81.761    13.014  |    486.25        0.00 (benchmark_run.go:160:samplingLoop)
Result of go_benchmark: {'response': True, 'err_code': 0, 'err_message': ''} 

    Open the go_log_file.log file under the current directory to check the detailed search log. The following is the search information you can find in the search log.

        reqs: number of search requests from the moment when concurrency happens to the current moment (the current time-span)

        fails: number of failed requests as a percentage of reqs in the current time-span

        Avg: average request response time in the current time-span (unit: milliseconds)

        Min: minimum request response time in the current time-span (unit: milliseconds)

        Max: maximum request response time in the current time-span (unit: milliseconds)

        Median: median request response time in the current time-span (unit: milliseconds)

        req/s: average request response time per second, i.e. QPS

        failures/s: average number of failed requests per second in the current time-span
