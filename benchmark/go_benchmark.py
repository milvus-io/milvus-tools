import subprocess
import sys
import os
import random
import json

# Only the following indexes are supported
INDEX_MAP = {
    "flat": "",
    "ivf_flat": "IVF_FLAT",
    "ivf_sq8": "IVF_SQ8",
    "hnsw": "HNSW",
}


def run_process(params: list):
    process = subprocess.Popen(params, stderr=subprocess.PIPE)
    return process.communicate()[1].decode('utf-8')


def generate_vectors(nq, dim):
    return [[random.random() for _ in range(dim)] for _ in range(nq)]


def write_json_file(vectors, json_file_path=""):
    if not os.path.isfile(json_file_path):
        print("[write_json_file] File(%s) is not exist." % json_file_path)
        # os.mknod(json_file_path)
        open(json_file_path, "a").close()
    else:
        print("[write_json_file] Remove file(%s)." % json_file_path)
        os.remove(json_file_path)

    with open(json_file_path, "w") as f:
        json.dump(vectors, f)
    print("[write_json_file] Write json file:{0} done.".format(json_file_path))
    return json_file_path


def go_search(go_benchmark: str, uri: str, user: str, password: str, collection_name: str, search_params: dict,
              index_type: str, search_timeout: int, search_vector_file: str, concurrent_num, during_time, interval,
              log_path: str, output_format="json", partition_names=[], use_account=False):
    """
    :param go_benchmark: path to the go executable
    :param uri: milvus connection address host:port
    :param user: root user name
    :param password: root password
    :param collection_name: searched for collection name
    :param search_params: params of search
                        {"anns_field": str,  # field name to search
                         "metric_type": str,  # e.g. L2
                         "param": {
                            "sp_value": int,  # search params e.g. ef and nprobe
                            "dim": int,  # vector dimension
                            },
                         "limit": int,  # topk
                         "expression": str,  # search expression
                        }
    :param index_type: str
    :param search_timeout: str
    :param search_vector_file: path to vector json file
    :param concurrent_num: int
    :param during_time: concurrency lasts time / second
    :param interval: interval for printing statistics / second
    :param log_path: The log path to save the go print information
    :param output_format: default json
    :param partition_names: list
    :param use_account: bool
    :return:
        "result": {
            "response": bool,
            "err_code": int,
            "err_message": str
        }
    """
    query_json = {
        "collection_name": collection_name,
        "partition_names": partition_names,
        "fieldName": search_params["anns_field"],
        "index_type": INDEX_MAP[index_type],
        "metric_type": search_params["metric_type"],
        "params": search_params["param"],
        "limit": search_params["limit"],
        "expr": search_params["expression"],
        "output_fields": [],
        "timeout": search_timeout
    }

    go_search_params = [go_benchmark,  # path to the go executable
                        'locust',
                        '-u', uri,  # host:port
                        # '-n', user,  # root user name
                        # '-w', password,  # root password
                        '-q', search_vector_file,  # vector file path for searching
                        '-s', json.dumps(query_json, indent=2),
                        '-p', str(concurrent_num),  # concurrent number
                        '-f', output_format,  # format of output
                        '-t', str(during_time),  # total time of concurrent, second
                        '-i', str(interval),  # log print interval, second
                        '-l', str(log_path),  # log file path
                        ]
    if use_account is True:
        # connect used user and password
        go_search_params.extend(['-n', user, '-w', password])
        go_search_params.append('-v=true')

    print("Params of go_benchmark: {}".format(go_search_params))
    process_result = run_process(params=go_search_params)
    try:
        result = json.loads(process_result)
    except ValueError:
        msg = "The type of go_benchmark response is not json: {}".format(process_result)
        raise ValueError(msg)

    if isinstance(result, dict) and "response" in result and result["response"] is True:
        print("Result of go_benchmark: {}".format(result))
        return result
    else:
        raise Exception(result)


if __name__ == "__main__":
    host = sys.argv[1]                                          # host address
    go_benchmark = sys.argv[2]                                  # path to benchmark binary file  e.g. "/home/benchmark-mac"

    # parameters needed to be modified
    uri = f"{host}:19530"                                       # host and port of milvus
    user = ""                                                   # leave it empty if no user configured
    password = ""
    use_account = False                                         # False if no user configured

    nq = 1
    dim = 128                                                   # dim of vectors
    topk = 1
    ef = 64                                                     # search params
    collection_name = "random_1m"                               # collection name
    index_type = "hnsw"                                         # index type
    anns_field = "embedding"                                    # vector field name
    metric_type = "L2"
    expression = None

    search_timeout = 600                                        # search timeout in second
    search_vector_file = "search_vector_file.json"              # vectors in json file that would be searched
    concurrent_num = 10                                         # concurrent number of search
    during_time = 60                                            # search time in second
    interval = 20                                               # report interval in second
    log_path = "go_log_file.log"                                # path to log file
    output_format = "json"
    partition_names = []

    # prepare search vectors
    vectors = generate_vectors(nq, dim)
    write_json_file(vectors=vectors, json_file_path=search_vector_file)

    # prepare search parameters
    search_parameters = {
        "anns_field": anns_field,
        "metric_type": metric_type,
        "param": {
            "sp_value": ef,
            "dim": dim,
        },
        "limit": topk,
        "expression": expression,
    }

    # execute the go program
    go_search(go_benchmark=go_benchmark, uri=uri, user=user, password=password, collection_name=collection_name,
              search_params=search_parameters, index_type=index_type, search_timeout=search_timeout,
              search_vector_file=search_vector_file, concurrent_num=concurrent_num, during_time=during_time,
              interval=interval, log_path=log_path, output_format=output_format, partition_names=partition_names,
              use_account=use_account)
