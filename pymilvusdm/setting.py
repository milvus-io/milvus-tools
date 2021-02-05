import os

MILVUS_TB = "Tables"
MILVUS_TBF = "TableFiles"

METRIC_DIC = {
    1: "L2",
    2: "IP",
    3: "HAMMING",
    4: "JACCARD",
    5: "TANIMOTO",
    6: "SUBSTRUCTURE",
    7: "SUPERSTRUCTURE"
}

H2M_YAML = {
    'milvus-version': '0.10.5',
    'data_path': ['/home/data/data1.hdf5', '/home/data/fdata2.hdf5'],
    'dest_host': '127.0.0.1',
    'dest_port': 19530,
    'mode': 'append',
    'dest_collection_name': 'test02',
    'dest_partition_name': 'partition_01',
    'collection_parameter': {'dimension': 128, 'index_file_size': 1024, 'metric_type': 'IP'},
}

WORK_PATH = os.getenv("MILVUSDM_PATH", (os.path.join(os.environ['HOME'], 'milvusdm')))
IDS_PATH = WORK_PATH + os.sep + 'ids'
LOGS_NUM = os.getenv("logs_num", 0)
