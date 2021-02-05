from milvus import *
import time
import sys


class MilvusIndex:
    def __init__(self, logger, milvus_host='127.0.0.1', milvus_port=19530):
        self.host = milvus_host
        self.port = milvus_port
        self.logger = logger
        self.client = self.milvus_client()

    def milvus_client(self):
        try:
            milvus = Milvus(self.host, self.port)
            return milvus
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

    def has_collection(self, collection_name):
        try:
            status, ok = self.client.has_collection(collection_name=collection_name)
            if status.code == 0:
                return ok
            else:
                raise Exception(status.message)
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

    def create_collection(self, collection_name, collection_param):
        try:
            # collection_param['collection_name'] = 'test1'
            status = self.client.create_collection(collection_param)
            if status.code != 0:
                raise Exception(status.message)
            self.logger.debug(status.message)
            # return status
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

    def has_partition(self, collection_name, partition_name):
        try:
            # collection_name = 'test123127'
            status, ok = self.client.has_partition(collection_name, partition_name)
            if status.code == 0:
                return ok
            else:
                raise Exception(status.message)
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

    def creat_partition(self, collection_name, partition_name):
        try:
            status = self.client.create_partition(collection_name, partition_name)
            if status.code != 0:
                raise Exception(status.message)
            self.logger.debug(status.message)
            # return status
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

    def drop_collection(self, collection_name):
        try:
            status = self.client.drop_collection(collection_name)
            if status.code != 0:
                raise Exception(status.message)
            self.logger.debug(status.message)
            # return status
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

    def drop_partition(self, collection_name, partition_name):
        try:
            status = self.client.drop_partition(collection_name, partition_name)
            if status.code != 0:
                raise Exception(status.message)
            self.logger.debug(status.message)
            # return status
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

    def insert(self, collection_name, vectors, ids=None, partition_name=None):
        try:
            status, ids = self.client.insert(collection_name, vectors, ids=ids, partition_tag=partition_name)
            # return status, ids
            if status.code == 0:
                return status, ids
            else:
                raise Exception(status.message)
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

    def count_entities(self, collection_name):
        try:
            status, rows = self.client.count_entities(collection_name)
            if status.code == 0:
                return rows
            else:
                raise Exception(status.message)
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

    def get_metric_type(self, collection_param):
        try:
            if collection_param['metric_type'] == 'IP':
                metric_type = MetricType.IP
            elif collection_param['metric_type'] == 'L2':
                metric_type = MetricType.L2
            elif collection_param['metric_type'] == 'HAMMING':
                metric_type = MetricType.HAMMING
            elif collection_param['metric_type'] == 'JACCARD':
                metric_type = MetricType.JACCARD
            elif collection_param['metric_type'] == 'TANIMOTO':
                metric_type = MetricType.TANIMOTO
            elif collection_param['metric_type'] == 'SUBSTRUCTURE':
                metric_type = MetricType.SUBSTRUCTURE
            elif collection_param['metric_type'] == 'SUPERSTRUCTURE':
                metric_type = MetricType.SUPERSTRUCTURE
            else:
                raise Exception("metric_type error: {}".format(collection_param['metric_type']))
            return metric_type
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

    # def create_index(user_id, client):
    #     param = {'nlist': 16384}
    #     try:
    #         status = client.create_index(user_id, IndexType.IVF_FLAT, param)
    #         return status
    #     except Exception as e:
    #         print("Milvus create index error:", e)

    # def milvus_search(client, vec, user_id):
    #     try:
    #         status, results = client.search(collection_name=user_id, query_records=vec, top_k=top_k, params=search_param)
    #         return status, results
    #     except Exception as e:
    #         print("Milvus search error:", e)
