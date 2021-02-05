from pymilvusdm.core.milvus_client import MilvusIndex
import math
import time
import sys


class DataToMilvus:
    def __init__(self, logger, client):
        self.client = client
        self.logger = logger
        self.batch_size = 100000

    def creat_none_partition(self, collection, collection_param, mode, has_collection):
        _continue = True
        if not has_collection:
            self.client.create_collection(collection, collection_param)
        else:
            if mode == 'overwrite':
                self.client.drop_collection(collection)
                while has_collection:
                    has_collection = self.client.has_collection(collection)
                self.client.create_collection(collection, collection_param)
            elif mode == 'skip':
                _continue = False
        return _continue

    def creat_assigned_partition(self, collection, collection_param, mode, partition, has_collection):
        _continue = True
        if not has_collection:
            self.client.create_collection(collection, collection_param)
            self.client.creat_partition(collection, partition)
        else:
            has_partition = self.client.has_partition(collection, partition)
            if not has_partition:
                self.client.creat_partition(collection, partition)
            else:
                if mode == 'overwrite':
                    self.client.drop_partition(collection, partition)
                    while has_partition:
                        has_partition = self.client.has_partition(collection, partition)
                    self.client.creat_partition(collection, partition)
                elif mode == 'skip':
                    _continue = False
        return _continue

    def insert_data(self, vectors, collection, collection_param_1, mode, ids=None, partition=None):
        try:
            metric_type = self.client.get_metric_type(collection_param_1)
            collection_param = {'collection_name': collection, 'dimension': collection_param_1['dimension'],
                                'index_file_size': collection_param_1['index_file_size'], 'metric_type': metric_type}
            has_collection = self.client.has_collection(collection)
            if partition:
                _continue = self.creat_assigned_partition(collection, collection_param, mode, partition, has_collection)
            else:
                _continue = self.creat_none_partition(collection, collection_param, mode, has_collection)
            return_ids = []
            if _continue:
                for i in range(math.ceil(len(vectors) / self.batch_size)):
                    _ids = None
                    if ids:
                        _ids = ids[i * self.batch_size: (i + 1) * self.batch_size]
                    vector = vectors[i * self.batch_size: (i + 1) * self.batch_size]
                    status, ids_ = self.client.insert(collection, vector, _ids, partition)
                    return_ids = return_ids + ids_

                if len(vectors) == len(return_ids):
                    self.logger.debug(
                        "Successfuly insert collection: {}/partition: {} , total num: {}".format(
                            collection, partition, len(return_ids)))
                else:
                    raise Exception(
                        "ERROR: The collection: {}/partition: {} data count is not equal!".format(collection,
                                                                                                  partition))
                    # self.logger.error(
                    #     "ERROR: The collection: {}/partition: {} data count is not equal!".format(collection,
                    #                                                                               partition))
            else:
                self.logger.info(
                    'The collection or partition exist,skip this collection: {}/partition: {} '.format(collection,
                                                                                                       partition))
            return return_ids
        except Exception as e:
            self.logger.error(e)
            sys.exit(2)
