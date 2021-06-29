from pymilvus_orm import connections
from pymilvus_orm.types import DataType
from pymilvus_orm.schema import FieldSchema, CollectionSchema
from pymilvus_orm.collection import Collection
from pymilvus_orm import utility
import time
import sys


class MilvusIndex:
    def __init__(self, logger, milvus_host='127.0.0.1', milvus_port=19530):
        self.logger = logger
        self.host = milvus_host
        self.port = milvus_port
        self.collection = None
        connections.connect(host=self.host, port=self.port)

    def set_collection(self, collection_name):
        try:
            if self.has_collection(collection_name):
                self.collection = Collection(name=collection_name)
            else:
                raise Exception("There has no collection named:{}".format(collection_name))
        except Exception as e:
            self.logger.error("Failed to load data to Milvus: {}".format(e))
            sys.exit(1)

    def has_collection(self, collection_name):
        try:
            return utility.has_collection(collection_name)
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

    def create_collection(self, collection_name, collection_param):
        try:
            if not self.has_collection(collection_name):
                field1 = FieldSchema(name="id", dtype=DataType.INT64, descrition="int64", is_primary=True, auto_id=False)
                field2 = FieldSchema(name="embedding", dtype=collection_param['data_type'], descrition="float vector", dim=collection_param['dimension'], is_primary=False)
                schema = CollectionSchema(fields=[field1,field2], description="collection description")
                self.collection = Collection(name=collection_name, schema=schema)   
                self.logger.debug("Create Milvus collection: {}".format(self.collection))
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

    def has_partition(self, collection_name, partition_name):
        try:
            self.set_collection(collection_name)
            return self.collection.has_partition(partition_name)
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

    def creat_partition(self, collection_name, partition_name):
        try:
            self.set_collection(collection_name)
            self.collection.create_partition(partition_name)
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

    def drop_collection(self, collection_name):
        try:
            self.set_collection(collection_name)
            self.collection.drop()
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

    def drop_partition(self, collection_name, partition_name):
        try:
            self.set_collection(collection_name)
            self.collection.drop_partition(partition_name)
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

    def insert(self, collection_name, vectors, ids, partition_name=None):
        try:
            self.set_collection(collection_name)
            data = [ids, vectors]
            mr = self.collection.insert(data=data, partition_name=partition_name)
            return "Insert success!", mr.primary_keys
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

    def count_entities(self, collection_name):
        try:
            self.set_collection(collection_name)
            num = self.collection.num_entities
            return num
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

    def get_data_type(self, collection_param):
        try:
            if collection_param['metric_type'] == 'IP':
                data_type = DataType.FLOAT_VECTOR
            elif collection_param['metric_type'] == 'L2':
                data_type = DataType.FLOAT_VECTOR
            elif collection_param['metric_type'] == 'HAMMING':
                data_type = DataType.BINARY_VECTOR
            elif collection_param['metric_type'] == 'JACCARD':
                data_type = DataType.BINARY_VECTOR
            elif collection_param['metric_type'] == 'TANIMOTO':
                data_type = DataType.BINARY_VECTOR
            elif collection_param['metric_type'] == 'SUBSTRUCTURE':
                data_type = DataType.BINARY_VECTOR
            elif collection_param['metric_type'] == 'SUPERSTRUCTURE':
                data_type = DataType.BINARY_VECTOR
            else:
                raise Exception("metric_type error: {}".format(collection_param['metric_type']))
            return data_type
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)
