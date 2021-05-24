import sys
from pymilvusdm.core.read_milvus_data import ReadMilvusDB
from pymilvusdm.core.read_milvus_meta import ReadMilvusMeta
from pymilvusdm.core.data_to_milvus import DataToMilvus
from pymilvusdm.setting import H2M_YAML
from pymilvusdm.core.milvus_client import MilvusIndex
from pymilvusdm.core.write_logs import write_log
from tqdm import tqdm

class MilvusToMilvus():
    def __init__(self, logger, milvusdb, milvus_meta, milvus_insert, mode):
        self.logger = logger
        self.milvusdb = milvusdb
        self.milvus_meta = milvus_meta
        self.milvus_insert = milvus_insert
        self.mode = mode

    def insert_collection_data(self, collection_name, partition_tags, collection_parameter):
        pbar = tqdm(partition_tags)
        for partition_tag in pbar:
            r_vectors, r_ids, r_rows = self.milvusdb.read_milvus_file(self.milvus_meta, collection_name, partition_tag)
            if r_rows == len(r_vectors) == len(r_ids)==0:
                self.logger.info('The collection: {}/partition: {} has no data'.format(collection_name, partition_tag))
            elif r_rows == len(r_vectors) == len(r_ids):
                self.milvus_insert.insert_data(r_vectors, collection_name, collection_parameter, self.mode, r_ids, partition_tag)
            else:
                self.logger.error("The collection: {}/partition: {} data count is not equal, data[meta_rows, read_vectors, read_ids] rows:{}!".format(collection_name, partition_tag, [r_rows, len(r_vectors), len(r_ids)]))

    def transform_milvus_data(self, collection_name, partition_tags):
        try:
            if not self.milvus_meta.has_collection_meta(collection_name):
                raise Exception("The source collection: {} does not exists.".format(collection_name))

            if not partition_tags:
                partition_tags = [None]
                partition_tags_meta = self.milvus_meta.get_all_partition_tag(collection_name)
                partition_tags += partition_tags_meta
            self.logger.info("Ready to transform all data of collection: {}/partitions: {}".format(collection_name, partition_tags))

            collection_parameter, _ = self.milvus_meta.get_collection_info(collection_name)
            self.insert_collection_data(collection_name, partition_tags, collection_parameter)
            self.logger.info("Successfully transformed all data.")
        except Exception as e:
            self.logger.error('Error with: {}'.format(e))
            sys.exit(1)

if __name__ == "__main__":
    # execute only if run as a script
    milvus_dir = '/Users/root/workspace/milvus10_mysql'
    mysql_p = {'host': '127.0.0.1', 'user': 'root', 'port': 3306, 'password': '123456', 'database': 'milvus'}      
    # collection_name = 'binary_example_collection'
    collection_name = 'folat_example_collection'
    partition_tags = None
    dest_host = '127.0.0.1'
    dest_port = 19530
    mode = 'append'

    logger = write_log()
    milvus_client = MilvusIndex(logger, dest_host, dest_port)
    milvusdb = ReadMilvusDB(logger, milvus_dir, mysql_p)
    milvus_meta = ReadMilvusMeta(logger, milvus_dir, mysql_p)
    milvus_insert = DataToMilvus(logger, milvus_client)

    m2m = MilvusToMilvus(logger, milvusdb, milvus_meta, milvus_insert, mode)
    m2m.transform_milvus_data(collection_name, partition_tags)