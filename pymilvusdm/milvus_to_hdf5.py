import sys
from pymilvusdm.core.read_milvus_data import ReadMilvusDB
from pymilvusdm.core.read_milvus_meta import ReadMilvusMeta
from pymilvusdm.core.save_data import SaveData
from pymilvusdm.core.write_logs import write_log
from pymilvusdm.setting import H2M_YAML
from tqdm import tqdm
from datetime import datetime
import os
import uuid


class MilvusToHDF5():
    def __init__(self, logger, milvusdb, milvus_meta, data_save, milvus_dir, data_dir, mysql_p=None):
        self.logger = logger
        self.milvusdb = milvusdb
        self.milvus_meta = milvus_meta
        self.data_save = data_save
        self.milvus_dir = milvus_dir
        self.data_dir = data_dir
        self.mysql_p = mysql_p

    def get_collection_data(self, collection_name, partition_tags, collection_parameter, version):
        pbar = tqdm(partition_tags)
        for partition_tag in pbar:
            r_vectors, r_ids, r_rows = self.milvusdb.read_milvus_file(self.milvus_meta, collection_name, partition_tag)
            if r_rows == len(r_vectors) == len(r_ids) == 0:
                self.logger.info('The collection: {}/partition: {} has no data.'.format(collection_name, partition_tag))
            elif r_rows == len(r_vectors) == len(r_ids):
                self.logger.debug(
                    "Saving the collection: {}/partition: {} data, total counts(rows, len(vectors), len(ids)) {}".format(
                        collection_name, partition_tag, [r_rows, len(r_vectors), len(r_ids)]))
                save_hdf5_name = self.data_save.save_hdf5_data(collection_name, partition_tag, r_vectors, r_ids)
                self.data_save.save_yaml(collection_name, partition_tag, collection_parameter, version, save_hdf5_name)
            else:
                self.logger.error(
                    "ERROR: The collection: {}/partition: {} data count is not equal!".format(collection_name,
                                                                                              partition_tag))

    def read_milvus_data(self, collection_name, partition_tags):
        try:
            if not self.milvus_meta.has_collection_meta(collection_name):
                raise Exception("The source collection: {} does not exists.".format(collection_name))

            if not partition_tags:
                partition_tags = [None]
                partition_tags_meta = self.milvus_meta.get_all_partition_tag(collection_name)
                partition_tags += partition_tags_meta
            self.logger.info(
                "Ready to read all data of collection: {}/partitions: {}".format(collection_name, partition_tags))

            collection_parameter, version = self.milvus_meta.get_collection_info(collection_name)
            self.get_collection_data(collection_name, partition_tags, collection_parameter, version)
            self.logger.info("Successfully copied all data.")
        except Exception as e:
            self.logger.error('Error with: {}'.format(e))
            sys.exit(1)


if __name__ == "__main__":
    # execute only if run as a script
    milvus_dir = '/Users/root/workspace/milvus10_mysql'
    data_dir = '/Users/root/workspace/test/milvusdm_data'
    mysql_p = {'host': '127.0.0.1', 'user': 'root', 'port': 3306, 'password': '123456', 'database': 'milvus'}

    # collection_name = 'binary_example_collection'
    collection_name = 'folat_example_collection'
    partition_tags = None

    logger = write_log()
    milvusdb = ReadMilvusDB(logger, milvus_dir, mysql_p)
    milvus_meta = ReadMilvusMeta(logger, milvus_dir, mysql_p)
    timestamp = str(uuid.uuid1())
    data_save = SaveData(logger, data_dir, timestamp)

    m2f = MilvusToHDF5(logger, milvusdb, milvus_meta, data_save, milvus_dir, data_dir, mysql_p)
    m2f.read_milvus_data(collection_name, partition_tags)
