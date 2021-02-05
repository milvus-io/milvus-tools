import numpy as np
import os

# read data from one collection(_defult) or one partition
class ReadMilvusDB:
    def __init__(self, logger, milvus_dir, mysql_p=None):
        self.logger = logger
        self.milvus_dir = milvus_dir
        self.mysql_p = mysql_p

    def read_del_file(self, collection_path, segment_id):
        del_file = collection_path + '/' + segment_id + '/deleted_docs'
        del_ids = np.fromfile(del_file, dtype=np.int32)
        return del_ids

    def read_uid_file(self, collection_path, segment_id):
        uid_file = collection_path + '/' + segment_id + '/' + segment_id + '.uid'
        ids = np.fromfile(uid_file, dtype=np.int64)
        ids = ids[1:]
        return ids

    def read_rv_float_file(self, collection_path, segment_id, rows, dim):
        rv_file = collection_path + '/' + segment_id + '/' + segment_id + '.rv'
        self.logger.debug("Reading float data in local file: {}".format(rv_file))
        vectors = np.fromfile(rv_file, dtype=np.float32)
        vectors = vectors[2:].reshape(rows, dim).tolist()
        return vectors

    def read_rv_binary_file(self, collection_path, segment_id, rows, dim):
        rv_file = collection_path + '/' + segment_id + '/' + segment_id + '.rv'
        self.logger.debug("Reading binary data in local file: {}".format(rv_file))
        v = np.fromfile(rv_file, dtype=np.uint8)
        vectors = [vector.tobytes() for vector in v[8:].reshape(rows, int(dim/8))]
        return vectors

    def get_segment_data(self, collection_path, segment_id, dim, rows, types):
        del_ids = self.read_del_file(collection_path, segment_id)
        rows += len(del_ids[2:])
        if types in [1,2]:
            vectors = self.read_rv_float_file(collection_path, segment_id, rows, dim)
        else:
            vectors = self.read_rv_binary_file(collection_path, segment_id, rows, dim)   
        ids = self.read_uid_file(collection_path, segment_id)
        if len(del_ids[2:]) != 0:
            vectors = np.delete(vectors, del_ids[2:], axis=0).tolist()
            ids = np.delete(ids, del_ids[2:], axis=0)
            self.logger.debug('The Segment: {} has deleted data, total num: {}.'.format(segment_id, len(del_ids[2:])))

        # print(len(vectors), ids)
        return vectors, ids

    def get_files_data(self, table_id, collection_path, milvus_meta):
        dim, types = milvus_meta.get_collection_dim_type(table_id)
        segment_list, row_list = milvus_meta.get_collection_segments_rows(table_id)

        total_vectors = []
        total_ids = []
        total_rows = 0
        for segment_id, rows in zip(segment_list, row_list):
            total_rows += rows
            vectors, ids = self.get_segment_data(collection_path, segment_id, dim, rows, types)
            total_vectors += vectors
            total_ids += ids.tolist()
        return total_vectors, total_ids, total_rows

    def get_partition_data(self, milvus_meta, collection_name, partition_tag):
        partition_name = milvus_meta.get_partition_name(collection_name, partition_tag)
        collection_path = self.milvus_dir + '/db/tables/' + partition_name
        total_vectors, total_ids, total_rows = self.get_files_data(partition_name, collection_path, milvus_meta)
        return total_vectors, total_ids, total_rows

    def read_milvus_file(self, milvus_meta, collection_name, partition_tag):
        self.logger.debug("Reading milvus/db data from collection: {}/partition: {}".format(collection_name, partition_tag))
        total_rows = 0
        if partition_tag:
            total_vectors, total_ids, total_rows = self.get_partition_data(milvus_meta, collection_name, partition_tag)
        else:
            collection_path = self.milvus_dir + '/db/tables/' + collection_name
            total_vectors, total_ids, total_rows = self.get_files_data(collection_name, collection_path, milvus_meta)

        return total_vectors, total_ids, total_rows