import os
import time
import uuid
from pymilvusdm.core.data_to_milvus import DataToMilvus
from pymilvusdm.core.read_faiss_data import ReadFaissData
from pymilvusdm.setting import WORK_PATH


class FaissToMilvus:
    def __init__(self, client, logger, mode, dest_collection_name, collection_parameter, dest_partition_name=None):
        self.client = client
        self.logger = logger
        # self.data_path = data_path
        self.mode = mode
        self.dest_collection_name = dest_collection_name
        self.dest_partition_name = dest_partition_name
        self.collection_parameter = collection_parameter

    def save_ids(self, file_path, ids):
        _, filename = os.path.split(file_path)
        filename, _ = os.path.splitext(filename)
        filename = filename + '_' + str(int(round(time.time() * 10000000))) + '.txt'
        data_dir = os.path.join(WORK_PATH, 'ids')
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        with open(os.path.join(data_dir, filename), 'w') as f:
            f.write('\n'.join(str(id) for id in ids))

    def faiss_to_milvus(self, data_path):
        if self.mode in ('append', 'overwrite', 'skip'):
            faiss_data = ReadFaissData(data_path, self.logger)
            # print(data_path)
            ids, vectors = faiss_data.read_faiss_data()
            insert_milvus = DataToMilvus(self.logger, self.client)
            ids = insert_milvus.insert_data(vectors, self.dest_collection_name, self.collection_parameter,
                                            self.mode, ids, self.dest_partition_name)
            self.save_ids(data_path, ids)
            self.logger.info('Data migration successful from Faiss to Milvus: {}'.format(os.path.split(data_path)[1]))
        else:
            self.logger.error('error mode type, only support [skip, append, overwrite]')
