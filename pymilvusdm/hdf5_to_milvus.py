# import sys
# sys.path.append('..')

from pymilvusdm.core.read_data import *
from pymilvusdm.core.data_to_milvus import DataToMilvus
from pymilvusdm.setting import IDS_PATH
import os
import uuid


class HDF5toMilvus():
    def __init__(self, client, logger, mode, filepath, dest_collection_name, collection_parameter,
                 dest_partition_name=None):
        self.client = client
        self.logger = logger
        self.mode = mode
        self.c_name = dest_collection_name
        self.c_param = collection_parameter
        self.p_name = dest_partition_name
        self.filepath = filepath
        self.file = ReadData(filepath)

    def generate_ids_file(self, ids):
        if not os.path.exists(IDS_PATH):
            os.makedirs(IDS_PATH)
        filename = os.path.split(self.filepath)[-1]
        filename = os.path.splitext(filename)[0]
        ids_file_path = IDS_PATH + os.sep + filename + '_' + str(uuid.uuid1()) + '.txt'
        with open(ids_file_path, 'w') as f:
            f.write('\n'.join(str(id) for id in ids))

    def tomilvus(self):
        try:
            if self.mode in ('append', 'overwrite', 'skip'):
                embeddings, ids = self.file.read_hdf5_data()
                insert_milvus = DataToMilvus(self.logger, self.client)
                ids = insert_milvus.insert_data(embeddings, self.c_name, self.c_param, self.mode, ids,
                                                self.p_name)
                self.generate_ids_file(ids)
            else:
                self.logger.error('error mode type, only support [skip, append, overwrite]')
        except Exception as e:
            print(e)
            return "Error with {}".format(e), 400
