import h5py
import numpy as np
import uuid
import yaml
import os, sys

class SaveData:
    def __init__(self, logger, data_dir, timestamp):
        self.logger = logger
        self.data_dir = data_dir
        self.timestamp = timestamp
        self.dirs = self.data_dir + '/' + timestamp
        if not os.path.exists(self.dirs):
            os.makedirs(self.dirs)
        if not os.path.exists(self.dirs + '/yamls'):
            os.makedirs(self.dirs + '/yamls')
        

    def save_hdf5_data(self, collection_name, partition_tag, vectors, ids):
        hdf5_filename = self.dirs + '/' + collection_name + '_' + str(partition_tag) + '.h5'
        try:
            f = h5py.File(hdf5_filename, 'w')
            if type(vectors[0]) == type(b'a'):
                v = []
                for i in vectors:
                    v.append(list(i))
                data = np.array(v, dtype=np.uint8) #save np.array and dtype=uint8
            else:
                data = np.array(vectors)

            f.create_dataset(name='embeddings', data=data) 
            f.create_dataset(name='ids', data=ids)
            self.logger.debug('Successfully saved data of collection: {}/partition: {} data in {}!'.format(collection_name, partition_tag, hdf5_filename))
            return hdf5_filename        
        except Exception as e:
            self.logger.error("Error with {}".format(e))
            sys.exit(1)

    def save_yaml(self, collection_name, partition_tag, collection_parameter, version, save_hdf5_name):
        try:
            hdf2_ymal = {
                'H2M':{
                    'milvus_version': version,
                    'data_path': [save_hdf5_name],
                    'data_dir': None,
                    'dest_host': '127.0.0.1',
                    'dest_port': 19530,
                    'mode': 'skip',
                    'dest_collection_name': collection_name,
                    'dest_partition_name': partition_tag,
                    'collection_parameter': collection_parameter,
                }
            }
            yaml_filename = self.dirs + '/yamls/' + collection_name + '_' + str(partition_tag) + '.yaml'
            with open(yaml_filename, 'w') as f:
                f.write(yaml.dump(hdf2_ymal))
            self.logger.debug('Successfully saved yamls of collection: {}/partition: {} data in {}!'.format(collection_name, partition_tag, yaml_filename))
        except Exception as e:
            self.logger.error("Error with {}".format(e))
            sys.exit(1)