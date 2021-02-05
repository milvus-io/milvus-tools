import yaml
import os
import uuid
import sys

# sys.path.append("..")
from pymilvusdm.hdf5_to_milvus import *
from pymilvusdm.core.milvus_client import MilvusIndex
from pymilvusdm.core.write_logs import write_log
from pymilvusdm.faiss_to_milvus import FaissToMilvus
from pymilvusdm.core.read_milvus_data import ReadMilvusDB
from pymilvusdm.core.read_milvus_meta import ReadMilvusMeta
from pymilvusdm.milvus_to_milvus import MilvusToMilvus
from pymilvusdm.milvus_to_hdf5 import MilvusToHDF5
from pymilvusdm.core.save_data import SaveData
from tqdm import tqdm


def read_yaml(yaml_path):
    with open(yaml_path, 'r') as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)
    mode = list(config.keys())[0]
    return mode, config[mode]


def hdf2mil(config, logger):
    if config['data_path'] and config['data_dir']:
        raise Exception("Please just config data_path or data_dir, while the other one is None.")
    mode = config['mode']
    client = MilvusIndex(logger, config['dest_host'], config['dest_port'])
    if mode == 'skip':
        if client.has_collection(config['dest_collection_name']):
            logger.info(
                'Already exists collection {} in Milvus, skip importing data!'.format(config['dest_collection_name']))
            sys.exit(0)
    if config['data_path']:
        fbar = tqdm(config['data_path'])
        for path in fbar:
            loader = HDF5toMilvus(client, logger, mode, path, config['dest_collection_name'],
                                  config['collection_parameter'], config['dest_partition_name'])
            loader.tomilvus()
            mode = 'append'
        logger.info('Migrate HDF5 data to Milvus Successfully: {}'.format(config['data_path']))
    elif config['data_dir']:
        fbar = tqdm(os.listdir(config['data_dir']))
        if config['data_dir'][-1] != os.sep:
            config['data_dir'] = config['data_dir'] + os.sep
        for filepath in fbar:
            if os.path.splitext(filepath)[-1] != ".h5":
                logger.debug('{} is not hdf5 file'.format(filepath))
                continue
            full_path = config['data_dir'] + filepath
            loader = HDF5toMilvus(client, logger, mode, full_path, config['dest_collection_name'],
                                  config['collection_parameter'], config['dest_partition_name'])
            loader.tomilvus()
            mode = 'append'
        logger.info('Migrate HDF5 data to Milvus Successfully: {}'.format(config['data_dir']))


def fai2mil(config, logger):
    try:
        if not (config['data_path'] and config['dest_host'] and config['dest_port'] and config['mode']
                and config['dest_collection_name'] and config['collection_parameter']):
            raise Exception("Please config data_path")
        # logger = write_log()
        client = MilvusIndex(logger, config['dest_host'], config['dest_port'])
        loader = FaissToMilvus(client, logger, config['mode'], config['dest_collection_name'],
                               config['collection_parameter'],
                               config['dest_partition_name'])
        loader.faiss_to_milvus(config['data_path'])
    except Exception as e:
        logger.error(e)
        sys.exit(1)


def mil2mil(config, logger):
    try:
        if not (config['source_milvus_path'] and config['source_collection'] and config['dest_host'] and config[
            'dest_port'] and config['mode']):
            raise Exception("Please configure the required parameters: {}".format(
                'source_milvus_path, source_collection, dest_host, dest_port, mode'))
        milvus_client = MilvusIndex(logger, config['dest_host'], config['dest_port'])
        milvusdb = ReadMilvusDB(logger, config['source_milvus_path'], config['mysql_parameter'])
        milvus_meta = ReadMilvusMeta(logger, config['source_milvus_path'], config['mysql_parameter'])
        milvus_insert = DataToMilvus(logger, milvus_client)

        m2m = MilvusToMilvus(logger, milvusdb, milvus_meta, milvus_insert, config['mode'])
        try:
            collection_name = list(config['source_collection'].keys())[0]         
        except Exception as e:
            logger.error("The collection name: {} must be a dic".format(config['source_collection']))
            sys.exit(1)
    
        m2m.transform_milvus_data(collection_name, config['source_collection'][collection_name])
    except Exception as e:
        logger.error('Milvus to Milvus Error with: {}'.format(e))
        sys.exit(1)


def mil2hdf(config, logger):
    try:
        if not (config['source_milvus_path'] and config['source_collection'] and config['data_dir']):
            raise Exception("Please configure the required parameters: {}".format(
                'source_milvus_path, source_collection or data_dir'))
        milvusdb = ReadMilvusDB(logger, config['source_milvus_path'], config['mysql_parameter'])
        milvus_meta = ReadMilvusMeta(logger, config['source_milvus_path'], config['mysql_parameter'])
        timestamp = str(uuid.uuid1())
        data_save = SaveData(logger, config['data_dir'], timestamp)

        m2f = MilvusToHDF5(logger, milvusdb, milvus_meta, data_save, config['source_milvus_path'], config['data_dir'],
                           config['mysql_parameter'])
        try:
            collection_name = list(config['source_collection'].keys())[0]         
        except Exception as e:
            logger.error("The collection name: {} must be a dic".format(config['source_collection']))
            sys.exit(1)
        
        m2f.read_milvus_data(collection_name, config['source_collection'][collection_name])
    except Exception as e:
        logger.error('Milvus to Milvus Error with: {}'.format(e))
        sys.exit(1)


def execute(yaml_path):
    try:
        logger = write_log()
        task, config = read_yaml(yaml_path)
        if task == 'H2M':
            hdf2mil(config, logger)
        elif task == 'F2M':
            fai2mil(config, logger)
        elif task == 'M2M':
            mil2mil(config, logger)
        elif task == 'M2H':
            mil2hdf(config, logger)
        else:
            raise Exception('error task type')
    except Exception as e:
        logger.error(e)