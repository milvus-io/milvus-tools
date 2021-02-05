import struct
import numpy as np
import sys
import os


def read_int32(f):
    return struct.unpack("i", f.read(4))


def read_int64(f):
    return struct.unpack("Q", f.read(8))


def read_float32(f):
    return struct.unpack("f", f.read(4))


def read_muilt_float32(f, num):
    return struct.unpack(num * "f", f.read(num * 4))


def read_muilt_int64(f, num):
    return struct.unpack(num * "Q", f.read(num * 8))


def skip_bytes(f, num):
    f.read(num)


class ReadFaissData:
    def __init__(self, filename, logger):
        self.filename = filename
        self.logger = logger

    def read_index_header(self, f):
        try:
            index_info = {}
            index_info['dim'] = read_int32(f)[0]
            index_info['ntotal'] = read_int64(f)[0]
            skip_bytes(f, 16)
            index_info['is_traind'] = struct.unpack("?", f.read(1))[0]
            index_info['MetricType'] = read_int32(f)[0]
            self.logger.debug('faiss data index info: {}'.format(index_info))
            return index_info
        except Exception as e:
            self.logger.error('read index header error: {}'.format(e))
            sys.exit(1)

    def read_flat_data(self, f):
        index_header = self.read_index_header(f)
        try:
            data_length = read_int64(f)[0]
            vectors = np.array(read_muilt_float32(f, data_length))
            vectors = vectors.reshape((index_header['ntotal'], index_header['dim']))
            ids = np.arange(index_header['ntotal'])
            return ids, vectors
        except Exception as e:
            self.logger.error('read faiss data error: {}'.format(e))
            sys.exit(1)

    def read_ivf_flat_data(self, f):
        index_header = self.read_index_header(f)
        try:
            index_header['nlist'] = read_int64(f)[0]
            skip_bytes(f, 45)
            data_len = read_int64(f)[0]
            skip_bytes(f, data_len * 4)
        except Exception as e:
            self.logger.error('read faiss ivf data error: {}'.format(e))
            sys.exit(1)
        try:
            direct_map = struct.unpack('B', f.read(1))[0]
            if direct_map == 2:
                size = read_int64(f)[0]
                skip_bytes(f, size * 16)
            dm_array_size = read_int64(f)[0]
            if dm_array_size:
                read_muilt_int64(f, dm_array_size)
        except Exception as e:
            self.logger.error('read faiss ivf data error: {}, direct_map: {}'.format(e,direct_map))
            sys.exit(1)
        header = f.read(4).decode()
        self.logger.debug('direct_map: {}, dm_array_size: {},type: {}'.format(direct_map, dm_array_size, header))
        vectors = []
        ids = []
        try:
            if header == 'ilar':
                nlist = read_int64(f)[0]
                code_size = read_int64(f)[0]
                list_type = f.read(4).decode()
                self.logger.debug('Clustering Type: {}'.format(list_type))
                # print(list_type)
                if list_type == 'full':
                    nlist = read_int64(f)[0]
                    nlist_size = struct.unpack(nlist * "Q", f.read(8 * nlist))
                    self.logger.debug('Clustering Size: {}'.format(nlist_size))
                    for size in nlist_size:
                        vector = list(read_muilt_float32(f, size * index_header['dim']))
                        vectors = vectors + vector
                        _id = list(read_muilt_int64(f, size))
                        ids = ids + _id
                    vectors = np.array(vectors)
                    vectors = vectors.reshape((index_header['ntotal'], index_header['dim']))
                    ids = np.array(ids)
                    index_sort = ids.argsort()
                    ids = ids[index_sort]
                    vectors = vectors[index_sort]
                    return ids, vectors
                elif list_type == 'sprs':
                    nlist = read_int64(f)[0]
                    nlist_size = struct.unpack(nlist * "Q", f.read(8 * nlist))
                    self.logger.debug('Clustering Size: {}'.format(nlist_size))
                    # print(nlist_size)
                    for size in nlist_size[1::2]:
                        vector = list(read_muilt_float32(f, size * index_header['dim']))
                        vectors = vectors + vector
                        _id = list(read_muilt_int64(f, size))
                        ids = ids + _id
                    vectors = np.array(vectors)
                    vectors = vectors.reshape((index_header['ntotal'], index_header['dim']))
                    ids = np.array(ids)
                    index_sort = ids.argsort()
                    ids = ids[index_sort]
                    vectors = vectors[index_sort]
                    return ids, vectors
                else:
                    raise Exception('read faiss data error, Clustering Type: {}'.format(list_type))
            else:
                raise Exception('read faiss data error: {}'.format(header))
        except Exception as e:
            self.logger.error('read faiss ivf data error: {}'.format(e))
            sys.exit(1)

    def read_faiss_data(self):
        try:
            f = open(self.filename, 'rb+')
            header = f.read(4).decode()
            self.logger.debug("faiss data header: {}".format(header))
            if header in ("IxFI", "IxF2", "IxFl"):
                ids, vectors = self.read_flat_data(f)
                return ids.tolist(), vectors.tolist()
            elif header == "IwFl":
                ids, vectors = self.read_ivf_flat_data(f)
                return ids.tolist(), vectors.tolist()
            else:
                raise Exception('pymilvusdm only supports faiss flat and ivf_flat index files')
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)
