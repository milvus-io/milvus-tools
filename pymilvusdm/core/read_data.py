import h5py
import numpy as np


class ReadData:
    def __init__(self, filepath):
        self.filepath = filepath

    def unit2byte(self, vecs):
        vectors = [vec.tobytes() for vec in vecs]
        return vectors

    def format(self, vecs):
        if vecs.dtype == 'uint8':
            vecs = self.unit2byte(vecs)
        else:
            vecs = vecs.tolist()
        return vecs

    def read_hdf5_data(self):
        try:
            f = h5py.File(self.filepath, 'r')
            keys = list(f.keys())
            if 'embeddings' not in keys:
                raise Exception("Please make sure the file has the 'embeddings' group!")
            if 'ids' in keys:
                if f['ids'].len() != f['embeddings'].len():
                    raise Exception("Please ensure that the length of 'ids' equals that of the 'embeddings'!")
                # print(f['embeddings'][:].dtype == 'uint8')
                vectors = self.format(f['embeddings'][:])
                return vectors, f['ids'][:].tolist()
            return f['embeddings'][:].tolist(), None
        except Exception as e:
            print(e)
            return "Error with {}".format(e), 400
