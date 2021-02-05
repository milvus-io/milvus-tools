import pymysql
import sqlite3
import os, sys
from pymilvusdm.setting import MILVUS_TB, MILVUS_TBF, METRIC_DIC

class ReadMilvusMeta():
    def __init__(self, logger, milvus_dir, mysql_p=None):
        self.logger = logger
        self.conn = None
        self.cursor = None
        if mysql_p:
            self.connect_mysql(mysql_p['host'], mysql_p['user'], mysql_p['port'], mysql_p['password'], mysql_p['database'])
        else:
            self.connect_sqlite(milvus_dir + '/db')

    def connect_mysql(self, host, user, port, password, database):
        try:
            self.conn = pymysql.connect(host=host, user=user, port=port, password=password, database=database, local_infile=True)
            self.cursor = self.conn.cursor()
            self.logger.debug("Successfully connect mysql")
        except Exception as e:
            self.logger.error("MYSQL ERROR: connect failed with {}".format(e))
            sys.exit(1)

    def connect_sqlite(self, milvus_collection_path):
        try:
            self.conn = sqlite3.connect(milvus_collection_path + '/meta.sqlite')
            self.cursor = self.conn.cursor()
            self.logger.debug("Successfully connect sqlite")
        except Exception as e:
            self.logger.error("SQLite ERROR: connect failed with {}".format(e))
            sys.exit(1)

    def has_collection_meta(self, collection_name):
        sql = "select * from " + MILVUS_TB + " where table_id='" + collection_name + "';"
        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            if not results:
                return None
            return results
        except Exception as e:
            self.logger.error("META DATA ERROR: {} with sql: {}".format(e, sql))
            sys.exit(1)

    def get_all_partition_tag(self, collection_name):
        sql = "select partition_tag from " + MILVUS_TB + " where owner_table='" + collection_name + "';"
        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            if results:
                results = [re[0] for re in results]
            else:
                results = []
            self.logger.debug("Get all partition tag:{}".format(results))
            return results
        except Exception as e:
            self.logger.error("META DATA ERROR: {} with sql: {}".format(e, sql))
            sys.exit(1)

    def get_collection_info(self, collection_name):
        sql = "select dimension, index_file_size, metric_type, version from " + MILVUS_TB + " where table_id='" + collection_name + "';"
        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            collection_parameter = {
                "dimension": int(results[0][0]),
                "index_file_size": int(int(results[0][1])/1024/1024),
                "metric_type": METRIC_DIC[results[0][2]]
            }
            self.logger.debug("Get collection info(dimension, index_file_size, metric_type, version):{}".format(results))
            return collection_parameter, results[0][3]
        except Exception as e:
            self.logger.error("META DATA ERROR: {} with sql: {}".format(e, sql))
            sys.exit(1)

    def get_partition_name(self, collection_name, partition_tag):
        sql = "select table_id from " + MILVUS_TB + " where owner_table='" + collection_name + "' and partition_tag = '" + partition_tag + "';"
        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            if not results:
                raise Exception("The source collection: {}/ partition_tag: {} does not exists.".format(collection_name, partition_tag))
            self.logger.debug("Get partition name: {}".format(results))
            return results[0][0]
        except Exception as e:
            self.logger.error("META DATA ERROR: {} with sql: {}".format(e, sql))
            sys.exit(1)

    def get_collection_dim_type(self, table_id):
        sql = "select dimension, engine_type from " + MILVUS_TB + " where table_id='" + table_id + "';"
        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            self.logger.debug("Get meta data about dimension and types: {}".format(results))
            return results[0][0], results[0][1]
        except Exception as e:
            self.logger.error("META DATA ERROR: {} with sql: {}".format(e, sql))
            sys.exit(1)

    def get_collection_segments_rows(self, table_id):
        sql = "select segment_id, row_count from " + MILVUS_TBF + " where table_id='" + table_id + "' and file_type=1;"
        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            segments = [re[0] for re in results]
            rows = [re[1] for re in results]
            self.logger.debug("Get meta data about segment and rows: {}".format(results))
            return segments, rows
        except Exception as e:
            self.logger.error("META DATA ERROR: {} with sql: {}".format(e, sql))
            sys.exit(1)