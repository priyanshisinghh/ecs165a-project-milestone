from lstore.table import Table
from lstore.disk import DiskManager
import os
import json


#handles all of the tables in the system
class Database():
    def __init__(self):
        self.table_count = 0
        self.tables = {}
        self.db_path = None
        self.disk = None

    #opens database from disk and reloads any previously saved tables
    def open(self, path):
        self.db_path = path
        self.disk = DiskManager(path)
        os.makedirs(path, exist_ok = True)
        meta_path = os.path.join(path, 'db_meta.json')
        if os.path.exists(meta_path):
            with open(meta_path, 'r') as f:
                meta = json.load(f)
            for t in meta['tables']:
                table = Table(t['name'], t['num_columns'], t['key'], self.disk)
                table.load_from_disk(path)
                self.tables[t['name']] = table
                self.table_count += 1

    #flushes all dirty pages and saves table metadata so open() can restore them
    def close(self):
        if self.db_path is None:
            return
        meta = {'tables': []}
        for name, table in self.tables.items():
            table.save_to_disk(self.db_path)
            meta['tables'].append({'name': table.name, 'num_columns': table.num_columns, 'key': table.key})
        with open(os.path.join(self.db_path, 'db_meta.json'), 'w') as f:
            json.dump(meta, f)

    def create_table(self, name, num_columns, key_index):
        #pass disk manager into Table so bufferpool can write to disk
        if self.disk is not None:
            disk = self.disk
        else:
            disk = DiskManager('./db_files')
        table = Table(name, num_columns, key_index, disk)
        self.tables[name] = table
        self.table_count += 1
        return table

    def drop_table(self, name):
        if name in self.tables:
            if self.disk is not None:
                self.disk.delete_table(name)
            del self.tables[name]
            self.table_count -= 1

    def get_table(self, name):
        for table in self.tables:
            if table == name:
                return self.tables[name]
