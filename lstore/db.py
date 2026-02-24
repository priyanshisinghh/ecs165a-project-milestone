from lstore.table import Table
from lstore.disk import DiskManager
import os
import json


# handles all the tables in the system
class Database():
    def __init__(self):
        self.table_count = 0
        self.tables = {}
        self.db_path = None
        self.disk = None

    # new - opens db from disk, reloads any previously saved tables
    def open(self, path):
        self.db_path = path
        self.disk = DiskManager(path)
        os.makedirs(path, exist_ok=True)
        meta_path = os.path.join(path, 'db_meta.json')
        if os.path.exists(meta_path):
            with open(meta_path, 'r') as f:
                meta = json.load(f)
            for t in meta['tables']:
                table = Table(t['name'], t['num_columns'], t['key'], self.disk)
                table.load_from_disk(path)
                self.tables[t['name']] = table
                self.table_count += 1

    # new - flushes all dirty pages and saves table metadata so open() can restore them
    def close(self):
        if self.db_path is None:
            return
        meta = {'tables': []}
        for name, table in self.tables.items():
            table.save_to_disk(self.db_path)
            meta['tables'].append({'name': table.name, 'num_columns': table.num_columns, 'key': table.key})
        with open(os.path.join(self.db_path, 'db_meta.json'), 'w') as f:
            json.dump(meta, f)

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        # new - pass disk manager into Table so bufferpool can write to disk
        disk = self.disk if self.disk is not None else DiskManager('./db_files')
        table = Table(name, num_columns, key_index, disk)
        self.tables[name] = table
        self.table_count += 1
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        # new - fixed bug: was del self.tables[table] using object not name
        if name in self.tables:
            if self.disk is not None:
                self.disk.delete_table(name)
            del self.tables[name]
            self.table_count -= 1

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables:
            if table == name:
                return self.tables[name]
