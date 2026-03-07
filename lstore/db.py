import os
import json
import threading
from lstore.table import Table
from lstore.disk import DiskManager
from lstore.lock_manager import LockManager


class Database:
    registry = {}
    registry_lock = threading.Lock()

    #return lock manager for the given path, for transaction worker to find database lock
    def get_lock_manager(cls, db_path = None):
        with cls.registry_lock:
            if db_path is not None:
                return cls.registry.get(db_path)
            #fall back to most recently registered
            #if cls.registry:
            #   return next(reversed(cls._registry.values()))
            #return None

    def __init__(self):
        self.table_count = 0
        self.tables = {}
        self.db_path = None
        self.disk = None
        self.lock_manager = LockManager()
        self.tables_lock = threading.Lock()

    #opens database from disk and reloads any previously saved tables
    def open(self, path):
        self.db_path = path
        self.disk = DiskManager(path)
        os.makedirs(path, exist_ok = True)

        #register this database's lock_manager so transaction worker can find it
        with Database.registry_lock:
            Database.registry[path] = self.lock_manager

        meta_path = os.path.join(path, 'db_meta.json')
        if os.path.exists(meta_path):
            with open(meta_path, 'r') as f:
                meta = json.load(f)
            for t in meta['tables']:
                table = Table(t['name'], t['num_columns'], t['key'], self.disk)
                table.load_from_disk(path)
                with self.tables_lock:
                    self.tables[t['name']] = table
                    self.table_count += 1

    #flushes all dirty pages and saves table metadata so open() can restore them
    def close(self):
        if self.db_path is None:
            return

        #unregisterlock when the database closes
        with Database._registry_lock:
            Database._registry.pop(self.db_path, None)

        meta = {'tables': []}
        with self.tables_lock:
            for name, table in self.tables.items():
                table.save_to_disk(self.db_path)
                meta['tables'].append({
                    'name': table.name,
                    'num_columns': table.num_columns,
                    'key': table.key
                })

        with open(os.path.join(self.db_path, 'db_meta.json'), 'w') as f:
            json.dump(meta, f)

    def create_table(self, name, num_columns, key_index):
        #pass disk manager into Table so bufferpool can write to disk
        if self.disk is not None:
            disk = self.disk
        else:
            disk = DiskManager('./db_files')

        table = Table(name, num_columns, key_index, disk)
        with self.tables_lock:
            self.tables[name] = table
            self.table_count += 1
        return table

    def drop_table(self, name):
        with self.tables_lock:
            if name not in self.tables:
                return
            if self.disk is not None:
                self.disk.delete_table(name)
            del self.tables[name]
            self.table_count -= 1

    def get_table(self, name):
        with self.tables_lock:
            return self.tables.get(name, None)
