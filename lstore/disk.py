import os
import json
from lstore.page import Page, PAGE_SIZE


class DiskManager:
    """
    handles reading and writing pages to disk
    file layout:
        db_path/
            <table_name>/
                range_<id>/
                    base_col_<c>_page_<p>.dat
                    tail_col_<c>_page_<p>.dat
            <table_name>_tps.json
    """

    def __init__(self, db_path):
        self.db_path = db_path
        os.makedirs(db_path, exist_ok = True)
        
        #layout of tps_cache: table_name -> { page_id_string -> tps_value }
        self.tps_cache = {}

    #page_id is a 5-tuple: (table_name, page_range_id, page_type, col_index, page_index)
    #gets/creates a path
    def get_path(self, page_id, create_dirs = False):
        table_name, page_range_id, page_type, col_index, page_index = page_id
        range_dir = os.path.join(self.db_path, table_name, f"range_{page_range_id}")
        if create_dirs:
            os.makedirs(range_dir, exist_ok = True)
        return os.path.join(range_dir, f"{page_type}_col_{col_index}_page_{page_index}.dat")

    #read a page's bytes
    def read_page(self, page_id):
        path = self.get_path(page_id)
        if not os.path.exists(path):
            return None
        with open(path, 'rb') as f:
            data = f.read()
        if len(data) < PAGE_SIZE:
            return None
        return data[:PAGE_SIZE]

    #writes PAGE_SIZE bytes to disk
    def write_page(self, page_id, page_data):
        path = self.get_path(page_id, create_dirs = True)
        with open(path, 'wb') as f:
            f.write(page_data)

    #delete a table from disk
    def delete_table(self, table_name):
        import shutil
        table_dir = os.path.join(self.db_path, table_name)
        if os.path.exists(table_dir):
            shutil.rmtree(table_dir)
        tps_path = os.path.join(self.db_path, f"{table_name}_tps.json")
        if os.path.exists(tps_path):
            os.remove(tps_path)
        self.tps_cache.pop(table_name, None)

    #check if table exists
    def table_exists(self, table_name):
        return os.path.exists(os.path.join(self.db_path, table_name))

    #store tps
    def tps_path(self, table_name):
        return os.path.join(self.db_path, f"{table_name}_tps.json")

    #load tps from disk
    def load_tps(self, table_name):
        if table_name in self.tps_cache:
            return
        path = self.tps_path(table_name)
        if os.path.exists(path):
            with open(path, 'r') as f:
                raw = json.load(f)
            self.tps_cache[table_name] = {k: int(v) for k, v in raw.items()}
        else:
            self.tps_cache[table_name] = {}

    #save tps to disk
    def save_tps(self, table_name):
        path = self.tps_path(table_name)
        with open(path, 'w') as f:
            json.dump(self.tps_cache[table_name], f)

    #read a table's tps
    def read_tps(self, page_id):
        table_name = page_id[0]
        self.load_tps(table_name)
        return self.tps_cache[table_name].get(str(page_id), None)

    #write tps
    def write_tps(self, page_id, tps_value):
        table_name = page_id[0]
        self.load_tps(table_name)
        self.tps_cache[table_name][str(page_id)] = tps_value
        self.save_tps(table_name)
