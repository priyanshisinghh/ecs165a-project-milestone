from lstore.page import Page, PAGE_SIZE
import os
import json


class DiskManager:
    """
    File layout:
        db_path/
            <table_name>/
                range_<id>/
                    base_col_<c>_page_<p>.dat   (PAGE_SIZE bytes)
                    tail_col_<c>_page_<p>.dat
            <table_name>_tps.json               (TPS metadata for all pages)
    """

    def __init__(self, db_path):
        self.db_path = db_path
        os.makedirs(db_path, exist_ok=True)

        # TPS metadata: table_name -> { page_id_string -> tps_value }
        # loaded once at startup, flushed on every write_tps call.
        self.tps_cache = {}
        self.tps_files = {}  # table_name -> file path

    # convert page id tuple to a file path
    # page id: (table_name, page_range_id, page_type, column_index, page_index)
    def get_page_file_path(self, page_id, create_dirs=False):
        table_name, page_range_id, page_type, column_index, page_index = page_id
        range_dir = os.path.join(self.db_path, table_name, f"range_{page_range_id}")

        if create_dirs:
            os.makedirs(range_dir, exist_ok=True)

        filename = f"{page_type}_col_{column_index}_page_{page_index}.dat"
        return os.path.join(range_dir, filename)

    # returns raw bytes of exactly PAGE_SIZE, or None if file doesn't exist
    def read_page(self, page_id):
        file_path = self.get_page_file_path(page_id, create_dirs=False)

        if not os.path.exists(file_path):
            return None

        with open(file_path, 'rb') as f:
            page_data = f.read()

        if len(page_data) < PAGE_SIZE:
            # partial or corrupt file, treat as missing so a fresh page is created
            return None

        return page_data[:PAGE_SIZE]

    # writes exactly PAGE_SIZE bytes to disk
    def write_page(self, page_id, page_data):
        if len(page_data) != PAGE_SIZE:
            raise Exception(
                f"Invalid page size when writing: got {len(page_data)}, expected {PAGE_SIZE}"
            )

        file_path = self.get_page_file_path(page_id, create_dirs=True)

        with open(file_path, 'wb') as f:
            f.write(page_data)

    def delete_page(self, page_id):
        file_path = self.get_page_file_path(page_id, create_dirs=False)
        if os.path.exists(file_path):
            os.remove(file_path)

    def delete_table(self, table_name):
        import shutil
        table_dir = os.path.join(self.db_path, table_name)
        if os.path.exists(table_dir):
            shutil.rmtree(table_dir)

        # remove TPS metadata file
        tps_path = self.tps_file_path(table_name)
        if os.path.exists(tps_path):
            os.remove(tps_path)

        # new - fixed attribute names (was _tps_cache and _tps_files with underscore, wrong name)
        self.tps_cache.pop(table_name, None)
        self.tps_files.pop(table_name, None)

    def table_exists(self, table_name):
        return os.path.exists(os.path.join(self.db_path, table_name))

    # ------------------------------------------------------------------
    # TPS persistence
    # TPS is stored in a per-table JSON file so it survives restarts
    # without expanding the fixed PAGE_SIZE binary format.
    # ------------------------------------------------------------------

    def tps_file_path(self, table_name):
        return os.path.join(self.db_path, f"{table_name}_tps.json")

    def load_tps_for_table(self, table_name):
        if table_name in self.tps_cache:
            return

        # new - fixed typo: was self_tps_file_path (missing the dot)
        path = self.tps_file_path(table_name)
        if os.path.exists(path):
            with open(path, 'r') as f:
                raw = json.load(f)
            self.tps_cache[table_name] = {k: int(v) for k, v in raw.items()}
        else:
            self.tps_cache[table_name] = {}

    # persist a table's TPS map to disk
    def save_tps_for_table(self, table_name):
        path = self.tps_file_path(table_name)
        # new - fixed attribute name (was self._tps_cache, wrong name)
        with open(path, 'w') as f:
            json.dump(self.tps_cache[table_name], f)

    def page_id_to_tps_key(self, page_id):
        return str(page_id)

    def read_tps(self, page_id):
        table_name = page_id[0]
        self.load_tps_for_table(table_name)
        key = self.page_id_to_tps_key(page_id)
        return self.tps_cache[table_name].get(key, None)

    # call whenever a base page is merged so TPS survives a restart
    def write_tps(self, page_id, tps_value):
        table_name = page_id[0]
        self.load_tps_for_table(table_name)
        key = self.page_id_to_tps_key(page_id)
        self.tps_cache[table_name][key] = tps_value
        self.save_tps_for_table(table_name)
