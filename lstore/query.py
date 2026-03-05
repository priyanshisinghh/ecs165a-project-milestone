from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import BASE_METADATA_COLUMNS

INDIRECTION_COLUMN = 0
SCHEMA_ENCODING_COLUMN = 3


class Query:
    def __init__(self, table):
        self.table = table
        self.lock_manager = None   #injected by TransactionWorker

    def get_shared_lock(self, rid, tid):
        if self.lock_manager is None:
            return True
        return self.lock_manager.acquire_shared(rid, tid)

    def get_exclusive_lock(self, rid, tid):
        if self.lock_manager is None:
            return True
        return self.lock_manager.acquire_exclusive(rid, tid)

    def lock_read(self, rids, tid):
        for rid in rids:
            if not self.get_shared_lock(rid, tid):
                return False
        return True

    def lock_write(self, rids, tid):
        for rid in rids:
            if not self.get_exclusive_lock(rid, tid):
                return False
        return True


    #reads the latest record version of a given RID
    def latest_record(self, search_RID, column_indexes, tid = None):
        if search_RID not in self.table.page_directory:
            return False
        loc = self.table.page_directory[search_RID]
        if not loc['is_base']:
            return False
        if not self.lock_read([search_RID], tid):
            return False
        return self.table.read_record(search_RID, column_indexes)

    #deletes a record (waits till merge to actually delete)
    def delete(self, primary_key, tid = None):
        rIDs = self.table.index.locate(self.table.key, primary_key)
        if not rIDs:
            return False

        #do not want to read tail pages directly
        base_rid = rIDs[0]
        if not self.lock_write([base_rid], tid):
            return False

        self.table.index.indices[self.table.key].pop(primary_key, None)
        del self.table.page_directory[base_rid]
        return True

    #insert creates a new RID, so we only need to lock page directory and index
    def insert(self, *columns, tid = None):
        return self.table.insert(list(columns))

    #selects the record required
    def select(self, search_key, search_key_index, projected_columns_index, tid = None):
        rIDs = self.table.index.locate(search_key_index, search_key)

        #fall back to full scan if column has no index
        if not rIDs and self.table.index.indices[search_key_index] is None:
            rIDs = [rid for rid, loc in self.table.page_directory.items() if loc['is_base']]

        base_rids = [
            rid for rid in rIDs
            if rid in self.table.page_directory
            and self.table.page_directory[rid]['is_base']
        ]

        if not self.lock_read(base_rids, tid):
            return False

        records = []
        for rid in base_rids:
            record = self.table.read_record(rid, projected_columns_index)
            if record is not False and record.columns[search_key_index] == search_key:
                records.append(record)
        return records

    #reads a given version for a given record
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version, tid = None):
        rIDs = self.table.index.locate(search_key_index, search_key)
        if not rIDs:
            return []

        base_rids = [
            rid for rid in rIDs
            if rid in self.table.page_directory
            and self.table.page_directory[rid]['is_base']
        ]

        if not self.lock_read(base_rids, tid):
            return False

        records = []
        for rid in base_rids:
            record = self.table.read_record_version(rid, projected_columns_index, relative_version)
            if record is not False:
                records.append(record)
        return records

    #update a given record
    def update(self, primary_key, *columns, tid = None):
        rIDs = self.table.index.locate(self.table.key, primary_key)
        if not rIDs:
            return False

        base_rid = rIDs[0]
        if not self.lock_write([base_rid], tid):
            return False

        return self.table.update(primary_key, list(columns))

    def sum(self, start_range, end_range, aggregate_column_index, tid = None):
        rIDs = self.table.index.locate_range(self.table.key, start_range, end_range)
        if not rIDs:
            return False

        base_rids = [
            rid for rid in rIDs
            if rid in self.table.page_directory
            and self.table.page_directory[rid]['is_base']
        ]

        if not self.lock_read(base_rids, tid):
            return False

        search_columns = [0] * self.table.num_columns
        search_columns[aggregate_column_index] = 1
        total = 0
        for rid in base_rids:
            record = self.table.read_record(rid, search_columns)
            if record is not False:
                total += record.columns[aggregate_column_index]
        return total

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version, tid = None):
        rIDs = self.table.index.locate_range(
            self.table.key, start_range, end_range)
        if not rIDs:
            return False

        base_rids = [
            rid for rid in rIDs
            if rid in self.table.page_directory
            and self.table.page_directory[rid]['is_base']
        ]

        if not self.lock_read(base_rids, tid):
            return False

        search_columns = [0] * self.table.num_columns
        search_columns[aggregate_column_index] = 1
        total = 0
        for rid in base_rids:
            record = self.table.read_record_version(
                rid, search_columns, relative_version)
            if record is not False:
                total += record.columns[aggregate_column_index]
        return total

    def increment(self, key, column, tid = None):
        records = self.select(key, self.table.key, [1] * self.table.num_columns, tid = tid)
        if not records or records is False:
            return False
        r = records[0]
        updated_columns = [None] * self.table.num_columns
        updated_columns[column] = r.columns[column] + 1
        return self.update(key, *updated_columns, tid = tid)
