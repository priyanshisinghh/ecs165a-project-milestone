from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import BASE_METADATA_COLUMNS

INDIRECTION_COLUMN = 0
SCHEMA_ENCODING_COLUMN = 3


class Query:
    """
    Queries that fail or crash must return False
    Queries that succeed should return the result or True
    """
    def __init__(self, table):
        self.table = table

    #reads the latest version of a certain RID
    def latest_record(self, search_RID, column_indexes):
        if search_RID not in self.table.page_directory:
            return False
        loc = self.table.page_directory[search_RID]

        #do not want to read tail pages directly
        if not loc['is_base']:
            return False
        return self.table.read_record(search_RID, column_indexes)

    def delete(self, primary_key):
        rIDs = self.table.index.locate(self.table.key, primary_key)
        if not rIDs:
            return False

        base_rid = rIDs[0]
        #base_loc = self.table.page_directory[base_rid]

        #update record to have special values so the record shows as deleted
        #invalidated_record = [None] * self.table.num_columns
        #self.table.update(primary_key, invalidated_record)
        self.table.index.indices[self.table.key].pop(primary_key, None)

        #remove from all active indexes
        del self.table.page_directory[base_rid]

        return True

    def insert(self, *columns):
        return self.table.insert(list(columns))

    #table takes care of reading record and record version, query just validates the RIDs
    def select(self, search_key, search_key_index, projected_columns_index):
        rIDs = self.table.index.locate(search_key_index, search_key)
    
        # if no index on this column, scan all base rids manually
        if not rIDs and self.table.index.indices[search_key_index] is None:
            rIDs = [rid for rid, loc in self.table.page_directory.items() if loc['is_base']]
    
        records = []
        for rID in rIDs:
            if rID not in self.table.page_directory:
                continue
            loc = self.table.page_directory[rID]
            if not loc['is_base']:
                continue
            record = self.table.read_record(rID, projected_columns_index)
            # when scanning without index, filter manually since index didnt do it for us
            if record is not False and record.columns[search_key_index] == search_key:
                records.append(record)
        return records

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        rIDs = self.table.index.locate(search_key_index, search_key)
        if not rIDs:
            return []

        records = []
        for rID in rIDs:
            if rID not in self.table.page_directory:
                continue
            loc = self.table.page_directory[rID]
            if not loc['is_base']:
                continue
            record = self.table.read_record_version(rID, projected_columns_index, relative_version)
            if record is not False:
                records.append(record)

        return records

    def update(self, primary_key, *columns):
        return self.table.update(primary_key, list(columns))

    def sum(self, start_range, end_range, aggregate_column_index):
        #find all BASE RIDs in the primary key range
        rIDs = self.table.index.locate_range(self.table.key, start_range, end_range)
        if not rIDs:
            return False

        total = 0
        search_columns = [0] * self.table.num_columns
        search_columns[aggregate_column_index] = 1

        for rID in rIDs:
            if rID not in self.table.page_directory:
                continue
            loc = self.table.page_directory[rID]
            if not loc['is_base']:
                continue
            record = self.table.read_record(rID, search_columns)
            if record is not False:
                total += record.columns[aggregate_column_index]

        return total

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        rIDs = self.table.index.locate_range(self.table.key, start_range, end_range)
        if not rIDs:
            return False

        total = 0
        search_columns = [0] * self.table.num_columns
        search_columns[aggregate_column_index] = 1

        for rID in rIDs:
            if rID not in self.table.page_directory:
                continue
            loc = self.table.page_directory[rID]
            if not loc['is_base']:
                continue
            record = self.table.read_record_version(rID, search_columns, relative_version)
            if record is not False:
                total += record.columns[aggregate_column_index]

        return total

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        records = self.select(key, self.table.key, [1] * self.table.num_columns)
        if records and len(records) > 0:
            r = records[0]
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
