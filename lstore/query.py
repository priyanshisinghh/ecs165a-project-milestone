from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import METADATA_COLUMNS

INDIRECTION_COLUMN = 0
SCHEMA_ENCODING_COLUMN = 3


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table

    # reads the latest version of a certain RID
    # new - goes through table.read_record instead of touching pages directly
    def latest_record(self, search_RID, column_indexes):
        if search_RID not in self.table.page_directory:
            return False
        loc = self.table.page_directory[search_RID]
        if not loc['is_base']:
            return False
        return self.table.read_record(search_RID, column_indexes)

    """
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        # find RID for the given primary key
        rIDs = self.table.index.locate(self.table.key, primary_key)
        if not rIDs:
            return False

        base_rid = rIDs[0]
        base_loc = self.table.page_directory[base_rid]

        # new - write a tombstone so the record shows as deleted
        tombstone = [None] * self.table.num_columns
        self.table.update(primary_key, tombstone)

        # remove from all active indexes
        for i in range(self.table.num_columns):
            val = self.table._read_base_col(base_loc, METADATA_COLUMNS + i)
            if self.table.index.indices[i] is not None:
                self.table.index.remove_entry(i, val, base_rid)

        return True

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        return self.table.insert(list(columns))

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        # lookup RIDs using index
        rIDs = self.table.index.locate(search_key_index, search_key)
        if not rIDs:
            return []

        records = []
        for rID in rIDs:
            if rID not in self.table.page_directory:
                continue
            loc = self.table.page_directory[rID]
            # we can only use base records for entry points
            if not loc['is_base']:
                continue
            record = self.table.read_record(rID, projected_columns_index)
            if record is not False:
                records.append(record)

        return records

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        # locate RIDs for search key
        rIDs = self.table.index.locate(search_key_index, search_key)
        if not rIDs:
            return []

        records = []
        for rID in rIDs:
            if rID not in self.table.page_directory:
                continue
            loc = self.table.page_directory[rID]
            # only want to enter using base records
            if not loc['is_base']:
                continue
            record = self.table.read_record_version(rID, projected_columns_index, relative_version)
            if record is not False:
                records.append(record)

        return records

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        return self.table.update(primary_key, list(columns))

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        # find all BASE RIDs in the primary key range
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

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
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
