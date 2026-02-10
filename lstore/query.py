from lstore.table import Table, Record
from lstore.index import Index

METADATA_COLUMNS = 4
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

    """
    reads the latest version of a certain RID
    """
    def latest_record(self, search_RID, column_indexs):
        base, base_location = self.table.page_directory[search_RID]
        if base != True:
            return False

        indirection = self.table.base_page.columns[INDIRECTION_COLUMN].read(base_location)
        values = [None]*self.table.num_columns

        #if it has no updates, read base record
        if indirection == -1:
            for i in range(self.table.num_columns):
                if not column_indexs[i]:
                    continue
                values[i] = self.table.base_page.columns[METADATA_COLUMNS + i].read(base_location)

        #if it has updates, go to newest tail
        else:
            tail, tail_location = self.table.page_directory[indirection]
            for i in range(self.table.num_columns):
                if not column_indexs[i]:
                    continue
                values[i] = self.table.tail_page.columns[METADATA_COLUMNS + i].read(tail_location)

        return Record(search_RID, values[self.table.key], values)

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        #find RID for the given primary key
        rIDs = self.table.index.locate(self.table.key, primary_key)

        #if there is nothing to delete the query fails
        if not rIDs:
            return False

        self.table.index.remove_entry(self.table.key, primary_key)
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
        #lookup RIDs using index
        rIDs = self.table.index.locate(search_key_index, search_key)
        if not rIDs:
            return False

        #this can be used for non primary keys, but that won't be necessary for milestone 1
        records = []
        for rID in rIDs:
            base, entry = self.table.page_directory[rID]
            #we can only use base records for entry points
            if base == True: 
                record = self.latest_record(rID, projected_columns_index)
                records.append(record)
                continue

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
        #locate RIDs for search key
        rIDs = self.table.index.locate(search_key_index, search_key)
        if not rIDs:
            return False

        records = []

        for rID in rIDs:
            base, base_location = self.table.page_directory[rID]
            #only want to enter using base records
            if base != True:
                continue

            #read indirection pointer
            indirection = self.table.base_page.columns[INDIRECTION_COLUMN].read(base_location)
            values = [None]*self.table.num_columns

            #if there are no updates, only base exists, so use those values
            if indirection == -1:
                for i in range(self.table.num_columns):
                    if not projected_columns_index[i]:
                        continue
                    values[i] = self.table.base_page.columns[METADATA_COLUMNS + i].read(base_location)

                records.append(Record(rID, values[self.table.key], values))
                continue

            #latest version - take these values
            if relative_version == 0:
                tail, tail_location = self.table.page_directory[indirection]
                for i in range(self.table.num_columns):
                    if not projected_columns_index[i]:
                        continue
                    values[i] = self.table.tail_page.columns[METADATA_COLUMNS + i].read(tail_location)

                records.append(Record(rID, values[self.table.key], values))
                continue

            #for older versions (negative value): walk backwards, go to base
            if relative_version < 0:
                steps = -relative_version
                current_tail_rid = indirection
                current_tail_location = self.table.page_directory[current_tail_rid][1]

                #loop thru previous versions
                for _ in range(steps):
                    prev = self.table.tail_page.columns[INDIRECTION_COLUMN].read(current_tail_location)

                    #stop if the base is reached
                    if prev == -1:
                        current_tail_rid = None
                        break

                    prev_flag, prev_loc = self.table.page_directory[prev]
                    if prev_flag == True:
                        current_tail_rid = None
                        break

                    current_tail_rid = prev
                    current_tail_location = prev_loc

                if current_tail_rid is None:
                    #fall back to base record if there is no current tail RID
                    for i in range(self.table.num_columns):
                        if not projected_columns_index[i]:
                            continue
                        values[i] = self.table.base_page.columns[METADATA_COLUMNS + i].read(base_location)
                else:
                    #read the requested historical tail
                    for i in range(self.table.num_columns):
                        if not projected_columns_index[i]:
                            continue
                        values[i] = self.table.tail_page.columns[METADATA_COLUMNS + i].read(current_tail_location)

                records.append(Record(rID, values[self.table.key], values))
                continue

            #if positive versions are not used, default to base
            for i in range(self.table.num_columns):
                if not projected_columns_index[i]:
                    continue
                values[i] = self.table.base_page.columns[METADATA_COLUMNS + i].read(base_location)

            records.append(Record(rID, values[self.table.key], values))

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
        #find all BASE RIDs in the primary key range
        rIDs = self.table.index.locate_range(self.table.key, start_range, end_range)
        if not rIDs:
            return False

        total = 0
        search_columns = [0]*self.table.num_columns
        search_columns[aggregate_column_index] = 1

        for rID in rIDs:
            record = self.latest_record(rID, search_columns)
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
        search_columns = [0]*self.table.num_columns
        search_columns[aggregate_column_index] = 1

        for rID in rIDs:
            base, base_location = self.table.page_directory[rID]
            if base != True:
                continue

            # convert RID to primary key value so that  select_version gets the right searchkey)
            pk_val = self.table.base_page.columns[METADATA_COLUMNS + self.table.key].read(base_location)
            record = self.select_version(pk_val, self.table.key, search_columns, relative_version)[0]
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
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
