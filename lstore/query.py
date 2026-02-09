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
        pass
    
    """
    reads the latest version of a certain RID
    """
    def latest_record(self, search_RID, column_indexs):
        base, base_location = self.table.page_directory[search_RID] 

        indirection = self.table.base_page.collumns[INDIRECTION_COLUMN].read(base_location)

        values = [] * self.table.num_columns
        if not indirection:
            for i in range(self.table.num_columns):
                if not column_indexs[i]:
                    continue
                values[i] = self.table.base_page.columns[METADATA_COLUMNS].read(base_location)
        else:
            tail, tail_location = self.page_directory[indirection]
            schema_encoding = self.table.tail_page.columns[SCHEMA_ENCODING_COLUMN].read(tail_location)
            
            for i in range(self.table.num_column):
                if not column_indexs[i]:
                    continue
            
                if schema_encoding[i]:
                    values[i] = self.table.tail_page.columns[METADATA_COLUMNS + i].read(tail_location)
                else:
                    values[i] = self.table.base_page.columns[METADATA_COLUMNS + 1].read(base_location)
        
        return Record(search_RID, values[self.table.key], values)

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        pass
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    # done -ish
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
    #done - ish
    def select(self, search_key, search_key_index, projected_columns_index):
        rIDs = self.table.index.locate(search_key_index, search_key)
        records = []
        for rID in rIDs:
            records.append(self.latest_record(rID, projected_columns_index))
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
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    # done - ish
    def update(self, primary_key, *columns):
        return self.table.update(primary_key,list(columns))

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        rIDs = self.table.index.locate_range(aggregate_column_index, start_range, end_range)

        if not rIDs:
            return False
        
        total = 0
        # for it work with the other function
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
        rIDs = self.table.index.locate_range(aggregate_column_index, start_range, end_range)
        if not rIDs:
            return False
        
        total = 0
        # for it work with the other function
        search_columns = [0]*self.table.num_columns
        search_columns[aggregate_column_index] = 1

        for rID in rIDs:
            records = self.select_version(rID,self.table.key, search_columns, relative_version)
            total += records[0].columns[aggregate_column_index] # assumes the search key is the primary key and will only have one value, otherwise change 

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
