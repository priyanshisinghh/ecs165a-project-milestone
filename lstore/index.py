"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

#might be allowed? unsure
from sortedcontainers import SortedDict

class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.columns = table.num_columns
        self.indices = [None] * self.columns 
        self.create_index(table.key)
        self.table = table

    """
      returns the location of all records with the given value on column "column"
      i assume if the value cannot be found/if the function fails it returns false
    """

    def locate(self, column, value):
        #bounds check/existing check (for all functions)
        if column >= self.columns or column < 0:
            raise ValueError("Column Out Of Bounds")

        if self.indices[column] is None:
            return []

        index = self.indices[column]
        if value in index.keys():
            return index.get(value, [])
            
        return []

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, column, begin, end):
        if column >= self.columns or column < 0:
            raise ValueError("Column Out Of Bounds")

        if self.indices[column] is None:
            return []

        index = self.indices[column]
        keys = list(index.keys())
        RIDs = []

        #check if the requested range is even in the index
        if len(keys) == 0 or end < keys[0] or begin > keys[-1]:
            return []
            
        wantedKeys = list(index.irange(begin, end))
        for i in wantedKeys:
            RIDs.extend(index.get(i, []))  # Changed to extend and get all RIDs

        return RIDs

    """
    # optional: Create index on specific column
    """

    def create_index(self, column):
        if column >= self.columns or column < 0:
            raise ValueError("Column Out Of Bounds")

        if self.indices[column] is not None:
            return False

        self.indices[column] = SortedDict()

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column):
        if column >= self.columns or column < 0:
            raise ValueError("Column Out Of Bounds")

        if self.indices[column] is None:
            return False

        if column == self.table.key:
            return False

        self.indices[column] = None

    """
    # insert_entry -> adding RID to ind for col value when new record added to table
    """

    def insert_entry(self, column, key, RID):
        if column >= self.columns or column < 0:
            raise ValueError("Column Out Of Bounds")

        if self.indices[column] is None:
            return False

        index = self.indices[column]
        
        if column == self.table.key:
            if index.get(key) is not None:
                return False #cannot interfere w already existing primary key
            
        if index.get(key) is not None:
            index[key].append(RID)
        else:
            index[key] = [RID]

        return True

    """
    Removing key and RIDs from index
    """
    def remove_entry(self, column, key):
        if column >= self.columns or column < 0:
            raise ValueError("Column Out Of Bounds")

        if self.indices[column] is None:
            return False

        index = self.indices[column]
        if index.get(key) is None:
            return False

        index.pop(key)
        return True