"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

import threading
from sortedcontainers import SortedDict


class Index:
    def __init__(self, table):
        self.columns = table.num_columns
        self.indices = [None] * self.columns
        self.table = table
        
        #one lock per column, always exists regardless of whether
        self.col_locks = [threading.RLock() for _ in range(self.columns)]

        #guards create_index / drop_index 
        self.meta_lock = threading.Lock()

        self.create_index(self.table.key)

    def locate(self, column, value):
        #range check
        if column >= self.columns or column < 0:
            raise ValueError("Column Out Of Bounds")

        #column locks needed now to not accidentally have multiple threads edit the same column at once
        with self.col_locks[column]:
            index = self.indices[column]
            if index is None:
                return []
            return list(index.get(value, []))

    def locate_range(self, column, begin, end):
        if column >= self.columns or column < 0:
            raise ValueError("Column Out Of Bounds")

        with self.col_locks[column]:
            index = self.indices[column]
            if index is None:
                return []

            keys = list(index.keys())
            if not keys or end < keys[0] or begin > keys[-1]:
                return []

            RIDs = []
            for k in index.irange(begin, end):
                RIDs.extend(index.get(k, []))
            return RIDs

    def create_index(self, column):
        if column >= self.columns or column < 0:
            raise ValueError("Column Out Of Bounds")

        with self.meta_lock:
            if self.indices[column] is not None:
                return False
            self.indices[column] = SortedDict()
        return True

    def drop_index(self, column):
        if column >= self.columns or column < 0:
            raise ValueError("Column Out Of Bounds")

        if column == self.table.key:
            return False

        with self.meta_lock:
            if self.indices[column] is None:
                return False
            self.indices[column] = None
        return True

    def insert_entry(self, column, key, RID):
        if column >= self.columns or column < 0:
            raise ValueError("Column Out Of Bounds")

        with self.col_locks[column]:
            index = self.indices[column]
            if index is None:
                return False

            if column == self.table.key:
                if index.get(key) is not None:
                    return False #do not want to insert data w/ already existing key
                index[key] = [RID]
                return True

            if index.get(key) is not None:
                if RID not in index[key]:
                    index[key].append(RID) #no duplicate RIDs
                else:
                    return False
            else:
                index[key] = [RID] #create new pair in index if it doesn't already exist
            return True

    def remove_entry(self, column, key, RID):
        if column >= self.columns or column < 0:
            raise ValueError("Column Out Of Bounds")

        if column == self.table.key:
            return False

        with self.col_locks[column]:
            index = self.indices[column]
            if index is None:
                return False
            if index.get(key) is None:
                return False

            index[key].remove(RID)
            if not index[key]:
                del index[key] #if there is nothing left in index[key], delete
            return True

    #not really necessayr
    def remove_key(self, column, key):
        if column >= self.columns or column < 0:
            raise ValueError("Column Out Of Bounds")

        if column == self.table.key:
            return False

        with self.col_locks[column]:
            index = self.indices[column]
            if index is None:
                return False
            if index.get(key) is None:
                return False
            index.pop(key)
            return True

    def update_entry(self, column, old_value, new_value, RID):
        if column >= self.columns or column < 0:
            raise ValueError("Column Out Of Bounds")

        if column == self.table.key:
            return False

        with self.col_locks[column]:
            index = self.indices[column]
            if index is None:
                return False

            #essentially remove entry then insert entry
            if index.get(old_value) is not None:
                index[old_value].remove(RID)
                if not index[old_value]:
                    del index[old_value]

            if index.get(new_value) is not None:
                if RID not in index[new_value]:
                    index[new_value].append(RID)
            else:
                index[new_value] = [RID]

            return True
