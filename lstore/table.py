from lstore.index import Index
from time import time
from lstore.page import Base_Page
from lstore.page import Tail_Page

METADATA_COLUMNS = 4 
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        
        #for reading and writing
        self.base_page = Base_Page(num_columns)
        self.tail_page = Tail_Page(num_columns)

        self.page_directory = {} # {rID, (base_version, offset)}
        self.index = Index(self)
        self.rid_num = 0
        self.merge_threshold_pages = 50  # The threshold to trigger a merge
        pass

    """
    insert is putting it into the base pages
    :param data: iterable containing all column data
    """ 
    def insert(self, data):
        if len(data) !=  self.num_columns:
            raise("WRONG DATA BEING INSERTED INTO DATA")
        rID = self.rid_num
        self.rid_num += 1

        # insert to base page
        # indirection , RID, timestap, Schema
        metadata = [0, rID, time(), 0] 

        entry = self.base_page.update(*(metadata + data))
        self.page_directory[rID] = (True, entry)

        # updating the index
        for i, cur_data in enumerate(data):
            self.index.insert_entry(i,cur_data, rID)

        return True
    
    """
    update is writing to tail pages
    to update a certain key's columns
    """
    def update(self, primary_key, data):

        rIDs = self.index.locate(self.key, primary_key)
        # if no matching keys were found
        if not rIDs:
            return False
        
        # assumes no duplicate on primary keys
        rID = rIDs[0]
        base, base_location = self.page_directory[rID]

        # find previous record location to track
        prev_tail = self.base_page.columns[INDIRECTION_COLUMN].read(base_location)

        new_rID = self.rid_num 
        self.rid_num += 1
        new_tail = []

        schema_encoding = '0' * (self.table.num_columns + METADATA_COLUMNS)
        for i, val in enumerate(data):
            if val is not None:
                schema_encoding[i] = '1'

        metadata = [prev_tail,new_rID, time(), schema_encoding]

        entry = self.tail_page.append(*(metadata + data))
        self.page_directory[new_rID] = (False, entry)

        # change the old indirection column in the base_page
        self.base_page.columns[INDIRECTION_COLUMN].replace(base_location, new_rID)

        # change the old schema if neccesary


    def __merge(self):
        print("merge is happening")
        pass
 