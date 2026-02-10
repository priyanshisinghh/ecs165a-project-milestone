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
        metadata = [-1, rID, int(time()), 0] 

        entry = self.base_page.update(*(metadata + data))
        self.page_directory[rID] = (True, entry)

        # updating the index
        for i, cur_data in enumerate(data):
            #self.index.remove_entry(i, rID) # remove old
            self.index.insert_entry(i, cur_data, rID) # input new

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

        schema_encoding = 0
        reverse = self.num_columns

        
        # cumulative
        old_data = []
        if prev_tail == -1: # if previous is the base
            for val in range(self.num_columns):
                old_data.append(self.base_page.columns[METADATA_COLUMNS+ val].read(base_location))
        else:
            temp_prev, temp_prev_location = self.page_directory[prev_tail]
            for val in range(self.num_columns):
                old_data.append(self.tail_page.columns[METADATA_COLUMNS+ val].read(temp_prev_location))

        #print("1 TABLE CULM", old_data, primary_key)
        #print("2",data, prev_tail)
        # the 1's represent most updated
        # non - culaltive
        for i, val in enumerate(data):
            if val is None:
                data[i] = old_data[i]
                schema_encoding |= ( 1 << (reverse - i-1))
            else:
                schema_encoding |= ( 1 << (reverse - i-1))
        #print("3",data)
            
            

        metadata = [prev_tail, new_rID, int(time()), schema_encoding]
        #print("UPDATE ", schema_encoding, new_rID)
        entry = self.tail_page.append(*(metadata + data))
        self.page_directory[new_rID] = (False, entry)

        # change the old indirection column in the base_page
        self.base_page.columns[INDIRECTION_COLUMN].replace(base_location, new_rID)
        # change the old schema, any 1's are considered updated by the some tail pointer
        return True

    def __merge(self):
        new_base_page = Base_Page(self.num_columns)
        new_page_directory = {}

        for rid in self.page_directory:
            is_base, base_offset = self.page_directory[rid]

            #Base record merge
            if not is_base:
                continue

            #read base values
            merged_data = []
            for i in range(self.num_columns):
                val = self.base_page.columns[METADATA_COLUMNS + i].read(base_offset)
                merged_data.append(val)

            #check  tail record
            tail_rid = self.base_page.columns[INDIRECTION_COLUMN].read(base_offset)

            if tail_rid != -1 and tail_rid in self.page_directory:
                tail_is_base, tail_offset = self.page_directory[tail_rid]
                if tail_is_base == False:
                    for i in range(self.num_columns):
                        merged_data[i] = self.tail_page.columns[METADATA_COLUMNS + i].read(tail_offset)

            #write the merged record into the new base page
            metadata = [-1, rid, int(time()), 0]
            new_offset = new_base_page.update(*(metadata + merged_data))
            new_page_directory[rid] = (True, new_offset)

        #replace old pages w/ merged ones
        self.base_page = new_base_page
        self.page_directory = new_page_directory
        self.tail_page = Tail_Page(self.num_columns)

        #reset rid counter to one higher than the highest current RID
        self.rid_num = max(new_page_directory.keys()) + 1 if new_page_directory else 0
 
