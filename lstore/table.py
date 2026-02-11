from lstore.index import Index
from time import time
from lstore.page import Base_Page, Tail_Page, METADATA_COLUMNS

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
        self.base_page = Base_Page(num_columns)  
        self.tail_page = Tail_Page(num_columns) 
        self.page_directory = {}
        self.index = Index(self)
        self.rid_num = 0
        self.merge_threshold_pages = 50 # The threshold to trigger a merge

    def insert(self, data):
        if len(data) != self.num_columns:
            raise ValueError("Wrong number of columns")
        
        # Generate new RID
        rID = self.rid_num
        self.rid_num += 1
        
        metadata = [-1, rID, int(time()), 0]

        entry = self.base_page.update(*(metadata + data))
        
        self.page_directory[rID] = (True, entry)
        
        # Update index for all columns
        for i, cur_data in enumerate(data):
            self.index.insert_entry(i, cur_data, rID)
        
        return True
    
    def update(self, primary_key, data):
        # Find the record's RID using primary key
        rIDs = self.index.locate(self.key, primary_key)
        if not rIDs:
            return False
        new_primary_key =  data[self.key]
        exists = self.index.locate(self.key, new_primary_key)
        if exists:
            return False
        
        rID = rIDs[0]
        base, base_location = self.page_directory[rID]
        
        prev_tail = self.base_page.columns[INDIRECTION_COLUMN].read(base_location)
        
        new_rID = self.rid_num
        self.rid_num += 1
        
        schema_encoding = 0
        reverse = self.num_columns
        
        # Get old values to fill in non-updated columns
        old_data = []
        if prev_tail == -1:
            for val in range(self.num_columns):
                old_data.append(self.base_page.columns[METADATA_COLUMNS + val].read(base_location))
        else:
            temp_prev, temp_prev_location = self.page_directory[prev_tail]
            for val in range(self.num_columns):
                old_data.append(self.tail_page.columns[METADATA_COLUMNS + val].read(temp_prev_location))
        
        # Sets schema to all 1's for culmative and replaces any Nones with old data
        for i, val in enumerate(data):
            if val is None:
                data[i] = old_data[i]
            schema_encoding |= (1 << (reverse - i - 1))
        
        metadata = [prev_tail, new_rID, int(time()), schema_encoding]

        entry = self.tail_page.append(*(metadata + data))
        self.page_directory[new_rID] = (False, entry)
        
        self.base_page.columns[INDIRECTION_COLUMN].replace(base_location, new_rID)
        
        return True

    def __merge(self):
        
        new_base_page = Base_Page(self.num_columns)
        new_page_directory = {}
        
        # Process each base record
        for rid in self.page_directory:
            is_base, base_offset = self.page_directory[rid]
            
            if not is_base:
                continue
            
            # Read current base values
            merged_data = []
            for i in range(self.num_columns):
                val = self.base_page.columns[METADATA_COLUMNS + i].read(base_offset)
                merged_data.append(val)
            
            tail_rid = self.base_page.columns[INDIRECTION_COLUMN].read(base_offset)
            
            if tail_rid != -1 and tail_rid in self.page_directory:
                # Apply the latest tail update
                tail_is_base, tail_offset = self.page_directory[tail_rid]
                if tail_is_base == False:  
                    for i in range(self.num_columns):
                        merged_data[i] = self.tail_page.columns[METADATA_COLUMNS + i].read(tail_offset)
            
            metadata = [-1, rid, int(time()), 0]
            new_offset = new_base_page.update(*(metadata + merged_data))
            new_page_directory[rid] = (True, new_offset)
        
        self.base_page = new_base_page
        self.page_directory = new_page_directory
        self.tail_page = Tail_Page(self.num_columns)
        
        self.rid_num = max(new_page_directory.keys()) + 1 if new_page_directory else 0
