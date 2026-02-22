# handles writing to disk
import os
import struct


class disk:
    def __init__(self):
        self.page_size = 4096 #match page class
        self.int_size = 8 #64 bit
        self.table_directory = {} # table_id : location of entry
        #self.file <- ill crash on init since its a single file
    
    #MIGHT NEED THIS??? IDKK =================================================================
    #============================================================================

    def write_page(self, page, page_type, col, page_entry):
        self.file.seek(0)
        self.file.read(self.page_size)

        pass

    def read_page(self, page, page_type, col, page_entry):
        
        pass

    def close(self):
        self.file.close()
        pass
    
    def open(self, Path):
        self.file = open(Path, 'ab')
        self.file.seek(0)
        table_nums = self.file.read(self.int_size)
        
        pass
