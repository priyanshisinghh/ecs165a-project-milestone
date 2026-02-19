# handles writing to disk

class disk:
    def __init__(self, database):
        self.page_size = 4096
        self.int_size = 8
        self.table_directory = {} # table_id : location of entry
        self.file

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
