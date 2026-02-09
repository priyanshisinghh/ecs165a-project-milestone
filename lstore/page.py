# these are constants but they can be put into config.py
VALUE_SIZE = 8 # size of the data
METADATA_COLUMNS = 4 
INDIRECTION_COLUMN = 0 
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

PAGE_SIZE = 4096
VALUES_PER_PAGE = PAGE_SIZE / VALUE_SIZE

class Page:

    def __init__(self):
        self.num_records = 0
        self.size = PAGE_SIZE
        self.ptr = 0 # points to end of written portion
        self.data = bytearray(self.size)

    # returns remaining capacity of page
    def has_capacity(self):
        return (self.size - self.ptr)

    # everything is in bits so read will have to turn it back
    def write(self, value):
        self.data[self.ptr:self.ptr + VALUE_SIZE] = (value).to_bytes(VALUE_SIZE, byteorder="big")
        self.num_records += 1
        self.ptr += VALUE_SIZE
    
    def overwrite(self, offset, value):
        start = offset*VALUE_SIZE
        self.data[start:start + VALUE_SIZE] = (value).to_bytes(VALUE_SIZE, byteorder="big")

    def read(self, offset):
        start = offset*VALUE_SIZE
        return int.from_bytes(self.data[start: start + VALUE_SIZE],byteorder='big',signed='True')

# to avoid duplicate work for Base Page and Tail page logic
class Page_Column:
    def __init__(self):
        self.pages = [Page()]
        self.page_num = 1 # debug info
    
    # writes to the end of the column 
    # allocates new page if not enough space
    def write(self, data):
        page = self.pages[-1]
        if page.has_capacity > VALUE_SIZE:
            new_page = Page()
            self.pages.append(new_page)
        new_page.write(data)

    # reads using the rID, 
    def read(self, rID):
        page = rID // VALUES_PER_PAGE
        offset =  rID % VALUES_PER_PAGE
        self.pages[page].read(offset)

    def replace(self, rID, data):
        page = rID // VALUES_PER_PAGE
        offset =  rID % VALUES_PER_PAGE
        self.pages[page].overwrite(offset,data)
    

# Not done
class Base_Page:
    def __init__(self, columns):
        self.num_records = 0
        self.column_num = columns
        self.columns = [Page_Column() for i in range(METADATA_COLUMNS + columns)]

    """
    :param   Data's first couple columns are metadata
    :returns index of where the record is in each page
    """
    def update(self, *data):
        for i, cur_data in enumerate(data):
            self.columns[i].write(cur_data)
        self.num_records += 1
        return self.num_records - 1
    
# Not done
class Tail_Page:
    def __init__(self, columns):
        self.num_records = 0
        self.column_num = columns
        self.columns = [Page_Column() for i in range(METADATA_COLUMNS + columns)]

    # Doesn't check if there is space in the page, thats the responsibility of calling function
    def append(self, *data):
        for i, cur_data in enumerate(data):
            self.columns[i].write(cur_data)
        self.num_records += 1
        return self.num_records - 1 
    