import threading

VALUE_SIZE = 8
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_RID_COLUMN = 4
TAIL_METADATA_COLUMNS = 5
BASE_METADATA_COLUMNS = 4  #base pages do not need the base rid column

PAGE_SIZE = 4096
HEADER_SIZE = VALUE_SIZE
DATA_SIZE = PAGE_SIZE - HEADER_SIZE
VALUES_PER_PAGE = DATA_SIZE // VALUE_SIZE  #511 usable slots (last 8 bytes = header)
MAX_RECORDS_PER_BASE_PAGE = VALUES_PER_PAGE


class Page:
    def __init__(self):
        self.num_records = 0
        self.size = PAGE_SIZE
        self.ptr = 0
        self.data = bytearray(self.size)
        self.write_lock = threading.Lock()

    def has_capacity(self):
        return (VALUE_SIZE + self.ptr) <= DATA_SIZE

    #write to page - lock is needed the whole time
    def write(self, value):
        with self.write_lock:
            if not self.has_capacity():
                return None
            if value is None:
                self.data[self.ptr: self.ptr + VALUE_SIZE] = (0).to_bytes(VALUE_SIZE, byteorder = 'big', signed = True)
            else:
                self.data[self.ptr: self.ptr + VALUE_SIZE] = (value).to_bytes(VALUE_SIZE, byteorder = 'big', signed = True)
                
            slot = self.num_records
            self.num_records += 1
            self.ptr += VALUE_SIZE
            #persist num_records in the page header so it survives eviction/reload
            self.data[DATA_SIZE: DATA_SIZE + VALUE_SIZE] = self.num_records.to_bytes(VALUE_SIZE, byteorder = 'big', signed = False)
            return slot

    def overwrite(self, offset, value):
        #no lock is needed bc a lock is needed before calling this function
        start = int(offset * VALUE_SIZE)
        self.data[start: start + VALUE_SIZE] = (value).to_bytes(VALUE_SIZE, byteorder = 'big', signed = True)

    #read the data at the given offset
    def read(self, offset):
        start = int(offset * VALUE_SIZE)
        return int.from_bytes(self.data[start: start + VALUE_SIZE], byteorder = 'big', signed =True)


class Base_Page:
    def __init__(self, num_columns):
        self.num_records = 0
        self.num_columns = num_columns
        self.total_columns = BASE_METADATA_COLUMNS + num_columns
        self.columns = [Page() for _ in range(self.total_columns)]
        self.alloc_lock = threading.Lock()

    def has_capacity(self):
        return self.num_records < VALUES_PER_PAGE

    def write(self, *data):
        with self.alloc_lock:
            if not self.has_capacity():
                return None
            slot = self.num_records
            self.num_records += 1

        #write each column outside the lock - each thread is now writing to a distinct slot so the writes dont conflict
        for i, value in enumerate(data):
            self.columns[i].write(value)
        return slot

    def read(self, slot, column_index):
        return self.columns[column_index].read(slot)

    def overwrite(self, slot, column_index, value):
        self.columns[column_index].overwrite(slot, value)


class Tail_Page:
    def __init__(self, num_columns):
        self.num_records = 0
        self.num_columns = num_columns
        self.total_columns = TAIL_METADATA_COLUMNS + num_columns
        self.columns = [Page() for _ in range(self.total_columns)]
        self.alloc_lock = threading.Lock()

    def has_capacity(self):
        return self.num_records < VALUES_PER_PAGE

    def append(self, *data):
        """
        Atomically allocate the next slot, then write each column value.
        Returns the slot index, or None if full.
        """
        with self.alloc_lock:
            if not self.has_capacity():
                return None
            slot = self.num_records
            self.num_records += 1

        for i, value in enumerate(data):
            self.columns[i].write(value)
        return slot

    def read(self, slot, column_index):
        return self.columns[column_index].read(slot)

    def overwrite(self, slot, column_index, value):
        self.columns[column_index].overwrite(slot, value)


class Page_Range:
    def __init__(self, num_columns, num_base_pages = 16):
        self.num_columns = num_columns
        self.num_base_pages = num_base_pages
        self.base_pages = [Base_Page(num_columns) for _ in range(num_base_pages)]
        self.tail_pages = [Tail_Page(num_columns)]
        self.current_base_page_index = 0
        self.tps = [0] * num_base_pages

        #guards base pages, tail pages, and tps reads/writes
        self.base_lock = threading.Lock()  
        self.tail_lock = threading.Lock()   
        self.tps_lock = threading.Lock()    

    #finds a base page, gets a slot, and writes the data
    def write_base(self, *data):
        with self.base_lock:
            while self.current_base_page_index < self.num_base_pages:
                bp = self.base_pages[self.current_base_page_index]
                if bp.has_capacity():
                    slot = bp.write(*data)
                    return (self.current_base_page_index, slot)
                self.current_base_page_index += 1
            return None

    def read_base(self, base_page_index, slot, column_index):
        return self.base_pages[base_page_index].read(slot, column_index)

    def overwrite_base(self, base_page_index, slot, column_index, value):
        self.base_pages[base_page_index].overwrite(slot, column_index, value)

    def base_is_full(self):
        with self.base_lock:
            return self.current_base_page_index >= self.num_base_pages

    #appends to tail record, makes a new one if all tail pages are full
    def append_tail(self, *data):
        with self.tail_lock:
            tail_page = self.tail_pages[-1]
            if not tail_page.has_capacity():
                tail_page = Tail_Page(self.num_columns)
                self.tail_pages.append(tail_page)
            tail_page_index = len(self.tail_pages) - 1

        #slot allocation inside append is itself atomic
        slot = tail_page.append(*data)
        return (tail_page_index, slot)

    def read_tail(self, tail_page_index, slot, column_index):
        return self.tail_pages[tail_page_index].read(slot, column_index)

    def overwrite_tail(self, tail_page_index, slot, column_index, value):
        self.tail_pages[tail_page_index].overwrite(slot, column_index, value)

    def get_tps(self, base_page_index):
        with self.tps_lock:
            return self.tps[base_page_index]

    def set_tps(self, base_page_index, tail_rid):
        with self.tps_lock:
            self.tps[base_page_index] = tail_rid
