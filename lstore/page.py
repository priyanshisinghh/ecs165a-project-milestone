VALUE_SIZE = 8
METADATA_COLUMNS = 4
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

PAGE_SIZE = 4096
VALUES_PER_PAGE = PAGE_SIZE // VALUE_SIZE  # 512 records per page
MAX_RECORDS_PER_BASE_PAGE = VALUES_PER_PAGE


class Page:
    def __init__(self):
        self.num_records = 0
        self.size = PAGE_SIZE
        self.ptr = 0
        self.data = bytearray(self.size)

    def has_capacity(self):
        return (VALUE_SIZE + self.ptr) <= self.size

    def write(self, value):
        if value is None:
            value = 0
        self.data[self.ptr:self.ptr + VALUE_SIZE] = int(value).to_bytes(
            VALUE_SIZE, byteorder="big", signed=True
        )
        self.num_records += 1
        self.ptr += VALUE_SIZE

    def overwrite(self, offset, value):
        start = int(offset * VALUE_SIZE)
        self.data[start: start + VALUE_SIZE] = (value).to_bytes(VALUE_SIZE, byteorder="big", signed=True)

    def read(self, offset):
        start = int(offset * VALUE_SIZE)
        return int.from_bytes(self.data[start: start + VALUE_SIZE], byteorder='big', signed=True)

class Base_Page:
    def __init__(self, num_columns):
        self.num_records = 0
        self.num_columns = num_columns
        self.total_columns = METADATA_COLUMNS + num_columns
        self.columns = [Page() for _ in range(self.total_columns)]

    def has_capacity(self):
        return self.num_records < VALUES_PER_PAGE

    def write(self, *data):
        if not self.has_capacity():
            return None
        for i, value in enumerate(data):
            self.columns[i].write(value)
        slot = self.num_records
        self.num_records += 1
        return slot

    def read(self, slot, column_index):
        return self.columns[column_index].read(slot)

    def overwrite(self, slot, column_index, value):
        self.columns[column_index].overwrite(slot, value)

class Tail_Page:
    def __init__(self, num_columns):
        self.num_records = 0
        self.num_columns = num_columns
        # Milestone 1: tail pages match base pages
        self.total_columns = METADATA_COLUMNS + num_columns
        self.columns = [Page() for _ in range(self.total_columns)]

    def has_capacity(self):
        return self.num_records < VALUES_PER_PAGE

    def append(self, *data):
        if not self.has_capacity():
            return None
        for i, value in enumerate(data):
            self.columns[i].write(value)
        slot = self.num_records
        self.num_records += 1
        return slot

    def read(self, slot, column_index):
        return self.columns[column_index].read(slot)

    def overwrite(self, slot, column_index, value):
        self.columns[column_index].overwrite(slot, value)

class Page_Range:
    def __init__(self, num_columns, num_base_pages=16):
        self.num_columns = num_columns
        self.num_base_pages = num_base_pages
        self.base_pages = [Base_Page(num_columns) for _ in range(num_base_pages)]
        self.tail_pages = [Tail_Page(num_columns)]
        self.current_base_page_index = 0

    # -------------------------
    # BASE PAGE OPERATIONS
    # -------------------------

    def write_base(self, *data):
        while self.current_base_page_index < self.num_base_pages:
            bp = self.base_pages[self.current_base_page_index]
            if bp.has_capacity():
                slot = bp.write(*data)
                # print("PR wrote data", data)
                return (self.current_base_page_index, slot)
            self.current_base_page_index += 1
        return None

    def read_base(self, base_page_index, slot, column_index):
        return self.base_pages[base_page_index].read(slot, column_index)

    def overwrite_base(self, base_page_index, slot, column_index, value):
        self.base_pages[base_page_index].overwrite(slot, column_index, value)

    def base_is_full(self):
        return self.current_base_page_index >= self.num_base_pages

    # -------------------------
    # TAIL PAGE OPERATIONS
    # -------------------------

    def append_tail(self, *data):
        tail_page = self.tail_pages[-1]
        if not tail_page.has_capacity():
            tail_page = Tail_Page(self.num_columns)
            self.tail_pages.append(tail_page)
        slot = tail_page.append(*data)
        tail_page_index = len(self.tail_pages) - 1
        return (tail_page_index, slot)

    def read_tail(self, tail_page_index, slot, column_index):
        return self.tail_pages[tail_page_index].read(slot, column_index)

    def overwrite_tail(self, tail_page_index, slot, column_index, value):
        self.tail_pages[tail_page_index].overwrite(slot, column_index, value)

