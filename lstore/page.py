VALUE_SIZE = 8
METADATA_COLUMNS = 4
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
# new - extra column on tail records that stores which base record this tail belongs to
# this is how the merge knows where to apply each tail update
BASE_RID_COLUMN = 4
# new - tail pages have one extra metadata column compared to base pages
TAIL_METADATA_COLUMNS = 5

PAGE_SIZE = 4096
VALUES_PER_PAGE = PAGE_SIZE // VALUE_SIZE  # 512 records per page
MAX_RECORDS_PER_BASE_PAGE = VALUES_PER_PAGE  # 512


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
            self.data[self.ptr: self.ptr + VALUE_SIZE] = (0).to_bytes(VALUE_SIZE, byteorder='big', signed=True)
        else:
            self.data[self.ptr:self.ptr + VALUE_SIZE] = (value).to_bytes(VALUE_SIZE, byteorder="big", signed=True)
        self.num_records += 1
        self.ptr += VALUE_SIZE

    def overwrite(self, offset, value):
        start = int(offset * VALUE_SIZE)
        self.data[start: start + VALUE_SIZE] = (value).to_bytes(VALUE_SIZE, byteorder="big", signed=True)

    def read(self, offset):
        start = int(offset * VALUE_SIZE)
        return int.from_bytes(self.data[start: start + VALUE_SIZE], byteorder='big', signed=True)


class Base_Page:
    """
    Holds one page per column (metadata + data).
    Each Base_Page holds up to VALUES_PER_PAGE (512) records.
    """
    def __init__(self, num_columns):
        self.num_records = 0
        self.num_columns = num_columns
        self.total_columns = METADATA_COLUMNS + num_columns
        # one Page object per column
        self.columns = [Page() for _ in range(self.total_columns)]

    def has_capacity(self):
        return self.num_records < VALUES_PER_PAGE

    def write(self, *data):
        """
        Write a full record (metadata + data columns) to this base page.
        Returns the slot index (offset within this page).
        """
        if not self.has_capacity():
            return None
        for i, value in enumerate(data):
            self.columns[i].write(value)
        slot = self.num_records
        self.num_records += 1
        return slot

    def read(self, slot, column_index):
        """Read value at slot for a given column index."""
        return self.columns[column_index].read(slot)

    def overwrite(self, slot, column_index, value):
        """Overwrite value at slot for a given column (e.g. update indirection)."""
        self.columns[column_index].overwrite(slot, value)


class Tail_Page:
    """
    Append-only, holds one page per column (metadata + data).
    Each Tail_Page holds up to VALUES_PER_PAGE (512) records.
    new - total_columns uses TAIL_METADATA_COLUMNS (5) instead of METADATA_COLUMNS (4)
          because tail records also store the base_rid they belong to
    """
    def __init__(self, num_columns):
        self.num_records = 0
        self.num_columns = num_columns
        # new - tail pages have 5 metadata columns (base + base_rid column)
        self.total_columns = TAIL_METADATA_COLUMNS + num_columns
        # one Page object per column
        self.columns = [Page() for _ in range(self.total_columns)]

    def has_capacity(self):
        return self.num_records < VALUES_PER_PAGE

    def append(self, *data):
        """
        Append a full record (metadata + data columns) to this tail page.
        Returns the slot index (offset within this page).
        """
        if not self.has_capacity():
            return None
        for i, value in enumerate(data):
            self.columns[i].write(value)
        slot = self.num_records
        self.num_records += 1
        return slot

    def read(self, slot, column_index):
        """Read value at slot for a given column index."""
        return self.columns[column_index].read(slot)

    def overwrite(self, slot, column_index, value):
        """Overwrite value at slot for a given column (e.g. update indirection)."""
        self.columns[column_index].overwrite(slot, value)


class Page_Range:
    """
    A Page_Range groups a set of Base_Pages and their corresponding Tail_Pages.
    - base_pages: fixed list of Base_Page objects (filled left to right)
    - tail_pages: grows dynamically as updates come in
    """
    def __init__(self, num_columns, num_base_pages=16):
        self.num_columns = num_columns
        self.num_base_pages = num_base_pages

        # pre-allocate base pages for this range
        self.base_pages = [Base_Page(num_columns) for _ in range(num_base_pages)]

        # tail pages grow dynamically; start with one
        self.tail_pages = [Tail_Page(num_columns)]

        # track which base page is currently being written to
        self.current_base_page_index = 0

        # TPS: Tail Page Sequence Number
        # tracks the RID of the last tail record merged into each base page
        # starts at 0 meaning nothing has been merged yet
        self.tps = [0] * num_base_pages

    # -------------------------
    # BASE PAGE OPERATIONS
    # -------------------------

    def write_base(self, *data):
        """
        Write a record into the current base page.
        Moves to next base page if current is full.
        Returns (base_page_index, slot) or None if range is full.
        """
        while self.current_base_page_index < self.num_base_pages:
            bp = self.base_pages[self.current_base_page_index]
            if bp.has_capacity():
                slot = bp.write(*data)
                return (self.current_base_page_index, slot)
            self.current_base_page_index += 1

        return None  # page range is full

    def read_base(self, base_page_index, slot, column_index):
        """Read a value from a specific base page slot and column."""
        return self.base_pages[base_page_index].read(slot, column_index)

    def overwrite_base(self, base_page_index, slot, column_index, value):
        """Overwrite a value in a base page (e.g. update indirection column)."""
        self.base_pages[base_page_index].overwrite(slot, column_index, value)

    def base_is_full(self):
        """Returns True if all base pages in this range are full."""
        return self.current_base_page_index >= self.num_base_pages

    # -------------------------
    # TAIL PAGE OPERATIONS
    # -------------------------

    def append_tail(self, *data):
        """
        Append a tail record. Allocates a new Tail_Page if current one is full.
        Returns (tail_page_index, slot).
        """
        tail_page = self.tail_pages[-1]
        if not tail_page.has_capacity():
            tail_page = Tail_Page(self.num_columns)
            self.tail_pages.append(tail_page)

        slot = tail_page.append(*data)
        tail_page_index = len(self.tail_pages) - 1
        return (tail_page_index, slot)

    def read_tail(self, tail_page_index, slot, column_index):
        """Read a value from a specific tail page slot and column."""
        return self.tail_pages[tail_page_index].read(slot, column_index)

    def overwrite_tail(self, tail_page_index, slot, column_index, value):
        """Overwrite a value in a tail page (e.g. update indirection)."""
        self.tail_pages[tail_page_index].overwrite(slot, column_index, value)

    # -------------------------
    # TPS (MERGE LINEAGE)
    # -------------------------

    def get_tps(self, base_page_index):
        """Return the TPS for a given base page."""
        return self.tps[base_page_index]

    def set_tps(self, base_page_index, tail_rid):
        """Update TPS after a merge completes for a base page."""
        self.tps[base_page_index] = tail_rid
