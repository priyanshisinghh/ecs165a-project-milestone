from lstore.index import Index
from time import time
from lstore.page import Base_Page, Tail_Page, METADATA_COLUMNS, Page_Range
from lstore.bufferpool import Bufferpool

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
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns

        self.page_ranges = [Page_Range(num_columns)]
        self.page_directory = {}
        self.index = Index(self)

        self.rid_num = 0
        self.merge_threshold_pages = 50

    # -------------------------
    # INSERT
    # -------------------------

    def insert(self, data):
        if len(data) != self.num_columns:
            raise ValueError("Wrong number of columns")

        rid = self.rid_num
        self.rid_num += 1

        metadata = [-1, rid, int(time()), 0]

        pr = self.page_ranges[-1]
        loc = pr.write_base(*(metadata + data))

        if loc is None:
            pr = Page_Range(self.num_columns)
            self.page_ranges.append(pr)
            loc = pr.write_base(*(metadata + data))

        base_page_index, slot = loc
        pr_index = len(self.page_ranges) - 1

        self.page_directory[rid] = (True, pr_index, base_page_index, slot)

        for col, val in enumerate(data):
            self.index.insert_entry(col, val, rid)

        return True

    # -------------------------
    # UPDATE
    # -------------------------

    def update(self, primary_key, data):
        rids = self.index.locate(self.key, primary_key)
        if not rids:
            return False

        rid = rids[0]
        is_base, pr_idx, bp_idx, slot = self.page_directory[rid]
        pr = self.page_ranges[pr_idx]

        prev_tail = pr.read_base(bp_idx, slot, INDIRECTION_COLUMN)

        new_rid = self.rid_num
        self.rid_num += 1

        old_data = []
        if prev_tail == -1:
            for i in range(self.num_columns):
                old_data.append(
                    pr.read_base(bp_idx, slot, 4 + i)
                )
        else:
            _, t_pr, t_pg, t_slot = self.page_directory[prev_tail]
            tpr = self.page_ranges[t_pr]
            for i in range(self.num_columns):
                old_data.append(
                    tpr.read_tail(t_pg, t_slot, 5 + i)
                )

        schema = 0
        for i in range(self.num_columns):
            if data[i] is None:
                data[i] = old_data[i]
            else:
                schema |= (1 << (self.num_columns - i - 1))

        metadata = [prev_tail, new_rid, int(time()), schema, rid]
        t_pg, t_slot = pr.append_tail(*(metadata + data))

        self.page_directory[new_rid] = (False, pr_idx, t_pg, t_slot)
        pr.overwrite_base(bp_idx, slot, INDIRECTION_COLUMN, new_rid)

        return True
    