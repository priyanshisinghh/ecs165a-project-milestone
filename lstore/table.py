from lstore.index import Index
from time import time
from lstore.page import Base_Page, Tail_Page, Page_Range, METADATA_COLUMNS, VALUES_PER_PAGE
import threading

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

BASE_RID_COLUMN = 4        # extra tail metadata: which base record this tail belongs to
TAIL_METADATA_COLUMNS = 5  # 4 standard + 1 base_rid


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
        self.page_ranges = []             # list of Page_Range
        self.page_directory = {}          # RID -> location dict
        self.index = Index(self)
        self.rid_counter = 0
        self.tail_rid_counter = -1        # tail RIDs count DOWN from -1
        self.merge_lock = threading.Lock()
        self.update_count = 0
        self.MERGE_THRESHOLD = 200
        self._add_page_range()            # start with one page range

    def _add_page_range(self):
        self.page_ranges.append(Page_Range(self.num_columns))

    def _new_base_rid(self):
        rid = self.rid_counter
        self.rid_counter += 1
        return rid

    def _new_tail_rid(self):
        rid = self.tail_rid_counter
        self.tail_rid_counter -= 1
        return rid

    # ------------------------------------------------------------------
    # LOCATION HELPERS
    # ------------------------------------------------------------------

    def _read_base_col(self, location, col_index):
        pr = self.page_ranges[location['page_range']]
        return pr.read_base(location['page_index'], location['slot'], col_index)

    def _write_base_col(self, location, col_index, value):
        pr = self.page_ranges[location['page_range']]
        pr.overwrite_base(location['page_index'], location['slot'], col_index, value)

    def _read_tail_col(self, location, col_index):
        pr = self.page_ranges[location['page_range']]
        return pr.read_tail(location['page_index'], location['slot'], col_index)

    # ------------------------------------------------------------------
    # INSERT
    # ------------------------------------------------------------------

    def insert(self, data):
        if len(data) != self.num_columns:
            return False

        # check primary key uniqueness
        if self.index.locate(self.key, data[self.key]):
            return False

        rid = self._new_base_rid()
        metadata = [0, rid, int(time()), 0]  # indirection=0 means no updates

        # find a page range with space, or add a new one
        pr_index = len(self.page_ranges) - 1
        if self.page_ranges[pr_index].base_is_full():
            self._add_page_range()
            pr_index += 1

        pr = self.page_ranges[pr_index]
        result = pr.write_base(*(metadata + data))
        if result is None:
            return False

        bp_index, slot = result
        self.page_directory[rid] = {
            'is_base': True,
            'page_range': pr_index,
            'page_index': bp_index,
            'slot': slot
        }

        for i, val in enumerate(data):
            self.index.insert_entry(i, val, rid)

        return True

    # ------------------------------------------------------------------
    # UPDATE
    # ------------------------------------------------------------------

    def update(self, primary_key, data):
        rids = self.index.locate(self.key, primary_key)
        if not rids:
            return False

        base_rid = rids[0]
        base_loc = self.page_directory[base_rid]

        # read current indirection (points to most recent tail, or 0 if never updated)
        prev_indirection = self._read_base_col(base_loc, INDIRECTION_COLUMN)

        # get old values to fill in None columns (cumulative update)
        old_data = self._get_latest_values(base_rid)

        schema_encoding = 0
        new_data = list(data)
        for i, val in enumerate(new_data):
            if val is None:
                new_data[i] = old_data[i]
            else:
                schema_encoding |= (1 << (self.num_columns - i - 1))

        tail_rid = self._new_tail_rid()
        timestamp = int(time())

        # tail metadata: indirection=prev tail, rid, timestamp, schema, base_rid
        tail_meta = [prev_indirection, tail_rid, timestamp, schema_encoding, base_rid]

        # append to same page range as the base record
        pr_index = base_loc['page_range']
        pr = self.page_ranges[pr_index]
        tp_index, slot = pr.append_tail(*(tail_meta + new_data))

        self.page_directory[tail_rid] = {
            'is_base': False,
            'page_range': pr_index,
            'page_index': tp_index,
            'slot': slot
        }

        # update base record's indirection to point to this new tail record
        self._write_base_col(base_loc, INDIRECTION_COLUMN, tail_rid)

        # update secondary indexes
        for i, val in enumerate(data):
            if val is not None and self.index.indices[i] is not None:
                self.index.update_entry(i, old_data[i], val, base_rid)

        self.update_count += 1
        if self.update_count >= self.MERGE_THRESHOLD:
            self.update_count = 0
            t = threading.Thread(target=self.__merge, daemon=True)
            t.start()

        return True

    # ------------------------------------------------------------------
    # READ HELPERS
    # ------------------------------------------------------------------

    def _get_latest_values(self, base_rid):
        """Returns the latest column values for a base RID."""
        base_loc = self.page_directory[base_rid]
        indirection = self._read_base_col(base_loc, INDIRECTION_COLUMN)

        if indirection == 0:
            # never updated — read from base directly
            return [
                self._read_base_col(base_loc, METADATA_COLUMNS + i)
                for i in range(self.num_columns)
            ]
        else:
            tail_loc = self.page_directory[indirection]
            return [
                self._read_tail_col(tail_loc, METADATA_COLUMNS + i)
                for i in range(self.num_columns)
            ]

    def read_record(self, base_rid, projected_columns):
        """
        Returns a Record for base_rid respecting projected_columns.
        Handles TPS check to avoid redundant tail lookups.
        """
        base_loc = self.page_directory[base_rid]
        indirection = self._read_base_col(base_loc, INDIRECTION_COLUMN)

        values = [None] * self.num_columns

        # check TPS: if indirection tail RID is already merged, read from base directly
        pr = self.page_ranges[base_loc['page_range']]
        tps = pr.get_tps(base_loc['page_index'])

        # tail RIDs are negative and count down; a MORE negative RID is newer
        # if indirection >= tps (less negative or equal), it's already merged
        tail_already_merged = (indirection != 0) and (indirection >= tps)

        if indirection == 0 or tail_already_merged:
            for i in range(self.num_columns):
                if projected_columns[i]:
                    values[i] = self._read_base_col(base_loc, METADATA_COLUMNS + i)
        else:
            tail_loc = self.page_directory[indirection]
            for i in range(self.num_columns):
                if projected_columns[i]:
                    values[i] = self._read_tail_col(tail_loc, METADATA_COLUMNS + i)

        return Record(base_rid, values[self.key], values)

    # ------------------------------------------------------------------
    # MERGE
    # ------------------------------------------------------------------

    def __merge(self):
        """
        Background merge: for each page range, consolidate tail records into
        a new copy of base pages. Then swap the page directory.
        """
        with self.merge_lock:
            for pr_index, pr in enumerate(self.page_ranges):

                # snapshot of how many tail pages exist right now (don't touch new ones)
                tail_page_snapshot = len(pr.tail_pages)
                if tail_page_snapshot == 0:
                    continue

                # Step 1: copy base pages
                new_base_pages = [Base_Page(self.num_columns) for _ in pr.base_pages]
                for bp_i, old_bp in enumerate(pr.base_pages):
                    for slot in range(old_bp.num_records):
                        row = [old_bp.read(slot, c) for c in range(old_bp.total_columns)]
                        new_base_pages[bp_i].write(*row)

                # Step 2: scan tail records in REVERSE order, newest first
                # track latest update seen per base RID
                seen = {}  # base_rid -> True
                last_tail_rid = None

                for tp_i in range(tail_page_snapshot - 1, -1, -1):
                    tp = pr.tail_pages[tp_i]
                    for slot in range(tp.num_records - 1, -1, -1):
                        tail_rid = tp.read(slot, RID_COLUMN)
                        base_rid = tp.read(slot, BASE_RID_COLUMN)

                        if last_tail_rid is None:
                            last_tail_rid = tail_rid

                        if base_rid in seen:
                            continue  # already applied a newer update for this record
                        seen[base_rid] = True

                        # find where this base_rid lives
                        if base_rid not in self.page_directory:
                            continue
                        base_loc = self.page_directory[base_rid]
                        if base_loc['page_range'] != pr_index:
                            continue

                        bp_i = base_loc['page_index']
                        base_slot = base_loc['slot']

                        # apply tail data columns to the new base page copy
                        for col in range(self.num_columns):
                            val = tp.read(slot, METADATA_COLUMNS + col)
                            new_base_pages[bp_i].overwrite(base_slot, METADATA_COLUMNS + col, val)

                # Step 3: swap page directory to point to new base pages
                # (this is the only foreground-touching part)
                for rid, loc in list(self.page_directory.items()):
                    if not loc['is_base']:
                        continue
                    if loc['page_range'] != pr_index:
                        continue
                    # location stays the same (same bp_index and slot), just the
                    # physical page object is replaced
                    pass  # page_directory entries don't change — we swap the objects

                # Step 4: replace page objects in the page range
                pr.base_pages = new_base_pages

                # Step 5: update TPS for each base page
                if last_tail_rid is not None:
                    for bp_i in range(len(pr.base_pages)):
                        pr.set_tps(bp_i, last_tail_rid)
