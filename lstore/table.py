from lstore.index import Index
from time import time
from lstore.page import (Base_Page, Tail_Page, Page_Range,
                         METADATA_COLUMNS, TAIL_METADATA_COLUMNS,
                         VALUES_PER_PAGE, VALUE_SIZE,
                         INDIRECTION_COLUMN, RID_COLUMN,
                         TIMESTAMP_COLUMN, SCHEMA_ENCODING_COLUMN,
                         BASE_RID_COLUMN)
from lstore.bufferpool import Bufferpool
import threading
import os
import json


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
    :param disk: DiskManager    #Shared disk manager from Database
    """
    def __init__(self, name, num_columns, key, disk):
        self.name = name
        self.key = key
        self.num_columns = num_columns

        # new - disk manager passed in from Database so all tables share one
        self.disk = disk
        # new - bufferpool wired to disk manager, 200 pages in memory at once
        self.bufferpool = Bufferpool(200, disk)

        # new - list of Page_Range objects instead of a single base/tail page
        self.page_ranges = []

        # page_directory maps RID -> location dict
        # location dict looks like:
        #   { 'is_base': True, 'page_range': 0, 'page_index': 2, 'slot': 47 }
        self.page_directory = {}

        self.index = Index(self)

        # new - base RIDs count up from 0
        self.rid_counter = 0
        # new - tail RIDs count DOWN from -1 so they never collide with base RIDs
        self.tail_rid_counter = -1

        # new - lock to protect the merge swapping the page directory
        self.merge_lock = threading.Lock()

        # new - how many updates before we kick off a background merge
        self.update_count = 0
        self.MERGE_THRESHOLD = 200

        # start with one empty page range
        self._add_page_range()

    # ------------------------------------------------------------------
    # INTERNAL HELPERS
    # ------------------------------------------------------------------

    def _add_page_range(self):
        self.page_ranges.append(Page_Range(self.num_columns))

    def _new_base_rid(self):
        rid = self.rid_counter
        self.rid_counter += 1
        return rid

    def _new_tail_rid(self):
        # new - tail RIDs go negative so they are always distinct from base RIDs
        rid = self.tail_rid_counter
        self.tail_rid_counter -= 1
        return rid

    # new - build the 5-tuple page_id that bufferpool and disk both understand
    # page_type is "base" or "tail"
    def _get_page_id(self, pr_index, page_type, col_index, page_index):
        return (self.name, pr_index, page_type, col_index, page_index)

    # new - read a single value from a base page column going through bufferpool
    def _read_base_col(self, location, col_index):
        pr_index = location['page_range']
        bp_index = location['page_index']
        slot = location['slot']
        pr = self.page_ranges[pr_index]
        # figure out which physical Page within the Base_Page column we need
        page_id = self._get_page_id(pr_index, 'base', col_index, bp_index)
        page = self.bufferpool.get_page(page_id)
        val = page.read(slot)
        self.bufferpool.unpin(page_id)
        return val

    # new - overwrite a single value in a base page column going through bufferpool
    def _write_base_col(self, location, col_index, value):
        pr_index = location['page_range']
        bp_index = location['page_index']
        slot = location['slot']
        page_id = self._get_page_id(pr_index, 'base', col_index, bp_index)
        page = self.bufferpool.get_page(page_id)
        page.overwrite(slot, value)
        self.bufferpool.mark_dirty(page_id)
        self.bufferpool.unpin(page_id)

    # new - read a single value from a tail page column going through bufferpool
    def _read_tail_col(self, location, col_index):
        pr_index = location['page_range']
        tp_index = location['page_index']
        slot = location['slot']
        page_id = self._get_page_id(pr_index, 'tail', col_index, tp_index)
        page = self.bufferpool.get_page(page_id)
        val = page.read(slot)
        self.bufferpool.unpin(page_id)
        return val

    # new - append a full record row to a base page going through bufferpool
    # writes all columns (metadata + data) for one record
    def _append_base_row(self, pr_index, bp_index, slot, full_data):
        for col_index, value in enumerate(full_data):
            page_id = self._get_page_id(pr_index, 'base', col_index, bp_index)
            page = self.bufferpool.get_page(page_id)
            # writing at a specific slot during merge (not just appending to end)
            page.overwrite(slot, value if value is not None else 0)
            self.bufferpool.mark_dirty(page_id)
            self.bufferpool.unpin(page_id)

    # new - append a full record row to a tail page going through bufferpool
    def _append_tail_row(self, pr_index, tp_index, full_data):
        for col_index, value in enumerate(full_data):
            page_id = self._get_page_id(pr_index, 'tail', col_index, tp_index)
            page = self.bufferpool.get_page(page_id)
            page.write(value if value is not None else 0)
            self.bufferpool.mark_dirty(page_id)
            self.bufferpool.unpin(page_id)

    # ------------------------------------------------------------------
    # INSERT
    # ------------------------------------------------------------------

    def insert(self, data):
        if len(data) != self.num_columns:
            return False

        # don't allow duplicate primary keys
        if self.index.locate(self.key, data[self.key]):
            return False

        rid = self._new_base_rid()
        # indirection = 0 means this record has never been updated
        metadata = [0, rid, int(time()), 0]

        # find a page range with space, or open a new one
        pr_index = len(self.page_ranges) - 1
        if self.page_ranges[pr_index].base_is_full():
            self._add_page_range()
            pr_index += 1

        pr = self.page_ranges[pr_index]
        result = pr.write_base(*(metadata + data))
        if result is None:
            return False

        bp_index, slot = result

        # new - write each column through bufferpool so disk is aware of changes
        full_data = metadata + data
        for col_index, value in enumerate(full_data):
            page_id = self._get_page_id(pr_index, 'base', col_index, bp_index)
            page = self.bufferpool.get_page(page_id)
            # the Page_Range already wrote to the in-memory Page object,
            # we just need to mark it dirty so bufferpool flushes it
            self.bufferpool.mark_dirty(page_id)
            self.bufferpool.unpin(page_id)

        self.page_directory[rid] = {
            'is_base': True,
            'page_range': pr_index,
            'page_index': bp_index,
            'slot': slot
        }

        # update index for all columns
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

        # read indirection: 0 = never updated, negative = points to a tail RID
        prev_indirection = self._read_base_col(base_loc, INDIRECTION_COLUMN)

        # get the current latest values so we can fill in None columns
        # (cumulative update: every tail record holds ALL column values, not just changed ones)
        old_data = self._get_latest_values(base_rid)

        schema_encoding = 0
        new_data = list(data)
        for i, val in enumerate(new_data):
            if val is None:
                # fill in unchanged columns with current value
                new_data[i] = old_data[i]
            else:
                # mark this column as changed in schema encoding bitmap
                schema_encoding |= (1 << (self.num_columns - i - 1))

        tail_rid = self._new_tail_rid()
        timestamp = int(time())

        # new - tail metadata includes base_rid so merge can find the right base record
        # layout: [indirection, rid, timestamp, schema_encoding, base_rid, col0, col1, ...]
        tail_meta = [prev_indirection, tail_rid, timestamp, schema_encoding, base_rid]

        pr_index = base_loc['page_range']
        pr = self.page_ranges[pr_index]
        tp_index, slot = pr.append_tail(*(tail_meta + new_data))

        # new - mark all tail page columns dirty through bufferpool
        full_tail_data = tail_meta + new_data
        for col_index in range(len(full_tail_data)):
            page_id = self._get_page_id(pr_index, 'tail', col_index, tp_index)
            page = self.bufferpool.get_page(page_id)
            self.bufferpool.mark_dirty(page_id)
            self.bufferpool.unpin(page_id)

        self.page_directory[tail_rid] = {
            'is_base': False,
            'page_range': pr_index,
            'page_index': tp_index,
            'slot': slot
        }

        # point base record's indirection to this new tail record
        self._write_base_col(base_loc, INDIRECTION_COLUMN, tail_rid)

        # update secondary indexes for changed columns
        for i, val in enumerate(data):
            if val is not None and self.index.indices[i] is not None:
                self.index.update_entry(i, old_data[i], val, base_rid)

        # new - trigger background merge if we've hit the threshold
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
        """Returns the latest column values for a base RID as a plain list."""
        base_loc = self.page_directory[base_rid]
        indirection = self._read_base_col(base_loc, INDIRECTION_COLUMN)

        if indirection == 0:
            # never been updated, read straight from base
            return [
                self._read_base_col(base_loc, METADATA_COLUMNS + i)
                for i in range(self.num_columns)
            ]
        else:
            tail_loc = self.page_directory[indirection]
            # new - tail data columns start at TAIL_METADATA_COLUMNS (5), not METADATA_COLUMNS (4)
            return [
                self._read_tail_col(tail_loc, TAIL_METADATA_COLUMNS + i)
                for i in range(self.num_columns)
            ]

    def read_record(self, base_rid, projected_columns):
        """
        Returns a Record for base_rid respecting projected_columns.
        Uses TPS to skip the tail lookup if the update is already merged into the base page.
        """
        if base_rid not in self.page_directory:
            return False

        base_loc = self.page_directory[base_rid]
        if not base_loc['is_base']:
            return False

        indirection = self._read_base_col(base_loc, INDIRECTION_COLUMN)

        values = [None] * self.num_columns

        # new - check TPS to see if we even need to look at tail pages
        # tail RIDs are negative, so "more negative" = newer
        # if indirection >= tps (less negative or zero), the update is already in the base page
        pr = self.page_ranges[base_loc['page_range']]
        tps = pr.get_tps(base_loc['page_index'])
        tail_already_merged = (indirection != 0) and (indirection >= tps) and (tps != 0)

        if indirection == 0 or tail_already_merged:
            # read directly from base page
            for i in range(self.num_columns):
                if projected_columns[i]:
                    values[i] = self._read_base_col(base_loc, METADATA_COLUMNS + i)
        else:
            # follow indirection to latest tail record
            tail_loc = self.page_directory[indirection]
            for i in range(self.num_columns):
                if projected_columns[i]:
                    # new - tail data starts at column index TAIL_METADATA_COLUMNS
                    values[i] = self._read_tail_col(tail_loc, TAIL_METADATA_COLUMNS + i)

        return Record(base_rid, values[self.key], values)

    def read_record_version(self, base_rid, projected_columns, relative_version):
        """
        Returns a Record at a historical version.
        relative_version=0 is latest, -1 is one step back, etc.
        """
        if base_rid not in self.page_directory:
            return False

        base_loc = self.page_directory[base_rid]
        indirection = self._read_base_col(base_loc, INDIRECTION_COLUMN)
        values = [None] * self.num_columns

        # no updates ever, only base exists
        if indirection == 0:
            for i in range(self.num_columns):
                if projected_columns[i]:
                    values[i] = self._read_base_col(base_loc, METADATA_COLUMNS + i)
            return Record(base_rid, values[self.key], values)

        if relative_version >= 0:
            # latest version
            tail_loc = self.page_directory[indirection]
            for i in range(self.num_columns):
                if projected_columns[i]:
                    values[i] = self._read_tail_col(tail_loc, TAIL_METADATA_COLUMNS + i)
            return Record(base_rid, values[self.key], values)

        # walk backwards through the tail chain for older versions
        steps = -relative_version
        current_rid = indirection

        for _ in range(steps):
            if current_rid == 0 or current_rid not in self.page_directory:
                current_rid = None
                break
            loc = self.page_directory[current_rid]
            if loc['is_base']:
                current_rid = None
                break
            prev = self._read_tail_col(loc, INDIRECTION_COLUMN)
            if prev == 0 or prev not in self.page_directory or self.page_directory[prev]['is_base']:
                current_rid = None
                break
            current_rid = prev

        if current_rid is None:
            # fell all the way back to base
            for i in range(self.num_columns):
                if projected_columns[i]:
                    values[i] = self._read_base_col(base_loc, METADATA_COLUMNS + i)
        else:
            tail_loc = self.page_directory[current_rid]
            for i in range(self.num_columns):
                if projected_columns[i]:
                    values[i] = self._read_tail_col(tail_loc, TAIL_METADATA_COLUMNS + i)

        return Record(base_rid, values[self.key], values)

    # ------------------------------------------------------------------
    # MERGE
    # ------------------------------------------------------------------

    def __merge(self):
        """
        Background merge: consolidate tail records into base pages.
        Runs in a background thread so normal queries keep going.
        The only thing that touches the main thread is the page directory swap at the end.
        """
        with self.merge_lock:
            for pr_index, pr in enumerate(self.page_ranges):

                # snapshot how many tail pages exist right now
                # we don't touch tail pages added after this snapshot
                tail_page_snapshot = len(pr.tail_pages)
                if tail_page_snapshot == 0:
                    continue

                # step 1: make a fresh copy of all base pages for this range
                new_base_pages = [Base_Page(self.num_columns) for _ in pr.base_pages]
                for bp_i, old_bp in enumerate(pr.base_pages):
                    for slot in range(old_bp.num_records):
                        row = [old_bp.read(slot, c) for c in range(old_bp.total_columns)]
                        new_base_pages[bp_i].write(*row)

                # step 2: scan tail records NEWEST FIRST
                # only apply the first (newest) update we see per base record
                # this skips all the stale older updates automatically
                seen = {}       # base_rid -> True, tracks records we already handled
                last_tail_rid = None

                for tp_i in range(tail_page_snapshot - 1, -1, -1):
                    tp = pr.tail_pages[tp_i]
                    for slot in range(tp.num_records - 1, -1, -1):
                        tail_rid = tp.read(slot, RID_COLUMN)
                        # new - BASE_RID_COLUMN tells us which base record this tail belongs to
                        base_rid = tp.read(slot, BASE_RID_COLUMN)

                        # keep track of the newest tail RID we touched for TPS
                        if last_tail_rid is None:
                            last_tail_rid = tail_rid

                        # already applied a newer update for this record, skip this one
                        if base_rid in seen:
                            continue
                        seen[base_rid] = True

                        if base_rid not in self.page_directory:
                            continue

                        base_loc = self.page_directory[base_rid]
                        if base_loc['page_range'] != pr_index:
                            continue

                        bp_i = base_loc['page_index']
                        base_slot = base_loc['slot']

                        # copy the tail's data columns into the new base page copy
                        for col in range(self.num_columns):
                            val = tp.read(slot, TAIL_METADATA_COLUMNS + col)
                            new_base_pages[bp_i].overwrite(base_slot, METADATA_COLUMNS + col, val)

                # step 3: write the new merged base pages to disk through bufferpool
                for bp_i, new_bp in enumerate(new_base_pages):
                    for col_index in range(new_bp.total_columns):
                        page_id = self._get_page_id(pr_index, 'base', col_index, bp_i)
                        page = self.bufferpool.get_page(page_id)
                        # replace the page data with the merged version
                        page.data = bytearray(new_bp.columns[col_index].data)
                        page.ptr = new_bp.columns[col_index].ptr
                        page.num_records = new_bp.columns[col_index].num_records
                        self.bufferpool.mark_dirty(page_id)
                        self.bufferpool.unpin(page_id)

                # step 4: swap the in-memory base page objects
                # page_directory entries don't need to change since bp_index and slot are the same
                pr.base_pages = new_base_pages

                # step 5: update TPS so future reads know these tail records are already merged
                if last_tail_rid is not None:
                    for bp_i in range(len(pr.base_pages)):
                        pr.set_tps(bp_i, last_tail_rid)
                        # new - also persist TPS to disk so it survives a restart
                        for col_index in range(METADATA_COLUMNS + self.num_columns):
                            page_id = self._get_page_id(pr_index, 'base', col_index, bp_i)
                            self.disk.write_tps(page_id, last_tail_rid)

    # ------------------------------------------------------------------
    # SAVE AND LOAD (for db.open / db.close)
    # ------------------------------------------------------------------

    def save_to_disk(self, db_path):
        """Flush all buffered pages and write table metadata to disk."""
        # flush all dirty pages out of bufferpool to their files
        self.bufferpool.flush_all()

        # save everything we need to reconstruct the table on next open
        meta = {
            'rid_counter': self.rid_counter,
            'tail_rid_counter': self.tail_rid_counter,
            'page_directory': {str(k): v for k, v in self.page_directory.items()},
            'num_page_ranges': len(self.page_ranges),
            # new - save how many records are in each page so ptr/num_records restore correctly
            'pr_base_record_counts': [
                [bp.num_records for bp in pr.base_pages]
                for pr in self.page_ranges
            ],
            'pr_tail_record_counts': [
                [tp.num_records for tp in pr.tail_pages]
                for tp in self.page_ranges
            ],
            'tps': [pr.tps for pr in self.page_ranges],
        }

        table_dir = os.path.join(db_path, self.name)
        os.makedirs(table_dir, exist_ok=True)
        with open(os.path.join(table_dir, 'meta.json'), 'w') as f:
            json.dump(meta, f)

    def load_from_disk(self, db_path):
        """Reconstruct table state from metadata file. Pages lazy-load via bufferpool."""
        table_dir = os.path.join(db_path, self.name)
        meta_path = os.path.join(table_dir, 'meta.json')
        if not os.path.exists(meta_path):
            return

        with open(meta_path, 'r') as f:
            meta = json.load(f)

        self.rid_counter = meta['rid_counter']
        self.tail_rid_counter = meta['tail_rid_counter']
        # new - keys were stringified for JSON, convert back to int
        self.page_directory = {int(k): v for k, v in meta['page_directory'].items()}

        # rebuild page range structure
        # we don't load page data here — bufferpool will pull from disk on first access
        self.page_ranges = []
        for i, (base_counts, tail_counts, tps) in enumerate(zip(
            meta['pr_base_record_counts'],
            meta['pr_tail_record_counts'],
            meta['tps']
        )):
            pr = Page_Range(self.num_columns, num_base_pages=len(base_counts))
            # restore record counts so has_capacity() and ptr work correctly
            for j, count in enumerate(base_counts):
                pr.base_pages[j].num_records = count
                for col in range(pr.base_pages[j].total_columns):
                    pr.base_pages[j].columns[col].ptr = count * VALUE_SIZE
                    pr.base_pages[j].columns[col].num_records = count
            pr.tail_pages = []
            for j, count in enumerate(tail_counts):
                tp = Tail_Page(self.num_columns)
                tp.num_records = count
                for col in range(tp.total_columns):
                    tp.columns[col].ptr = count * VALUE_SIZE
                    tp.columns[col].num_records = count
                pr.tail_pages.append(tp)
            pr.tps = tps
            self.page_ranges.append(pr)

        # new - rebuild indexes from scratch since we don't persist them
        self._rebuild_indexes()

    def _rebuild_indexes(self):
        """Walk all base records and re-insert them into every active index."""
        for rid, loc in self.page_directory.items():
            if not loc['is_base']:
                continue
            for i in range(self.num_columns):
                if self.index.indices[i] is not None:
                    val = self._read_base_col(loc, METADATA_COLUMNS + i)
                    self.index.insert_entry(i, val, rid)
