from lstore.index import Index
from time import time
from lstore.page import (Base_Page, Tail_Page, Page_Range,
                         BASE_METADATA_COLUMNS, TAIL_METADATA_COLUMNS, VALUE_SIZE,
                         INDIRECTION_COLUMN, RID_COLUMN, TIMESTAMP_COLUMN,
                         SCHEMA_ENCODING_COLUMN, BASE_RID_COLUMN)
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
    :param disk: DiskManager    #Shared disk manager passed in from Database
    """
    def __init__(self, name, num_columns, key, disk):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.disk = disk
        #200 pages in bufferpool disk
        self.bufferpool = Bufferpool(200, disk)
        self.page_ranges = []
        #rid to is_base, page range, page index, slot
        self.page_directory = {}
        self.index = Index(self)
        #avoids collisions
        self.rid_counter = 0
        self.tail_rid_counter = -1
        self.update_count = 0
        self.MERGE_THRESHOLD = 200 #merge at 200 updates
        self.merge_lock = threading.Lock()
        self._add_page_range()

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
      
    def _page_id(self, pr_index, page_type, col_index, page_index):
        return (self.name, pr_index, page_type, col_index, page_index)

    #read one value from a base page column through bufferpool
    def _read_base_col(self, location, col_index):
        page_id = self._page_id(location['page_range'], 'base', col_index, location['page_index'])
        page = self.bufferpool.get_page(page_id)
        val = page.read(location['slot'])
        self.bufferpool.unpin(page_id)
        return val

    #write one value in a base page column through bufferpool
    def _write_base_col(self, location, col_index, value):
        page_id = self._page_id(location['page_range'], 'base', col_index, location['page_index'])
        page = self.bufferpool.get_page(page_id)
        page.overwrite(location['slot'], value)
        self.bufferpool.mark_dirty(page_id)
        self.bufferpool.unpin(page_id)

    #read one value from a tail page column through bufferpool
    def _read_tail_col(self, location, col_index):
        page_id = self._page_id(location['page_range'], 'tail', col_index, location['page_index'])
        page = self.bufferpool.get_page(page_id)
        val = page.read(location['slot'])
        self.bufferpool.unpin(page_id)
        return val

    def insert(self, data):
        if len(data) != self.num_columns:
            return False

        if self.index.locate(self.key, data[self.key]):
            return False

        rid = self._new_base_rid()
        # indirection = 0 means never updated
        metadata = [0, rid, int(time()), 0]

        pr_index = len(self.page_ranges) - 1
        if self.page_ranges[pr_index].base_is_full():
            self._add_page_range()
            pr_index += 1

        pr = self.page_ranges[pr_index]
        result = pr.write_base(*(metadata + data))
        if result is None:
            return False

        bp_index, slot = result

        # new - mark all base page columns dirty so bufferpool flushes them
        for col_index in range(METADATA_COLUMNS + self.num_columns):
            page_id = self._page_id(pr_index, 'base', col_index, bp_index)
            page = self.bufferpool.get_page(page_id)
            self.bufferpool.mark_dirty(page_id)
            self.bufferpool.unpin(page_id)

        self.page_directory[rid] = {
            'is_base': True,
            'page_range': pr_index,
            'page_index': bp_index,
            'slot': slot
        }

        for i, val in enumerate(data):
            self.index.insert_entry(i, val, rid)

        return True

    def update(self, primary_key, data):
        rids = self.index.locate(self.key, primary_key)
        if not rids:
            return False

        base_rid = rids[0]
        base_loc = self.page_directory[base_rid]
        prev_indirection = self._read_base_col(base_loc, INDIRECTION_COLUMN)

        # get current values to fill in None columns (cumulative update)
        old_data = self._get_latest_values(base_rid)

        schema_encoding = 0
        new_data = list(data)
        for i, val in enumerate(new_data):
            if val is None:
                # fill in unchanged columns with current value so tail record is cumulative
                new_data[i] = old_data[i]
            else:
                schema_encoding |= (1 << (self.num_columns - i - 1))

        tail_rid = self._new_tail_rid()
        pr_index = base_loc['page_range']
        pr = self.page_ranges[pr_index]

        # find which tail page has space, allocate slot from in-memory object only
        tail_page = pr.tail_pages[-1]
        if not tail_page.has_capacity():
            tail_page = Tail_Page(self.num_columns)
            pr.tail_pages.append(tail_page)

        tp_index = len(pr.tail_pages) - 1
        slot = tail_page.num_records
        tail_page.num_records += 1

        # new - tail metadata includes base_rid so merge knows which base record this belongs to
        tail_meta = [prev_indirection, tail_rid, int(time()), schema_encoding, base_rid]
        full_tail_data = tail_meta + new_data

        # write every column through bufferpool so data and disk stay in sync
        for col_index, value in enumerate(full_tail_data):
            page_id = self._page_id(pr_index, 'tail', col_index, tp_index)
            page = self.bufferpool.get_page(page_id)
            page.write(value)
            self.bufferpool.mark_dirty(page_id)
            self.bufferpool.unpin(page_id)

        self.page_directory[tail_rid] = {
            'is_base': False,
            'page_range': pr_index,
            'page_index': tp_index,
            'slot': slot
        }

        # point base record indirection to this new tail record
        self._write_base_col(base_loc, INDIRECTION_COLUMN, tail_rid)

        # update secondary indexes for changed columns
        for i, val in enumerate(data):
            if val is not None and self.index.indices[i] is not None:
                self.index.update_entry(i, old_data[i], val, base_rid)

        # kick off background merge after enough updates
        self.update_count += 1
        if self.update_count >= self.MERGE_THRESHOLD:
            self.update_count = 0
            threading.Thread(target=self.__merge, daemon=True).start()

        return True

    def _get_latest_values(self, base_rid):
        base_loc = self.page_directory[base_rid]
        indirection = self._read_base_col(base_loc, INDIRECTION_COLUMN)
        if indirection == 0:
            # never been updated, read straight from base columns
            return [self._read_base_col(base_loc, METADATA_COLUMNS + i) for i in range(self.num_columns)]
        else:
            # has been updated - tail records are cumulative so latest tail has ALL column values
            tail_loc = self.page_directory[indirection]
            return [self._read_tail_col(tail_loc, TAIL_METADATA_COLUMNS + i) for i in range(self.num_columns)]

    def read_record(self, base_rid, projected_columns):
        if base_rid not in self.page_directory:
            return False
        base_loc = self.page_directory[base_rid]
        if not base_loc['is_base']:
            return False

        indirection = self._read_base_col(base_loc, INDIRECTION_COLUMN)
        values = [None] * self.num_columns

        # new - use TPS to skip tail lookup if the update is already merged into base page
        pr = self.page_ranges[base_loc['page_range']]
        tps = pr.get_tps(base_loc['page_index'])
        already_merged = (indirection != 0) and (tps != 0) and (indirection >= tps)

        if indirection == 0 or already_merged:
            for i in range(self.num_columns):
                if projected_columns[i]:
                    values[i] = self._read_base_col(base_loc, METADATA_COLUMNS + i)
        else:
            tail_loc = self.page_directory[indirection]
            for i in range(self.num_columns):
                if projected_columns[i]:
                    values[i] = self._read_tail_col(tail_loc, TAIL_METADATA_COLUMNS + i)

        return Record(base_rid, values[self.key], values)

    def read_record_version(self, base_rid, projected_columns, relative_version):
        if base_rid not in self.page_directory:
            return False
        base_loc = self.page_directory[base_rid]
        indirection = self._read_base_col(base_loc, INDIRECTION_COLUMN)
        values = [None] * self.num_columns

        # no updates, only base exists
        if indirection == 0:
            for i in range(self.num_columns):
                if projected_columns[i]:
                    values[i] = self._read_base_col(base_loc, METADATA_COLUMNS + i)
            return Record(base_rid, values[self.key], values)

        # latest version
        if relative_version >= 0:
            tail_loc = self.page_directory[indirection]
            for i in range(self.num_columns):
                if projected_columns[i]:
                    values[i] = self._read_tail_col(tail_loc, TAIL_METADATA_COLUMNS + i)
            return Record(base_rid, values[self.key], values)

        # walk backwards through tail chain for older versions
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
            for i in range(self.num_columns):
                if projected_columns[i]:
                    values[i] = self._read_base_col(base_loc, METADATA_COLUMNS + i)
        else:
            tail_loc = self.page_directory[current_rid]
            for i in range(self.num_columns):
                if projected_columns[i]:
                    values[i] = self._read_tail_col(tail_loc, TAIL_METADATA_COLUMNS + i)

        return Record(base_rid, values[self.key], values)

    # new - background merge: consolidates tail records into base pages
    # runs in a background thread so normal queries keep going unblocked
    def __merge(self):
        with self.merge_lock:
            for pr_index, pr in enumerate(self.page_ranges):
                tail_snapshot = len(pr.tail_pages)
                if tail_snapshot == 0:
                    continue

                # step 1: copy current base pages so we don't touch the live ones
                new_base_pages = [Base_Page(self.num_columns) for _ in pr.base_pages]
                for bp_i, old_bp in enumerate(pr.base_pages):
                    for slot in range(old_bp.num_records):
                        row = [old_bp.read(slot, c) for c in range(old_bp.total_columns)]
                        new_base_pages[bp_i].write(*row)

                # step 2: scan tail records newest first, apply only the latest per base record
                seen = {}
                last_tail_rid = None
                for tp_i in range(tail_snapshot - 1, -1, -1):
                    tp = pr.tail_pages[tp_i]
                    for slot in range(tp.num_records - 1, -1, -1):
                        tail_rid = tp.read(slot, RID_COLUMN)
                        base_rid = tp.read(slot, BASE_RID_COLUMN)
                        if last_tail_rid is None:
                            last_tail_rid = tail_rid
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
                        for col in range(self.num_columns):
                            val = tp.read(slot, TAIL_METADATA_COLUMNS + col)
                            new_base_pages[bp_i].overwrite(base_slot, METADATA_COLUMNS + col, val)

                # step 3: write merged pages through bufferpool
                for bp_i, new_bp in enumerate(new_base_pages):
                    for col_index in range(new_bp.total_columns):
                        page_id = self._page_id(pr_index, 'base', col_index, bp_i)
                        page = self.bufferpool.get_page(page_id)
                        page.data = bytearray(new_bp.columns[col_index].data)
                        page.ptr = new_bp.columns[col_index].ptr
                        page.num_records = new_bp.columns[col_index].num_records
                        self.bufferpool.mark_dirty(page_id)
                        self.bufferpool.unpin(page_id)

                # step 4: swap in the merged base pages
                pr.base_pages = new_base_pages

                # step 5: update TPS so readers know these tail records are already in the base page
                if last_tail_rid is not None:
                    for bp_i in range(len(pr.base_pages)):
                        pr.set_tps(bp_i, last_tail_rid)
                        for col_index in range(METADATA_COLUMNS + self.num_columns):
                            page_id = self._page_id(pr_index, 'base', col_index, bp_i)
                            self.disk.write_tps(page_id, last_tail_rid)

    # new - flush all pages and write table metadata to disk for db.close()
    def save_to_disk(self, db_path):
        self.bufferpool.flush_all()
        meta = {
            'rid_counter': self.rid_counter,
            'tail_rid_counter': self.tail_rid_counter,
            'page_directory': {str(k): v for k, v in self.page_directory.items()},
            'pr_base_record_counts': [[bp.num_records for bp in pr.base_pages] for pr in self.page_ranges],
            'pr_tail_record_counts': [[tp.num_records for tp in pr.tail_pages] for pr in self.page_ranges],
            'tps': [pr.tps for pr in self.page_ranges],
        }
        os.makedirs(os.path.join(db_path, self.name), exist_ok=True)
        with open(os.path.join(db_path, self.name, 'meta.json'), 'w') as f:
            json.dump(meta, f)

    # new - rebuild table structure from saved metadata for db.open()
    # pages are lazy-loaded from disk by bufferpool on first access
    def load_from_disk(self, db_path):
        meta_path = os.path.join(db_path, self.name, 'meta.json')
        if not os.path.exists(meta_path):
            return
        with open(meta_path, 'r') as f:
            meta = json.load(f)
        self.rid_counter = meta['rid_counter']
        self.tail_rid_counter = meta['tail_rid_counter']
        self.page_directory = {int(k): v for k, v in meta['page_directory'].items()}
        self.page_ranges = []
        for base_counts, tail_counts, tps in zip(
            meta['pr_base_record_counts'],
            meta['pr_tail_record_counts'],
            meta['tps']
        ):
            pr = Page_Range(self.num_columns, num_base_pages=len(base_counts))
            for j, count in enumerate(base_counts):
                pr.base_pages[j].num_records = count
                for col in range(pr.base_pages[j].total_columns):
                    pr.base_pages[j].columns[col].ptr = count * VALUE_SIZE
                    pr.base_pages[j].columns[col].num_records = count
            pr.tail_pages = []
            for count in tail_counts:
                tp = Tail_Page(self.num_columns)
                tp.num_records = count
                for col in range(tp.total_columns):
                    tp.columns[col].ptr = count * VALUE_SIZE
                    tp.columns[col].num_records = count
                pr.tail_pages.append(tp)
            pr.tps = tps
            self.page_ranges.append(pr)
        # rebuild indexes since we don't persist them
        self._rebuild_indexes()

    def _rebuild_indexes(self):
        for rid, loc in self.page_directory.items():
            if not loc['is_base']:
                continue
            for i in range(self.num_columns):
                if self.index.indices[i] is not None:
                    val = self._read_base_col(loc, METADATA_COLUMNS + i)
                    self.index.insert_entry(i, val, rid)
