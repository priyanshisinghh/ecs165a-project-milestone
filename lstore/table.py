from lstore.index import Index
from time import time
from lstore.page import *
from lstore.bufferpool import Bufferpool
import os
import json
import threading


class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class Table:
    def __init__(self, name, num_columns, key, disk):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.disk = disk
        self.bufferpool = Bufferpool(32, disk)
        self.page_ranges = []
        self.page_directory = {}
        self.index = Index(self)
        self.rid_counter = 0
        self.tail_rid_counter = -1
        self.update_count = 0
        self.MERGE_THRESHOLD = 10
        self.merged = False
        self.last_merged = -1

        #methods that hold it can call other methods that also acquire it
        self.dir_lock = threading.RLock()

        #Guards rid_counter and tail_rid_counter
        self.rid_lock = threading.Lock()

        #Serializes the duplicate-key check + page_directory/index write in insert()
        self.insert_lock = threading.Lock()
        self.add_page_range()


    def add_page_range(self):
        self.page_ranges.append(Page_Range(self.num_columns))

    def new_base_rid(self):
        with self.rid_lock:
            rid = self.rid_counter
            self.rid_counter += 1
            return rid

    def new_tail_rid(self):
        with self.rid_lock:
            rid = self.tail_rid_counter
            self.tail_rid_counter -= 1
            return rid

    def page_id(self, pr_index, page_type, col_index, page_index):
        return (self.name, pr_index, page_type, col_index, page_index)

    #read/write one base/tail col
    def read_base_col(self, location, col_index):
        page_id = self.page_id(location['page_range'], 'base', col_index, location['page_index'])
        page = self.bufferpool.get_page(page_id)
        val = page.read(location['slot'])
        self.bufferpool.unpin(page_id)
        return val

    def write_base_col(self, location, col_index, value):
        page_id = self.page_id(location['page_range'], 'base', col_index, location['page_index'])
        page = self.bufferpool.get_page(page_id)
        page.overwrite(location['slot'], value)
        self.bufferpool.mark_dirty(page_id)
        self.bufferpool.unpin(page_id)

    def read_tail_col(self, location, col_index):
        page_id = self.page_id(location['page_range'], 'tail', col_index, location['page_index'])
        page = self.bufferpool.get_page(page_id)
        val = page.read(location['slot'])
        self.bufferpool.unpin(page_id)
        return val

    def insert(self, data):
        if len(data) != self.num_columns:
            return False

        with self.insert_lock:
            if self.index.locate(self.key, data[self.key]):
                return False

            rid = self.new_base_rid()

            #find page range, base page, slot, and insert values
            with self.dir_lock:
                pr_index = len(self.page_ranges) - 1
                if self.page_ranges[pr_index].base_is_full():
                    self.add_page_range()
                    pr_index += 1

                pr = self.page_ranges[pr_index]

                while pr.current_base_page_index < pr.num_base_pages:
                    if pr.base_pages[pr.current_base_page_index].has_capacity():
                        break
                    pr.current_base_page_index += 1

                if pr.current_base_page_index >= pr.num_base_pages:
                    return False

                bp_index = pr.current_base_page_index
                slot = pr.base_pages[bp_index].num_records
                pr.base_pages[bp_index].num_records += 1

            all_values = [0, rid, int(time()), 0] + list(data)
            for col_index, value in enumerate(all_values):
                page_id = self._page_id(pr_index, 'base', col_index, bp_index)
                page = self.bufferpool.get_page(page_id)
                page.write(value)
                self.bufferpool.mark_dirty(page_id)
                self.bufferpool.unpin(page_id)

            #add to page directory/index
            with self.dir_lock:
                self.page_directory[rid] = {
                    'is_base': True,
                    'page_range': pr_index,
                    'page_index': bp_index,
                    'slot': slot
                }
                for i, val in enumerate(data):
                    if self.index.indices[i] is None:
                        self.index.create_index(i)
                    self.index.insert_entry(i, val, rid)

        return True


    def update(self, primary_key, data):
        #check if record exists
        rids = self.index.locate(self.key, primary_key)
        if not rids:
            return False

        base_rid = rids[0]

        #read under dir_lock so we don't race with a concurrent delete or another update on same record.
        with self.dir_lock:
            if base_rid not in self.page_directory:
                return False
            base_loc = self.page_directory[base_rid]
            prev_indirection = self.read_base_col(base_loc, INDIRECTION_COLUMN)
            old_data = self.get_latest_values(base_rid)

        schema_encoding = 0
        new_data = list(data)
        if new_data[self.key] is not None and new_data[self.key] != primary_key:
            return False
        for i, val in enumerate(new_data):
            if val is None:
                new_data[i] = old_data[i]
            else:
                schema_encoding |= (1 << (self.num_columns - i - 1))

        tail_rid = self.new_tail_rid()

        with self.dir_lock:
            pr_index = base_loc['page_range']
            pr = self.page_ranges[pr_index]
            
            tail_page = pr.tail_pages[-1]
            if not tail_page.has_capacity():
                tail_page = Tail_Page(self.num_columns)
                pr.tail_pages.append(tail_page)
            tp_index = len(pr.tail_pages) - 1
            slot = tail_page.num_records
            tail_page.num_records += 1

        tail_meta = [prev_indirection, tail_rid, int(time()), schema_encoding, base_rid]
        full_tail_data = tail_meta + new_data

        for col_index, value in enumerate(full_tail_data):
            page_id = self.page_id(pr_index, 'tail', col_index, tp_index)
            page = self.bufferpool.get_page(page_id)
            page.write(value)
            self.bufferpool.mark_dirty(page_id)
            self.bufferpool.unpin(page_id)

        #insert into page directory and index
        with self.dir_lock:
            self.page_directory[tail_rid] = {
                'is_base': False,
                'page_range': pr_index,
                'page_index': tp_index,
                'slot': slot
            }
            self.write_base_col(base_loc, INDIRECTION_COLUMN, tail_rid)
            
            for i, val in enumerate(data):
                if val is not None and self.index.indices[i] is not None:
                    self.index.update_entry(i, old_data[i], val, base_rid)

        self.update_count += 1

        #check for merge
        with self.dir_lock:
            tail_pages = len(pr.tail_pages)
        if (tail_pages % self.MERGE_THRESHOLD == 0
                and tail_pages > self.last_merged
                and not self.merged):
            self.merge(pr_index)
            self.merged = True
            self.last_merged = tail_pages
        elif tail_pages != self.last_merged:
            self.merged = False

        return True


    def get_latest_values(self, base_rid):
        #caller must already hold dir_lock 
        base_loc = self.page_directory[base_rid]
        indirection = self.read_base_col(base_loc, INDIRECTION_COLUMN)
        if indirection == 0:
            return [self.read_base_col(base_loc, BASE_METADATA_COLUMNS + i)
                    for i in range(self.num_columns)]
        else:
            tail_loc = self.page_directory[indirection]
            return [self.read_tail_col(tail_loc, TAIL_METADATA_COLUMNS + i)
                    for i in range(self.num_columns)]

    def read_record(self, base_rid, projected_columns):
        with self.dir_lock:
            if base_rid not in self.page_directory:
                return False
            base_loc = self.page_directory[base_rid]
            if not base_loc['is_base']:
                return False

            indirection = self.read_base_col(base_loc, INDIRECTION_COLUMN)
            pr = self.page_ranges[base_loc['page_range']]
            tps = pr.get_tps(base_loc['page_index'])

            #tps == 0 means no merge has happened yet
            already_merged = (tps != 0) and (indirection != 0) and (indirection >= tps)

            values = [None] * self.num_columns
            if indirection == 0 or already_merged:
                for i in range(self.num_columns):
                    if projected_columns[i]:
                        values[i] = self.read_base_col(base_loc,
                                                        BASE_METADATA_COLUMNS + i)
            else:
                if indirection not in self.page_directory:
                    return False
                tail_loc = self.page_directory[indirection]
                for i in range(self.num_columns):
                    if projected_columns[i]:
                        values[i] = self.read_tail_col(tail_loc,
                                                        TAIL_METADATA_COLUMNS + i)

        return Record(base_rid, values[self.key], values)

    def read_record_version(self, base_rid, projected_columns, relative_version):
        with self.dir_lock:
            if base_rid not in self.page_directory:
                return False
            base_loc = self.page_directory[base_rid]
            indirection = self.read_base_col(base_loc, INDIRECTION_COLUMN)
            values = [None] * self.num_columns

            #no updates at all, return base values directly
            if indirection == 0:
                for i in range(self.num_columns):
                    if projected_columns[i]:
                        values[i] = self.read_base_col(base_loc, BASE_METADATA_COLUMNS + i)
                return Record(base_rid, values[self.key], values)

            if relative_version >= 0:
                return self.read_record(base_rid, projected_columns)

            #walk backward "steps" times through the indirection chain
            steps = -relative_version
            current_rid = indirection

            for _ in range(steps):
                loc = self.page_directory.get(current_rid)
                if loc is None or loc['is_base']:
                    current_rid = None
                    break
                prev = self._read_tail_col(loc, INDIRECTION_COLUMN)
                #if next "hop" exits tail chain
                if (prev == 0 or prev not in self.page_directory or self.page_directory[prev]['is_base']):
                    current_rid = None
                    break
                current_rid = prev

            if current_rid is None:
                for i in range(self.num_columns):
                    if projected_columns[i]:
                        values[i] = self.read_base_col(base_loc, BASE_METADATA_COLUMNS + i)
            else:
                tail_loc = self.page_directory[current_rid]
                for i in range(self.num_columns):
                    if projected_columns[i]:
                        values[i] = self.read_tail_col(tail_loc, TAIL_METADATA_COLUMNS + i)

        return Record(base_rid, values[self.key], values)

    def merge(self, pr_index):
        with self.dir_lock:
            pr = self.page_ranges[pr_index]
            if not pr.tail_pages:
                return

            #snapshot page_directory under the lock
            tail_entries = sorted(
                [(rid, loc) for rid, loc in self.page_directory.items()
                 if not loc['is_base'] and loc['page_range'] == pr_index],
                key = lambda x: x[0]
            )

        if not tail_entries:
            return

        #find the latest values per base_rid 
        seen = {}
        for tail_rid, tail_loc in tail_entries:
            base_rid_val = self.read_tail_col(tail_loc, BASE_RID_COLUMN)
            if base_rid_val in seen:
                continue
            with self.dir_lock:
                if base_rid_val not in self.page_directory:
                    continue
                base_loc = self.page_directory[base_rid_val]
            if not base_loc['is_base'] or base_loc['page_range'] != pr_index:
                continue
            vals = [self.read_tail_col(tail_loc, TAIL_METADATA_COLUMNS + col) for col in range(self.num_columns)]
            seen[base_rid_val] = (base_loc, vals)

        if not seen:
            return

        #write merged values back to base pages
        for base_rid_val, (base_loc, vals) in seen.items():
            bp_i = base_loc['page_index']
            base_slot = base_loc['slot']
            for col in range(self.num_columns):
                pid = self.page_id(pr_index, 'base', BASE_METADATA_COLUMNS + col, bp_i)
                page = self.bufferpool.get_page(pid)
                page.overwrite(base_slot, vals[col])
                self.bufferpool.mark_dirty(pid)
                self.bufferpool.unpin(pid)

        last_tail_rid = tail_entries[0][0]
        total_cols = BASE_METADATA_COLUMNS + self.num_columns
        with self.dir_lock:
            pr = self.page_ranges[pr_index]
            for bp_i in range(len(pr.base_pages)):
                pr.set_tps(bp_i, last_tail_rid)
                for col_index in range(total_cols):
                    page_id = self.page_id(pr_index, 'base', col_index, bp_i)
                    self.disk.write_tps(page_id, last_tail_rid)

    #save data to disk
    def save_to_disk(self, db_path):
        self.bufferpool.flush_all()
        with self.dir_lock:
            meta = {
                'rid_counter': self.rid_counter,
                'tail_rid_counter': self.tail_rid_counter,
                'page_directory': {str(k): v for k, v in self.page_directory.items()},
                'pr_base_record_counts': [[bp.num_records for bp in pr.base_pages]
                                          for pr in self.page_ranges],
                'pr_tail_record_counts': [[tp.num_records for tp in pr.tail_pages]
                                          for pr in self.page_ranges],
                'tps': [pr.tps for pr in self.page_ranges],
            }
        os.makedirs(os.path.join(db_path, self.name), exist_ok = True)
        with open(os.path.join(db_path, self.name, 'meta.json'), 'w') as f:
            json.dump(meta, f)

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
            pr = Page_Range(self.num_columns, num_base_pages = len(base_counts))
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
        self.rebuild_indexes()

    def rebuild_indexes(self):
        #runs single-threaded at load time, no lock needed
        for rid, loc in self.page_directory.items():
            if not loc['is_base']:
                continue
            latest = self.get_latest_values(rid)
            for i in range(self.num_columns):
                if self.index.indices[i] is not None:
                    self.index.insert_entry(i, latest[i], rid)
