from typing import Dict, Tuple
from lstore.page import Page, PAGE_SIZE, VALUE_SIZE

# page_id: (table_name, page_range_id, "base"/"tail", col_index, page_index)
PageId = Tuple[str, int, str, int, int]


class Frame:
    def __init__(self, page, dirty, dirty_seq, pin_count, last_used):
        self.page = page
        self.dirty = dirty
        self.dirty_seq = dirty_seq
        self.pin_count = pin_count
        self.last_used = last_used


# fixed size bufferpool with pin/unpin, dirty tracking, LRU eviction, and disk read/write
class Bufferpool:
    def __init__(self, capacity_pages, disk_manager):
        self.capacity = capacity_pages
        self.disk = disk_manager
        self.frames: Dict[PageId, Frame] = {}
        self.clock = 0
        self.dirty_clock = 0

    def _tick(self, frame):
        self.clock += 1
        frame.last_used = self.clock

    def _next_dirty_seq(self):
        self.dirty_clock += 1
        return self.dirty_clock

    # new - scan backwards to find where written data ends so ptr is correct after loading from disk
    def _find_ptr(self, page):
        ptr = PAGE_SIZE
        while ptr > 0:
            chunk = page.data[ptr - VALUE_SIZE: ptr]
            if any(b != 0 for b in chunk):
                break
            ptr -= VALUE_SIZE
        return ptr

    # pins and returns page, must be used with unpin
    def get_page(self, page_id):
        frame = self.frames.get(page_id)

        # already in bufferpool
        if frame is not None:
            frame.pin_count += 1
            self._tick(frame)
            return frame.page

        if len(self.frames) >= self.capacity:
            self.evict_one()

        # new - read raw bytes from disk and reconstruct a Page object
        page_data = self.disk.read_page(page_id)
        if page_data is None:
            page = Page()
        else:
            page = Page()
            page.data = bytearray(page_data)
            page.ptr = self._find_ptr(page)
            page.num_records = page.ptr // VALUE_SIZE

        frame = Frame(page=page, dirty=False, dirty_seq=0, pin_count=1, last_used=0)
        self._tick(frame)
        self.frames[page_id] = frame
        return page

    def mark_dirty(self, page_id):
        frame = self.frames.get(page_id)
        if frame is None:
            return
        frame.dirty = True
        frame.dirty_seq = self._next_dirty_seq()

    def unpin(self, page_id):
        frame = self.frames.get(page_id)
        if frame is None:
            raise KeyError(f"unpin called for non-cached page_id = {page_id}")
        if frame.pin_count <= 0:
            raise RuntimeError(f"unpin called when pin_count already 0 for {page_id}")
        frame.pin_count -= 1

    # writes page from memory to disk
    def flush_page(self, page_id, max_dirty_seq=None):
        while True:
            frame = self.frames.get(page_id)
            if frame is None or not frame.dirty:
                return
            if max_dirty_seq is not None and frame.dirty_seq > max_dirty_seq:
                return
            snap_page = frame.page
            snap_seq = frame.dirty_seq
            self.disk.write_page(page_id, bytes(snap_page.data))
            frame = self.frames.get(page_id)
            if frame is None:
                return
            if frame.dirty and frame.dirty_seq == snap_seq:
                frame.dirty = False
                return

    # flush all dirty pages - pages dirtied after this call are not guaranteed to be flushed
    def flush_all(self):
        cutoff = self.dirty_clock
        while True:
            pending = [pid for pid, f in self.frames.items() if f.dirty and f.dirty_seq <= cutoff]
            if not pending:
                return
            for pid in pending:
                self.flush_page(pid, max_dirty_seq=cutoff)

    # using LRU to pick which unpinned page to evict
    def evict_one(self):
        victim_id = None
        victim_last_used = None
        for pid, frame in self.frames.items():
            if frame.pin_count != 0:
                continue
            if victim_id is None or frame.last_used < victim_last_used:
                victim_id = pid
                victim_last_used = frame.last_used
        if victim_id is None:
            raise RuntimeError("Bufferpool full and all pages are pinned; cannot evict.")
        victim_frame = self.frames.pop(victim_id)
        if not victim_frame.dirty:
            return
        try:
            self.disk.write_page(victim_id, bytes(victim_frame.page.data))
        except Exception as e:
            self.frames[victim_id] = victim_frame
            raise RuntimeError(f"Disk write failed during eviction for {victim_id}") from e
