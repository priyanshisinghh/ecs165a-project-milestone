from typing import Any, Dict, Optional, Tuple
from lstore.page import Page, PAGE_SIZE, VALUE_SIZE

# new - updated PageId to 5-tuple to include page_range_id
# old was (table, "base"/"tail", col, page_index) - 4 elements
# new is  (table, page_range_id, "base"/"tail", col, page_index) - 5 elements
PageId = Tuple[str, int, str, int, int]


class Frame:
    def __init__(self, page, dirty, dirty_seq, pin_count, last_used):
        self.page = page
        self.dirty = dirty
        self.dirty_seq = dirty_seq
        self.pin_count = pin_count
        self.last_used = last_used


# fixed size bufferpool with pin and unpin, dirty tracking, least recently used eviction, and disk read/write
class Bufferpool:
    def __init__(self, capacity_pages, disk_manager):
        self.capacity = capacity_pages
        self.disk = disk_manager
        self.frames: Dict[PageId, Frame] = {}

        self.clock = 0

        # checkpoint for dirty pages, make sure newly dirtied pages are not marked clean
        self.dirty_clock = 0

    # records when a frame was last used
    def last_touched(self, frame):
        self.clock += 1
        frame.last_used = self.clock

    def next_dirty_seq(self):
        self.dirty_clock += 1
        return self.dirty_clock

    # writes a page to disk
    def write_page_to_disk(self, page_id, page):
        # new - pass raw bytes to disk, not the page object itself
        self.disk.write_page(page_id, bytes(page.data))

    # pins and returns page, must be used with unpin
    def get_page(self, page_id):
        frame = self.frames.get(page_id)

        # check if frame is already in bufferpool
        if frame is not None:
            frame.pin_count += 1
            self.last_touched(frame)
            return frame.page

        # bufferpool is full, need to kick someone out
        if len(self.frames) >= self.capacity:
            self.evict_one()

        # new - ask disk for raw bytes, reconstruct a Page object from them
        page_data = self.disk.read_page(page_id)

        if page_data is None:
            # page doesn't exist on disk yet, make a fresh empty one
            page = Page()
        else:
            page = Page()
            page.data = bytearray(page_data)
            # new - figure out where the written portion ends so ptr is correct
            page.ptr = self._find_ptr(page)
            page.num_records = page.ptr // VALUE_SIZE

        # bring new page into bufferpool
        frame = Frame(page=page, dirty=False, dirty_seq=0, pin_count=1, last_used=0)
        self.last_touched(frame)
        self.frames[page_id] = frame
        return page

    # new - scan backwards through the page data to find where written data ends
    # this lets us correctly set ptr when loading a page back from disk
    def _find_ptr(self, page):
        ptr = PAGE_SIZE
        while ptr > 0:
            chunk = page.data[ptr - VALUE_SIZE: ptr]
            if any(b != 0 for b in chunk):
                break
            ptr -= VALUE_SIZE
        return ptr

    def mark_dirty(self, page_id):
        frame = self.frames.get(page_id)
        if frame is None:
            return
        frame.dirty = True
        frame.dirty_seq = self.next_dirty_seq()

    def unpin(self, page_id):
        frame = self.frames.get(page_id)
        # check if page is able to be unpinned
        if frame is None:
            raise KeyError(f"unpin called for non-cached page_id = {page_id}")
        if frame.pin_count <= 0:
            raise RuntimeError(f"unpin called when pin_count already 0 for {page_id}")
        frame.pin_count -= 1

    # writes page from memory to disk
    def flush_page(self, page_id, max_dirty_seq=None):
        while True:
            frame = self.frames.get(page_id)
            # check if the frame can be written from memory to disk
            if frame is None or not frame.dirty:
                return
            if max_dirty_seq is not None and frame.dirty_seq > max_dirty_seq:
                return

            page_snapshot = frame.page
            dirty_seq_snapshot = frame.dirty_seq

            try:
                self.write_page_to_disk(page_id, page_snapshot)
            except Exception as e:
                raise RuntimeError(f"Disk write failed for page_id={page_id}") from e

            frame = self.frames.get(page_id)
            if frame is None:
                return
            if frame.dirty and frame.dirty_seq == dirty_seq_snapshot:
                frame.dirty = False
                return

    # flush all dirty pages - pages written after this call are not guaranteed to be flushed
    def flush_all(self):
        cutoff_dirty_seq = self.dirty_clock

        while True:
            pending = [
                pid
                for pid, frame in self.frames.items()
                if frame.dirty and frame.dirty_seq <= cutoff_dirty_seq
            ]

            if not pending:
                return

            for pid in pending:
                self.flush_page(pid, max_dirty_seq=cutoff_dirty_seq)

    # using LRU to pick which page to evict
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

        # remove first then write back if dirty
        victim_frame = self.frames.pop(victim_id)

        if not victim_frame.dirty:
            return

        try:
            self.write_page_to_disk(victim_id, victim_frame.page)
        except Exception as e:
            # restore cache entry if eviction write-back fails
            self.frames[victim_id] = victim_frame
            raise RuntimeError(f"Disk write failed during eviction for {victim_id}") from e
