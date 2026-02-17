from typing import Any, Dict, Optional, Tuple

PageId = Tuple[str, str, int, int]  # (table, "base"/"tail", col, page_index)


class Frame:
    def __init__(self, page: Any, dirty: bool = False, dirty_seq: int = 0, pin_count: int = 0, last_used: int = 0):
        self.page = page
        self.dirty = dirty
        self.dirty_seq = dirty_seq
        self.pin_count = pin_count
        self.last_used = last_used


#fixed size bifferpool with pin and unpin, dirty tracking, least recently used eviction, and disk read/write
class Bufferpool:
    def __init__(self, capacity_pages: int, disk_manager):
        #make sure we have any pages and a disk manager
        if capacity_pages <= 0:
            raise ValueError(f"capacity pages must be > 0, got {capacity_pages}")
        if disk_manager is None:
            raise ValueError("disk manager cannot be None")

        self.capacity = capacity_pages
        self.disk = disk_manager
        self.frames: Dict[PageId, Frame] = {}

        self.clock = 0
        self.dirty_clock = 0

    #records when a frame was last used
    def last_touched(self, frame: Frame):
        self.clock += 1
        frame.last_used = self.clock

    def next_dirty_seq(self) -> int:
        self.dirty_clock += 1
        return self.dirty_clock

    #writes a page to disk
    def write_page_to_disk(self, page_id: PageId, page: Any):
        table, page_type, col, page_index = page_id
        self.disk.write_page(table, page_type, col, page_index, page)

    #pins and returns page, must be used with unpin
    def get_page(self, page_id: PageId):
        frame = self.frames.get(page_id)

        #check if frame exists and if there is space
        if frame is not None:
            frame.pin_count += 1
            self.last_touched(frame)
            return frame.page

        if len(self.frames) >= self.capacity:
            self.evict_one()

        table, page_type, col, page_index = page_id
        try:
            page = self.disk.read_page(table, page_type, col, page_index)
        except Exception as e:
            raise RuntimeError(f"Disk read failed for page_id = {page_id}") from e

        if page is None:
            raise RuntimeError(f"Disk read returned None for page_id=  {page_id}")

        frame = Frame(page = page, dirty = False, dirty_seq = 0, pin_count = 1, last_used = 0)
        self.last_touched(frame)
        self.frames[page_id] = frame
        return page

    def mark_dirty(self, page_id: PageId):
        frame = self.frames.get(page_id)
        if frame is None:
            raise KeyError(f"mark_dirty called for non-cached page_id = {page_id}")
        frame.dirty = True
        frame.dirty_seq = self.next_dirty_seq()

    def unpin(self, page_id: PageId):
        frame = self.frames.get(page_id)
        #check if page is able to be unpinned
        if frame is None:
            raise KeyError(f"unpin called for non-cached page_id = {page_id}")
        if frame.pin_count <= 0:
            raise RuntimeError(f"unpin called when pin_count already 0 for {page_id}")
        frame.pin_count -= 1

    #writes page from memory to disk
    def flush_page(self, page_id: PageId, max_dirty_seq: Optional[int] = None):
        while True:
            frame = self.frames.get(page_id)
            #check if the frame can be written from memory to disk
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

    def flush_all(self):
        """
        Flushes all pages dirty at the start of this call.
        Pages dirtied after this call begins are not guaranteed to be flushed.
        """
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

    def evict_one(self):
        """
        Evict one unpinned page using LRU.
        """
        victim_id: Optional[PageId] = None
        victim_last_used: Optional[int] = None

        for pid, frame in self.frames.items():
            if frame.pin_count != 0:
                continue
            if victim_id is None or frame.last_used < victim_last_used:
                victim_id = pid
                victim_last_used = frame.last_used

        if victim_id is None:
            raise RuntimeError("Bufferpool full and all pages are pinned; cannot evict.")

        # Remove first, then write back if dirty.
        victim_frame = self.frames.pop(victim_id)

        if not victim_frame.dirty:
            return

        try:
            self.write_page_to_disk(victim_id, victim_frame.page)
        except Exception as e:
            # Restore cache entry if eviction write-back fails.
            self.frames[victim_id] = victim_frame
            raise RuntimeError(f"Disk write failed during eviction for {victim_id}") from e
