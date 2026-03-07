import threading


class LockManager:
    def __init__(self):
        # rid -> {'shared': set(), 'exclusive': tid | None}
        self._locks = {}
        self._lock = threading.Lock()

    #returning/creating rid lock entries
    def entry(self, rid):
        if rid not in self.locks:
            self.locks[rid] = {'shared': set(), 'exclusive': None}
        return self.locks[rid]

    #only granted is no transaction holds an exclusive lock
    def acquire_shared(self, rid, tid):
        with self.lock:
            entry = self.entry(rid)
            exclusive = entry['exclusive']
            if exclusive is not None and exclusive != tid:
                return False        
            entry['shared'].add(tid)
            return True

    #only granted if no transaction has any lock
    #a transaction w/ shared lock can upgrade to exclusive (read -> write)
    def acquire_exclusive(self, rid, tid):
        with self.lock:
            entry = self.entry(rid)
            other_transactions_shared = entry['shared'] - {tid}
            if other_transactions_shared:
                return False        
            exclusive = entry['exclusive']
            if exclusive is not None and exclusive != tid:
                return False       
            entry['exclusive'] = tid
            entry['shared'].discard(tid)   #in case of upgrade
            return True

    
    def release_all(self, tid):
        with self.lock:
            for entry in self.locks.values():
                entry['shared'].discard(tid)
                if entry['exclusive'] == tid:
                    entry['exclusive'] = None
                    
            #remove entries with no active locks 
            empty = [rid for rid, e in self.locks.items()
                     if not e['shared'] and e['exclusive'] is None]
            for rid in empty:
                del self.locks[rid]
