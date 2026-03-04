import threading

class LockManager:
    def __init__(self):
        #dictionary of locks - rid -> {shared: set of transaction ids, exclusive: transaction id or None}
        #keeps track of all active locks on all RIDs
        self.rid_locks = {}
        self.lock = threading.Lock()  #protects the rid_locks dict itself

    #read lock - multiple transactions can hold a shared lock simultaneuosly 
    def acquire_shared(self, rid, tid):
        with self.lock:
            #add to dictionary of locks
            if rid not in self.rid_locks:
                self.rid_locks[rid] = {'shared': set(), 'exclusive': None}
            record = self.locks[rid]
            #can't get shared if someone else has exclusive
            if record['exclusive'] is not None and record['exclusive'] != tid:
                return False
            record['shared'].add(tid)
            return True

    #write lock - only one transaction can hold an exclusive lock at a time
    def acquire_exclusive(self, rid, tid):
        with self.lock:
            if rid not in self.rid_locks:
                self.rid_locks[rid] = {'shared': set(), 'exclusive': None}
            record = self.rid_locks[rid]
            #can't get exclusive if anyone else holds shared or exclusive
            others_records_sharing = record['shared'] - {tid}
            if others_records_sharing:
                return False
            if record['exclusive'] is not None and record['exclusive'] != tid:
                return False
            record['exclusive'] = tid
            return True

    def release_all(self, tid):
        with self._lock:
            for rid, locks in self.rid_locks.items():
                locks['shared'].discard(tid)
                if locks['exclusive'] == tid:
                    locks['exclusive'] = None
