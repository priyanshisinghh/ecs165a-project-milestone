from lstore.table import Table, Record
from lstore.index import Index

class Transaction:
    tid_counter = 0 
    tid_lock = threading.Lock() #lock to protect transaction

    def __init__(self):
        self.queries = []
        self.locks_held = []  #list of rids locked by this transaction
        with Transaction.tid_lock:
            self.tid = Transaction.tid_counter
            Transaction.tid_counter += 1
        self.lock_manager = None  #set by TransactionWorker before run()

    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))

    def run(self):
        #acquire the right lock before running the query - if the lock cannot be acquired, then abort.
        for query, table, args in self.queries:
            if not self.acquire_lock(query, table, args):
                return self.abort()
            result = query(*args)
            if result == False:
                return self.abort()
        return self.commit()

    #determine which lock is needed and attempt to acquire it.
    def acquire_lock(self, query, table, args):
        if self.lock_manager is None:
            return True  #no lock manager = no concurrency control, allow

        query_name = query.__name__
        write_ops = {'update', 'insert', 'delete', 'increment'}
        read_ops  = {'select', 'select_version', 'sum', 'sum_version'}

        if query_name in write_ops:
            primary_key = args[0]
            rids = table.index.locate(table.key, primary_key)
            if not rids:
                return True 
            rid = rids[0]
            if not self.lock_manager.acquire_exclusive(rid, self.tid):
                return False
            self.locks_held.append(rid)

        elif query_name in read_ops:
            primary_key = args[0]
            rids = table.index.locate(table.key, primary_key)
            if not rids:
                return True
            rid = rids[0]
            if not self.lock_manager.acquire_shared(rid, self.tid):
                return False
            self.locks_held.append(rid)

        return True

    #let the transaction go / go thru
    def abort(self):
        if self.lock_manager:
            self.lock_manager.release_all(self.tid)
        return False

    def commit(self):
        if self.lock_manager:
            self.lock_manager.release_all(self.tid)
        return True
