import threading
from lstore.table import Table, Record


class Transaction:
    tid_counter = 0
    tid_lock = threading.Lock()

    def __init__(self):
        with Transaction.tid_lock:
            self.tid = Transaction.tid_counter
            Transaction.tid_counter += 1

        #list of (query_fn, args) to execute
        self.queries = []

        #undo log: list of callables that reverse each successful write
        self._undo_log = []

        #injected by TransactionWorker before run() is called
        self.lock_manager = None

    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))

    def run(self):
        for query, table, args in self.queries:
            #inject our tid and lock manager into the Query object so it can acquire locks
            query.__self__.lock_manager = self.lock_manager

            result = query(*args, tid = self.tid)
            if result is False:
                return self.abort()

            self.record_undo(query, table, args, result)

        return self.commit()

    def commit(self):
        if self.lock_manager:
            self.lock_manager.release_all(self.tid)
        self.undo_log.clear()
        return True

    #undo every successful write in reverse order
    def abort(self):
        for undo_fn in reversed(self.undo_log):
            try:
                undo_fn()
            except Exception:
                pass  

        if self.lock_manager:
            self.lock_manager.release_all(self.tid)
        self.undo_log.clear()
        return False

    def record_undo(self, query, table, args, result):
        query_name = query.__name__

        #undo an insert by deleting the record that was just createsd
        if query_name == 'insert':
            primary_key = args[table.key]
            q = query.__self__          
            self.undo_log.append(lambda pk = primary_key, _q=q: _q.delete(pk))

        #undo an update by restoring the valyes that existed before
        elif query_name == 'update':
            primary_key = args[0]
            q = query.__self__

            #old_data is saved implicitly via the undo: re-read the latest tail record and use its INDIRECTION to find the previous state
            #for 2PL - read the current values from the table right now and store them as the restore target
            
            rids = table.index.locate(table.key, primary_key)
            if rids:
                base_rid = rids[0]
                old_vals = table._get_latest_values(base_rid)
                self.undo_log.append(lambda pk=primary_key, vals=old_vals, _q=q: _q.update(pk, *vals))

        elif query_name == 'delete':
            pass
