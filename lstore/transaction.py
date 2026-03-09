import threading
from lstore.table import Table, Record


class Transaction:
    tid_counter = 0
    tid_lock = threading.Lock()

    def __init__(self):
        with Transaction.tid_lock:
            self.tid = Transaction.tid_counter
            Transaction.tid_counter += 1

        self.queries = []     

        #list of callables to reverse successful writes
        self.undo_log = []     

        #injected by TransactionWorker before run()
        self.lock_manager = None  

    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))

def run(self):
    #collect all query objects that need lock manager
    unlocked_queries = []
    for query, table, args in self.queries:
        query_obj = query.__self__
        if query_obj not in unlocked_queries:
            query_obj.lock_manager = self.lock_manager
            unlocked_queries.append(query_obj)

    #run queries
    try:
        for query, table, args in self.queries:
            pre_snapshot = self.capture_pre_snapshot(query, table, args)
            result = query(*args, tid = self.tid)
            if result is False:
                return self.abort()
            self.record_undo(query, table, args, pre_snapshot)
        return self.commit()
        
    except Exception:
        return self.abort()
        
    finally:
        #resent lock manager
        for query_obj in unlocked_queries:
            query_obj.lock_manager = None
            
    def commit(self):
        if self.lock_manager:
            self.lock_manager.release_all(self.tid)
        self.undo_log.clear()
        return True

    def abort(self):
        #run undo log in reverse to roll back all successful writes
        for undo_fn in reversed(self.undo_log):
            try:
                undo_fn()
            except Exception:
                pass  
        if self.lock_manager:
            self.lock_manager.release_all(self.tid)
        self.undo_log.clear()
        return False

    def capture_pre_snapshot(self, query, table, args):
        if query.__name__ in ('update', 'delete'):
            primary_key = args[0]
            rids = table.index.locate(table.key, primary_key)
            if rids:
                with table.dir_lock:
                    return table.get_latest_values(rids[0])
        return None

    def record_undo(self, query, table, args, pre_snapshot):
        q = query.__self__
        name = query.__name__

        if name == 'insert':
            #undo insert by deleting the record that was just created
            primary_key = args[table.key]
            self.undo_log.append(lambda pk = primary_key, _q=q, t = self.tid: _q.delete(pk, tid = t))

        elif name == 'update' and pre_snapshot is not None:
            #undo update by restoring the values that existed before
            primary_key = args[0]
            self.undo_log.append(lambda pk = primary_key, vals = pre_snapshot, _q=q, t = self.tid: _q.update(pk, *vals, tid = t))

        elif name == 'delete' and pre_snapshot is not None:
            #undo delete by reinserting the record
            self.undo_log.append(lambda vals = pre_snapshot, _q=q, t = self.tid: _q.insert(*vals, tid = t))
