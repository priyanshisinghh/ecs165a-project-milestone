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
            query.__self__.lock_manager = self.lock_manager

            pre_snapshot = self._capture_pre_snapshot(query, table, args)

            result = query(*args, tid=self.tid)
            if result is False:
                return self.abort()

            self.record_undo(query, table, args, result, pre_snapshot)

        return self.commit()

    def _capture_pre_snapshot(self, query, table, args):
        if query.__name__ in ('update', 'delete'):
            primary_key = args[0]
            rids = table.index.locate(table.key, primary_key)
            if rids:
                return table._get_latest_values(rids[0])
        return None

    def record_undo(self, query, table, args, result, pre_snapshot=None):
        query_name = query.__name__

        if query_name == 'insert':
            primary_key = args[table.key]
            q = query.__self__
            self._undo_log.append(lambda pk=primary_key, _q=q: _q.delete(pk))

        elif query_name == 'update':
            primary_key = args[0]
            q = query.__self__
            if pre_snapshot is not None:
                old_vals = pre_snapshot
                self._undo_log.append(lambda pk=primary_key, vals=old_vals, _q=q: _q.update(pk, *vals))

        elif query_name == 'delete':
            q = query.__self__
            if pre_snapshot is not None:
                old_vals = pre_snapshot
                self._undo_log.append(lambda vals=old_vals, _q=q: _q.insert(*vals))
