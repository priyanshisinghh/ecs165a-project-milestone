import threading
from lstore.db import Database


class TransactionWorker:
    MAX_RETRIES = 20

    def __init__(self, transactions = None):
        self.transactions = transactions if transactions is not None else []
        self.stats = []
        self.result = 0
        self.lock_manager = None
        self.thread = None

    def add_transaction(self, t):
        self.transactions.append(t)

    def run(self):
        self.thread = threading.Thread(target = self._run, daemon = True)
        self.thread.start()

    def join(self):
        if self.thread is not None:
            self.thread.join()

    def _run(self):
        #set up lock manager now
        if self.lock_manager is None:
            self.lock_manager = Database.get_lock_manager()

        for transaction in self.transactions:
            transaction.lock_manager = self.lock_manager
            committed = False
            for attempt in range(self.MAX_RETRIES):
                outcome = transaction.run()
                if outcome:
                    committed = True
                    break
                #abort() is already called inside transaction.run()
            
            self.stats.append(committed)
        self.result = sum(1 for s in self.stats if s)
