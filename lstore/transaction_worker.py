import threading
from lstore.LockManager import LockManager


class TransactionWorker:
    MAX_RETRIES = 20

    def __init__(self, transactions = []):
        self.transactions = transactions
        self.stats = []         
        self.result = 0         #count of committed transactions
        self.lock_manager = None   #set externally before run()
        self.thread = None

    def add_transaction(self, t):
        self.transactions.append(t)

    def run(self):
        self.thread = threading.Thread(target = self.run_transaction, daemon = True)
        self.thread.start()

    def join(self):
        if self.thread is not None:
            self.thread.join()

    def run_transaction(self):
        #inject shared lock manager so the transaction can acquire/release locks
        for transaction in self.transactions:
            transaction.lock_manager = self.lock_manager

            committed = False
            for attempt in range(self.MAX_RETRIES):
                outcome = transaction.run()
                if outcome:
                    committed = True
                    break
                #if aborted, reset the transaction's undo log and retry
                transaction._undo_log.clear()

            self.stats.append(committed)

        self.result = sum(1 for s in self.stats if s)
