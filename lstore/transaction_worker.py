from lstore.table import Table, Record
from lstore.index import Index
import threading

class TransactionWorker:
    def __init__(self, transactions = []):
        self.transactions = transactions
        self.stats = []
        self.result = 0
        self.thread = None
        self.lock_manager = None  # set externally before run()

    def add_transaction(self, t):
        self.transactions.append(t)

    def run(self):
        self.thread = threading.Thread(target = self.run)
        self.thread.start()

    def join(self):
        if self.thread:
            self.thread.join()

    def __run(self):
        for transaction in self.transactions:
            transaction.lock_manager = self.lock_manager
            self.stats.append(transaction.run())
        self.result = len(list(filter(lambda x: x, self.stats)))


   # def __run(self):
       # for transaction in self.transactions:
           # self.stats.append(transaction.run())
        #self.result = len(list(filter(lambda x: x, self.stats)))
