from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        #use table for append
        #poop poopppopopopp
        #popopopopopopoppop


    def run(self):
        for query, args in self.queries:
            result = query(*args)
            #if query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    
    def abort(self):
        #do rollback and any other necessary operations
        return False

    
    def commit(self):
        #commit to database
        return True
