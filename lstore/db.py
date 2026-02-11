from lstore.table import Table

#handles all the tables in the system 
class Database():

    def __init__(self):
        self.table_count = 0 
        self.tables = []
        pass

    #not needed for m1
    def open(self, path):
        pass

    #not needed for m1
    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        self.tables.append(table)
        self.table_count += 1
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for table in self.tables:
            if table.name == name:
                del self.tables[table]

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table 
