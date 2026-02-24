import pickle
from lstore.table import Table
from lstore.bufferpool import Bufferpool
from lstore.disk import Disk


class Database:
    def __init__(self):
        self.tables = {}
        self.disk = None
        self.path = None

    # --------------------------------------------------
    # OPEN / CLOSE (REQUIRED)
    # --------------------------------------------------

    def open(self, path):
        self.path = path
        self.disk = Disk(path)
        try:
            with open(path, "rb") as f:
                state = pickle.load(f)

            # restore disk
            self.disk.pages = state["disk_pages"]

            # restore tables
            for meta in state["tables"]:
                table = Table(
                    meta["name"],
                    meta["num_columns"],
                    meta["key"]
                )

                # reattach bufferpool to shared disk
                table.bufferpool = Bufferpool(
                    capacity_pages=50,
                    disk_manager=self.disk
                )

                # restore table internals
                table.page_ranges = meta["page_ranges"]
                table.page_directory = meta["page_directory"]
                table.rid_num = meta["rid_num"]
                table.index = meta["index"]

                self.tables[table.name] = table

        except FileNotFoundError:
            # fresh database
            pass

    def close(self):
        # flush all dirty pages first
        for table in self.tables.values():
            table.bufferpool.flush_all()

        # serialize DB state
        state = {
            "disk_pages": self.disk.pages,
            "tables": []
        }

        for table in self.tables.values():
            state["tables"].append({
                "name": table.name,
                "num_columns": table.num_columns,
                "key": table.key,
                "page_ranges": table.page_ranges,
                "page_directory": table.page_directory,
                "rid_num": table.rid_num,
                "index": table.index
            })

        with open(self.path, "wb") as f:
            pickle.dump(state, f)

    # --------------------------------------------------
    # TABLE MANAGEMENT
    # --------------------------------------------------

    def create_table(self, name, num_columns, key_index):
        if name in self.tables:
            return self.tables[name]

        table = Table(name, num_columns, key_index)
        table.bufferpool = Bufferpool(50, self.disk)

        self.tables[name] = table
        return table

    def drop_table(self, name):
        if name in self.tables:
            self.tables[name].bufferpool.flush_all()
            del self.tables[name]

    def get_table(self, name):
        return self.tables.get(name)
