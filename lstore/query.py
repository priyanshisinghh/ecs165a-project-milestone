from lstore.table import Record

METADATA_COLUMNS = 4
INDIRECTION_COLUMN = 0



class Query:
    def __init__(self, table):
        self.table = table

    # --------------------------------------------------
    # INTERNAL HELPERS
    # --------------------------------------------------

    def _read_base(self, pr, bp_idx, slot, col):
        return pr.read_base(bp_idx, slot, METADATA_COLUMNS + col)

    def _read_tail(self, pr, tp_idx, slot, col):
        return pr.read_tail(tp_idx, slot, METADATA_COLUMNS + col)

    def _latest_values(self, base_rid, projected):
        is_base, pr_idx, bp_idx, slot = self.table.page_directory[base_rid]
        pr = self.table.page_ranges[pr_idx]

        values = [None] * self.table.num_columns
        tail_rid = pr.read_base(bp_idx, slot, INDIRECTION_COLUMN)

        # no updates → base record
        if tail_rid == -1:
            for i in range(self.table.num_columns):
                if projected[i]:
                    values[i] = self._read_base(pr, bp_idx, slot, i)
            return values

        # updated → newest tail
        _, t_pr, t_pg, t_slot = self.table.page_directory[tail_rid]
        tpr = self.table.page_ranges[t_pr]

        for i in range(self.table.num_columns):
            if projected[i]:
                values[i] = self._read_tail(tpr, t_pg, t_slot, i)

        return values

    # somewhere here there is an error
    def _versioned_values(self, base_rid, projected, relative_version):
        is_base, pr_idx, bp_idx, slot = self.table.page_directory[base_rid]
        pr = self.table.page_ranges[pr_idx]

        values = [None] * self.table.num_columns
        tail_rid = pr.read_base(bp_idx, slot, INDIRECTION_COLUMN)

        # no updates
        if tail_rid == -1:
            for i in range(self.table.num_columns):
                if projected[i]:
                    values[i] = self._read_base(pr, bp_idx, slot, i)
            return values

        if relative_version == 0:
            _, t_pr, t_pg, t_slot = self.table.page_directory[tail_rid]
            tpr = self.table.page_ranges[t_pr]
            for i in range(self.table.num_columns):
                if projected[i]:
                    values[i] = self._read_tail(tpr, t_pg, t_slot, i)
            return values

        current = tail_rid
        steps = -relative_version

        for _ in range(steps):
            _, t_pr, t_pg, t_slot = self.table.page_directory[current]
            tpr = self.table.page_ranges[t_pr]
            prev = tpr.read_tail(t_pg, t_slot, INDIRECTION_COLUMN)
            if prev == -1:
                for i in range(self.table.num_columns):
                    if projected[i]:
                        values[i] = self._read_base(pr, bp_idx, slot, i)
                return values
            current = prev

        _, t_pr, t_pg, t_slot = self.table.page_directory[current]
        tpr = self.table.page_ranges[t_pr]

        for i in range(self.table.num_columns):
            if projected[i]:
                values[i] = self._read_tail(tpr, t_pg, t_slot, i)

        return values

    # --------------------------------------------------
    # PUBLIC API
    # --------------------------------------------------

    def insert(self, *columns):
        return self.table.insert(list(columns))

    def delete(self, primary_key):
        rids = self.table.index.locate(self.table.key, primary_key)
        if not rids:
            return False
        self.table.index.remove_entry(self.table.key, primary_key)
        return True

    def select(self, search_key, search_key_index, projected_columns_index):
        rids = self.table.index.locate(search_key_index, search_key)
        if not rids:
            return []

        records = []
        for rid in rids:
            is_base, *_ = self.table.page_directory[rid]
            if not is_base:
                continue

            values = self._latest_values(rid, projected_columns_index)
            records.append(Record(rid, values[self.table.key], values))

        return records

# doesn't work as intended
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        if relative_version >= 0:
            return self.select(search_key,search_key_index,projected_columns_index)
        
        rids = self.table.index.locate(search_key_index, search_key)
        if not rids:
            return []

        records = []
        for rid in rids:
            is_base, *_ = self.table.page_directory[rid]
            if not is_base:
                continue

            values = self._versioned_values(rid, projected_columns_index, relative_version)
            records.append(Record(rid, values[self.table.key], values))

        return records

    def update(self, primary_key, *columns):
        return self.table.update(primary_key, list(columns))

    def sum(self, start_range, end_range, aggregate_column_index):
        rids = self.table.index.locate_range(self.table.key, start_range, end_range)
        if not rids:
            return False

        total = 0
        projected = [0] * self.table.num_columns
        projected[aggregate_column_index] = 1

        for rid in rids:
            values = self._latest_values(rid, projected)
            total += values[aggregate_column_index]

        return total

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        if relative_version >= 0:
            return self.sum(start_range, end_range, aggregate_column_index)

        rids = self.table.index.locate_range(self.table.key, start_range, end_range)
        if not rids:
            return False

        total = 0
        projected = [0] * self.table.num_columns
        projected[aggregate_column_index] = 1

        for rid in rids:
            values = self._versioned_values(rid, projected, relative_version)
            total += values[aggregate_column_index]

        return total

    def increment(self, key, column):
        records = self.select(key, self.table.key, [1] * self.table.num_columns)
        if not records:
            return False

        r = records[0]
        updated = [None] * self.table.num_columns
        updated[column] = r.columns[column] + 1
        return self.update(key, *updated)
