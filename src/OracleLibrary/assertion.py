#!/usr/bin/env python
# -*- coding: utf-8 -*-
# rmerkushin@gmail.com


class Assertion(object):

    def database_should_contain(self, select_statement, row_count=None):
        rows = self.query(select_statement)
        count = len(rows)
        if row_count:
            if count != int(row_count):
                raise AssertionError("Query result is not equal to expected number of rows. Expected: %s, Actual: %s"
                                     % (row_count, str(count)))
        else:
            if not rows:
                raise AssertionError("Expected to have at least one row from '%s', but got 0 rows" % select_statement)

    def database_should_not_contain(self, select_statement):
        rows = self.query(select_statement)
        if rows:
            raise AssertionError("Expected to have have no rows from '%s', but got some rows : %s."
                                 % (select_statement, rows))