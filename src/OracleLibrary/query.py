#!/usr/bin/env python
# -*- coding: utf-8 -*-
# rmerkushin@gmail.com

import time
import cx_Oracle
from robot.api import logger


class Query(object):

    @staticmethod
    def _utf8_decode(rows):
        i = 0
        for row in rows:
            rows[i] = map(lambda s: str(s).decode("utf-8") if s else '', row)
            i += 1
        return rows

    def _execute_sql(self, sql_statement):
        cursor = self.connection.cursor()
        cursor.prepare(sql_statement)
        cursor.execute(None)
        return cursor

    def query(self, select_statement, one_row="False"):
        cursor = self._execute_sql(select_statement)
        rows = cursor.fetchall()
        rows = self._utf8_decode(rows)
        if one_row.upper() == "TRUE":
            united_rows = []
            for row in rows:
                united_rows.extend(row)
            rows = united_rows
        return rows

    def row_count(self, select_statement):
        cursor = self._execute_sql(select_statement)
        cursor.fetchall()
        return cursor.rowcount

    def execute_sql_string(self, sql_statement):
        try:
            cursor = self._execute_sql(sql_statement)
            self.connection.commit()
        finally:
            if cursor:
                self.connection.rollback()

    def execute_sql_script(self, file_name, replace=None, one_row="False"):
        sql_statement = open(file_name, "r").read().decode("utf-8")
        if replace:
            for x, y in replace.iteritems():
                sql_statement = sql_statement.replace(x, y)
        cursor = self._execute_sql(sql_statement)
        try:
            rows = cursor.fetchall()
            rows = self._utf8_decode(rows)
            if one_row.upper() == "TRUE":
                united_rows = []
                for row in rows:
                    united_rows.extend(row)
                rows = united_rows
            return rows
        except cx_Oracle.InterfaceError:
            self.connection.commit()
        if cursor:
            self.connection.rollback()
            return False
        else:
            return True

    def wait_until_exists_in_database(self, select_statement, timeout=10):
        for i in range(int(timeout)):
            cursor = self._execute_sql(select_statement)
            cursor.fetchall()
            if cursor.rowcount != 0:
                return True
            time.sleep(1)
        raise AssertionError("Record was not found in %s second(s)" % str(timeout))

    def wait_until_not_exists_in_database(self, select_statement, timeout=10):
        for i in range(int(timeout)):
            cursor = self._execute_sql(select_statement)
            cursor.fetchall()
            if cursor.rowcount == 0:
                return True
            time.sleep(1)
        raise AssertionError("Record was not deleted in %s second(s)." % str(timeout))

    def dump_table(self, select_statement):
        table = '<table border="1">'
        cursor = self._execute_sql(select_statement)
        table_body = cursor.fetchall()
        for row in table_body:
            table += "<tr>"
            for cell in row:
                table += "<td>" + str(cell).decode("utf-8") + "</td>"
            table += "</tr>"
        table += "</table>"
        logger.info(table, html=True)

    def call_function(self):
        pass

    def call_procedure(self):
        pass