#!/usr/bin/env python
# -*- coding: utf-8 -*-
# rmerkushin@gmail.com

import cx_Oracle
from os import environ


class Connection(object):

    def __init__(self):
        self.connection = None

    def connect_to_database(self, user_name, password, host, port, sid_service, encoding=None):
        if encoding:
            environ["NLS_LANG"] = encoding
        if sid_service.split("=")[0] == "sid":
            dsn = cx_Oracle.makedsn(host,  port, sid_service.split("=")[1])
        else:
            dsn = cx_Oracle.makedsn(host,  port, service_name=sid_service.split("=")[1])
        self.connection = cx_Oracle.connect(user_name, password, dsn)

    def connect_to_database_by_tns(self, user_name, password, network_alias, encoding=None):
        if encoding:
            environ["NLS_LANG"] = encoding
        self.connection = cx_Oracle.connect(user_name, password, network_alias)

    def connect_to_database_by_connection_string(self, connection_string, encoding=None):
        if encoding:
            environ["NLS_LANG"] = encoding
        self.connection = cx_Oracle.connect(connection_string)

    def disconnect_from_database(self):
        self.connection.close()