#!/usr/bin/env python
# -*- coding: utf-8 -*-
# rmerkushin@gmail.com

from connection import Connection
from query import Query
from assertion import Assertion


class OracleLibrary(Connection, Query, Assertion):

    ROBOT_LIBRARY_SCOPE = "GLOBAL"