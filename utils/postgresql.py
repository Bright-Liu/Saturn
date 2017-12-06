#!/usr/local/bin/python3
# encoding: utf-8

"""
@version: 1.0.0
@author: bright
@contact: bright.liu2012@gmail.com
@file: postgresql.py
@time: 2017/12/5 09:30
"""

import psycopg2


def getConnection(host, port, user, password, database):
    """
    Create a new database connection.
    :param host: database host address (defaults to UNIX socket if not provided)
    :param port: connection port number (defaults to 5432 if not provided)
    :param user: user name used to authenticate
    :param password: password used to authenticate
    :param database: the database name (only as keyword argument)
    :return: connection
    """
    connection = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
    print("Create a new database connection successfully")
    return connection


def getRows(connection, sql):
    cur = connection.cursor()

    cur.execute(sql)
    rows = cur.fetchall()

    connection.close()
    print("Close the current database connection successfully")
    return rows
