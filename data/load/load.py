#!/usr/local/bin/python3
# encoding: utf-8

"""
@version: 1.0.0
@author: bright
@contact: bright.liu2012@gmail.com
@file: load.py
@time: 2017/12/6 09:30
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *


def getSparkSession():
    """
    Get or create SparkSession
    :return:
    """
    session = SparkSession.builder.master("local[2]").getOrCreate()
    return session


def getSchema():
    """
    Create Schema
    :return: StructType
    """

    fields = [
        StructField("log_time", TimestampType(), True),
        StructField("machine_id", StringType(), True),
        StructField("ci_nacelle_position", FloatType(), True),
        StructField("ci_rotor_speed", FloatType(), True)
    ]
    return StructType(fields)


def getDataFrame(data, schema):
    """
    Creates a :class:`DataFrame` from an :class:`RDD`, a list or a :class:`pandas.DataFrame`.
    :param data: an :class:`RDD`, a list or a :class:`pandas.DataFrame`
    :param schema:is a list of column names, the type of each column will be inferred from ``data``.
    :return:
    """
    # Get SparkSession
    session = getSparkSession()

    # Apply the schema to the RDD.
    return session.createDataFrame(data, schema)


if __name__ == '__main__':
    pass
