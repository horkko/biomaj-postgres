#! /usr/bin/env python
"""
This small script is a test script to transfert data from a Biomaj MongoDB
database into a PostgreSQL data using Jsonb data typexs
"""
from __future__ import print_function
from pymongo import MongoClient
import psycopg2
from psycopg2 import OperationalError, DatabaseError, IntegrityError
import json
from biomaj.config import BiomajConfig
import os
import sys

__author__ = 'tuco'

if __name__ == '__main__':

    BiomajConfig.load_config()
    mongo_url = BiomajConfig.global_config.get('GENERAL', 'db.url')
    mongo_db = BiomajConfig.global_config.get('GENERAL', 'db.name')
    mc = MongoClient(mongo_url)
    m_bank = mc[mongo_db].banks
    banks = []
    dbname = 'biomaj'

    insert_query = "INSERT INTO bank(data) VALUES "
    for bank in m_bank.find({}, {'_id': 0}):
        insert_query += "('%s')," % json.dumps(bank)
    insert_query = insert_query.strip(',')

    # In case we empty the databble
    if len(sys.argv) > 1:
        dbname = sys.argv[1]

    try:
        pc = psycopg2.connect("dbname=%s user=%s password=%s port=%s" %
                              (dbname, os.getenv('PGUSER'),
                               os.getenv('PGPASSWORD'), os.getenv('PGPORT')))
        cursor = pc.cursor()
        cursor.execute(insert_query)
        pc.commit()
    except OperationalError as err:
        print("Can't connect to database:\n%s" % str(err.message))
    except DatabaseError as err:
        print("Error:\n%s" % str(err))







