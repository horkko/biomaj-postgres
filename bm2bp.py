__author__ = 'tuco'

from pymongo import MongoClient
import psycopg2
from psycopg2 import OperationalError, DatabaseError, IntegrityError
import json
from biomaj.config import BiomajConfig

"""
This small script is a test script to transfert data from a Biomaj MongoDB
database into a PostgreSQL data using Jsonb data typexs
"""
if __name__ == '__main__':

    BiomajConfig.load_config()
    mongo_url = BiomajConfig.global_config.get('GENERAL', 'db.url')
    mongo_db = BiomajConfig.global_config.get('GENERAL', 'db.name')
    mc = MongoClient(mongo_url)
    m_bank = mc[mongo_db].banks
    banks = []

    insert_query = "INSERT INTO bank(data) VALUES "
    #for bank in m_bank.find({'name': {'$in': ['alu', 'btaurus']}}, {'_id': 0}):
    for bank in m_bank.find({}, {'_id': 0}):
        # banks.append(json.dumps(bank))
        insert_query += "('%s')," % json.dumps(bank)
    insert_query = insert_query.strip(',')

    # In case we empty the databble

    try:
        pc = psycopg2.connect("dbname=biomaj user=tuco password=tucobell port=5432")
        cursor = pc.cursor()
        cursor.execute(insert_query)
        pc.commit()
    except OperationalError as err:
        print("Can't connect to database:\n%s" % str(err.message))
    except DatabaseError as err:
        print("Error:\n%s" % str(err))







