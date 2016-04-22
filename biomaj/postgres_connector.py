"""

Created on Nov 25 2015

@author: tuco
"""
from biomaj.connector import Connector
import psycopg2
import os
import re

class PostgresConnector(Connector):


    def __init__(self, url=None, db='biomaj'):
        """
        Connector to speak woth PostgreSQL

        :param url: dsn to connect to PostgreSQL database. Expected format "postgres://[username:password]@host:port/db"
        :type url: str
        :param db: Database name to connect to, default 'biomaj'
        :type db: str
        """
        if url is None:
            raise Exception("A url string (dsn) is mandatory to connect to PostgreSQL")
        port = 5432
        user = os.getenv("PGUSER")
        password = os.getenv("PGPASSWORD")
        #                                  [user  ]:[password] @ host   [:port]    / dbname
        pattern = re.compile(r"postgres://(([^:@]*):?([^@]+)?)?@?([^/:]+):?([\d]+)?/([^\s]+)")
        url_match = pattern.match(url)
        if url_match:
            (user, password) = url_match.group(2, 3)
            (host, port) = url_match.group(4, 5)
            db = url_match.group(6)

        self.pc = psycopg2.connect("dbname=%s user=%s password=%s host=%s port=%d" %
                                   (db, user, password, host, port))

    def get_bank_list(self):
        pass

    def update(self, filter, values):
        """
        Update the database
        :param filter: Search criteria
        :param values: Values for setting the update
        :return:
        """
        pass

    def get(self, filter, display, first=True):
        """

        :param filter: Filter to apply for the request
        :param display: Fields to display from the results
        :param first:
        :return:
        """
        pass

    def get_collection(self):
        """

        :return:
        """
        pass

    def set(self, collection, values):
        """
        Set (insert) values in database
        :param collection: table name
        :type collection: String
        :param values: Values to insert
        :type values: Dict
        :return: Inserted id
        """
        pass