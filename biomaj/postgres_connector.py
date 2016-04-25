"""

Created on Nov 25 2015

@author: tuco
"""
from biomaj.baseconnector import BaseConnector
import psycopg2
from psycopg2 import OperationalError, DatabaseError, IntegrityError
from psycopg2.extras import DictCursor, Json
import os
import re


class PostgresConnector(BaseConnector):


    def __init__(self, url=None, db='biomaj'):
        """
        Connector that speaks with PostgreSQL

        :param url: dsn to connect to PostgreSQL database. Expected format "postgres://[username:password]@host[:port]"
        :type url: str
        :param db: Database name to connect to, default 'biomaj'
        :type db: str
        """
        super(BaseConnector, self).__init__()
        if url is None:
            raise Exception("A url string is mandatory to connect to PostgreSQL")
        port = 5432
        user = os.getenv("PGUSER")
        password = os.getenv("PGPASSWORD")
        #                                  [user  ]:[password] @ host   [:port]
        pattern = re.compile(r"postgres://(([^:@]*):?([^@]*)?)?@?([^:]+):?([\d]+)?")
        url_match = pattern.match(url)
        if url_match:
            (user, password) = url_match.group(2, 3)
            (host, port) = url_match.group(4, 5)
        try:
            self.client = psycopg2.connect("dbname=%s user=%s password=%s host=%s port=%s" %
                                           (db, user, password, host, str(port)))

        except OperationalError as err:
            raise Exception("Can't connect to database: %s" % str(err))
        except DatabaseError as err:
            raise Exception("Unexpected error: %s" % str(err))
        self.driver = 'postgres'

    def get_bank_list(self, visibility='public'):
        """
        Get the list of available bank(s)

        :param visibility: Bank visibility type, default 'public'
        :type visibility: str
        :return:
        """
        if visibility not in ['public', 'private'] and visibility is not None:
            raise Exception("Visibility '%s' not allowed dude" % visibility)
        cursor = self.client.cursor(cursor_factory=DictCursor)
        query = "SELECT data->>'name' AS name FROM bank"
        if visibility:
            query += " WHERE data->'properties'->>'visibility' = '%s'" % visibility
        try:
            cursor.execute(query)
            for row in cursor:
                self.dislay("Bank %s" % row['name'])
            self.client.commit()
        except DatabaseError as err:
            raise Exception("Can't perform query: %s\nquery: %s" % (str(err), query))


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