"""

Created on Nov 25 2015

@author: tuco
"""

import psycopg2

class PostgresConnector():


    def __init__(self, url=None, db='biomaj'):
        pass

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