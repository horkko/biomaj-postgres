"""
Created on Nov 25, 2015

@author: tuco
"""
from biomaj.mongo_connector import MongoConnector
from biomaj.postgres_connector import PostgresConnector
from biomaj.config import BiomajConfig
from string import split


class Connector(object):

    url = None
    db = None
    driver = None
    _connector = None

    def __init__(self):
        # If connector type not set, try to get it from the global.properties
        if not BiomajConfig.global_config:
            BiomajConfig.load_config()
        url = BiomajConfig.global_config.get('GENERAL', 'db.url')
        db = BiomajConfig.global_config.get('GENERAL', 'db.name')
        if url is None:
            raise Exception("No connection url set!")
        if db is None:
            raise Exception("No connection db set!")
        driver = split(url, ':')[0]
        if not driver:
            raise Exception("Can't determine database driver")

        Connector.url = url
        Connector.db = db
        Connector.driver = driver

    def get_connector(self):
        """
        Creates inherited connector
        :return: Connector
        :rtype: :class:`Connector.` inherited class instance
        """
        if Connector._connector is not None:
            return Connector._connector
        if Connector.url is None or Connector.db is None:
            raise Exception("Can't create connector, params not set!")
        if Connector.driver == 'mongodb':
            Connector._connector = MongoConnector(url=Connector.url, url=Connector.db)
        elif Connector.driver == 'postgres':
            Connector._connector = PostgresConnector(url=Connector.url, db=Connector.db)
        return Connector._connector

    def update(self, query, values):
        """
        Update the database
        :param query: Filter what we want to update (a.k.a. 'LIKE')
        :type query: dict
        :param values: Values to set
        :type values: dict
        :return:
        """
        self.__not_implemented()

    def get(self, query, display={}, first=False):
        """
        Get the entries from the database

        :param query: Query to apply for the request
        :type query: dict
        :param display: Fields to display from the results
        :type display: dict
        :param first: Only return the first row
        :type first: bool
        :return: Results of the query
        :rtype: dict
        """
        self.__not_implemented()

    def get_collection(self, name):
        """
        Get the collection for the given name
        :param name: Name of the collection
        :type name: str
        :return: Collection from database
        :rtype:
        """
        self.__not_implemented()

    def set(self, collection, values):
        """
        Set (insert) values in database

        :param collection: Collection to set values to
        :type collection: str
        :param values: Values to insert
        :type values: dict
        :return: Document id
        :rtype: str
        """
        self.__not_implemented()

    def __not_implemented(self, msg):
        """
        Just throw a message about a non  implemented method
        :param msg: Message to raise
        :type msg: String
        :return:
        """
        if not msg:
            msg = "This method is not implemented in the base connector. Please overwrite it in inheriting module"
        raise Exception(msg)