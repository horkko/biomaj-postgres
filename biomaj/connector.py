"""
Created on Nov 25, 2015

@author: tuco
"""
from biomaj.mongo_connector import MongoConnector
from biomaj.postgres_connector import PostgresConnector
from biomaj.config import BiomajConfig
from string import split


class Connector(object):

    _connector = None

    def __init__(self, driver=None, url=None, db=None):
        """
        Create new connector (MongoDB/PostgreSQL)

        :param driver: Driver type, now supported (mongodb/postgres)
        :type driver: str
        :param url: URL (dsn) connection
        :type url: str
        :param db: Database name
        :type: db: str
        """
        self.url = None
        self.db = None

        # If connector type not set, try to get it from the global.properties
        if not BiomajConfig.global_config:
            BiomajConfig.load_config()

        url = BiomajConfig.global_config.get('GENERAL', 'db.url')
        db = BiomajConfig.global_config.get('GENERAL', 'db.name')

        if url is None:
            raise Exception("No connection url found!")
        if db is None:
            raise Exception("No connection db found!")

        self.url = url
        self.db = db

    def get_connector(self):
        """
        Creates inherited connector
        :return: Connector or None if driver not supported
        :rtype: :class:`Connector.` inherited class instance
        """
        if Connector._connector is not None:
            return Connector._connector

        driver = split(self.url, ':')[0]

        if not driver:
            raise Exception("Can't determine database driver")

        if self.url is None or self.db is None:
            raise Exception("Can't create connector, params not set!")
        if driver == 'mongodb':
            Connector._connector = MongoConnector(url=self.url, db=self.db)
        elif driver == 'postgres':
            Connector._connector = PostgresConnector(url=self.url, db=self.db)
        return Connector._connector
