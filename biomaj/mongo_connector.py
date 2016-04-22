from pymongo import MongoClient
from biomaj.connector import Connector

__author__ = 'Emmanuel Quevillon <horkko@gmail.com>'


class MongoConnector(Connector):
    """
    Connector to mongodb
    """

    client = None
    db = None
    banks = None
    users = None

    def __init__(self, url=None, db='biomaj'):
        """
        Initiate the connector object to MongoDB

        :param url: Server connection url
        :type url: str
        :param db: Database name to connect ro
        :type db: str
        """
        self.client = MongoClient(url)
        self.db = self.client[db]
        self.banks = self.db.banks
        self.users = self.db.users

        # MongoConnector.client = MongoClient(url)
        # MongoConnector.db = MongoConnector.client[db]
        # MongoConnector.banks = MongoConnector.db.banks
        # MongoConnector.users = MongoConnector.db.users

    def update(self, query, values):
        """
        Update the document for the given filter and set it with values
        :param query: NoSQL query to perform
        :type query: dict
        :param values: Values for setting the update
        :type values: dict
        :return:
        """
        self.banksMongoConnector.banks.update(query, values)

    def get(self, query, display={}, first=False):
        """

        :param query: Query to apply for the request
        :type query: dict
        :param display: Fields to display from the results
        :type display: dict
        :param first: Only return the first row
        :type first: bool
        :return: Results of the query
        :rtype: dict
        """
        if first:
            return MongoConnector.banks.find_one(query, display)
        else:
            return MongoConnector.banks.find(query, display)

    def get_collection(self, name):
        """
        Get the collection for the given name
        :param name: Name of the collection
        :type name: str
        :return: Collection from Mongo or None
        :rtype: :class:`pymongo.collection`
        """
        if not name:
            raise Exception("A collection name is required")
        if name in MongoConnector.__dict__:
            return MongoConnector.__dict__[name]
        return None

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
        if not values:
            raise Exception("Values are required to be set")
        if not collection or collection not in MongoConnector.__dict__:
            raise Exception("Collection '%s' does not exists" % collection)
        if not values.keys():
            raise Exception("Not keys found in you values")
        doc_id = MongoConnector.__dict__[collection].insert_one(values)
        return str(doc_id)
