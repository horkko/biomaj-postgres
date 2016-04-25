from biomaj.baseconnector import BaseConnector
from pymongo import MongoClient

__author__ = 'Emmanuel Quevillon <horkko@gmail.com>'


class MongoConnector(BaseConnector):
    """
    Connector to mongodb
    """

    def __init__(self, url=None, db='biomaj'):
        """
        Initiate the connector object to MongoDB

        :param url: Server connection url
        :type url: str
        :param db: Database name to connect ro
        :type db: str
        """
        super(BaseConnector, self).__init__()
        self.client = MongoClient(url)
        self.db = self.client[db]
        self.banks = self.db.banks
        self.users = self.db.users
        self.driver = 'mongodb'

    def get_bank_list(self, visibility='public'):
        """
        Get the list of available bank(s)

        :param visibility: Bank visibility type, default 'public'
        :type visibility: str
        :return:
        """
        if visibility not in ['public', 'private'] and visibility is not None:
            raise Exception("Visibility '%s' not allowed dude" % visibility)
        banks = self.get({}, {'name': 1})
        for bank in banks:
            self.dislay("Bank %s" % bank['name'])


    def update(self, query, values):
        """
        Update the document for the given filter and set it with values
        :param query: NoSQL query to perform
        :type query: dict
        :param values: Values for setting the update
        :type values: dict
        :return:
        """
        self.banks.update(query, values)

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
            return self.banks.find_one(query, display)
        else:
            return self.banks.find(query, display)

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
        if name in self.db:
            return self.db[name]
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
