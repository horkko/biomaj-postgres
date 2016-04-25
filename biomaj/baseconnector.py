from __future__ import print_function
import sys


class BaseConnector(object):

    """ Base Connector """

    def __init__(self):
        """
        """
        pass

    def dislay(self, msg):
        """
        Small printer

        :param msg: Message to print
        :type msg: str
        :return:
        """
        print("[%s] %s" % (self.driver, str(msg)), file=sys.stdout)

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

    def get(self, query, display, first=False):
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