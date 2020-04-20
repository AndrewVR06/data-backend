import sqlite3
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DbAdapter(object):
    __instance = None
    __connection = None
    __cursor = None

    # Singleton class instantiation
    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super(DbAdapter, cls).__new__(cls)
        return cls.__instance

    def connect_to_database(self):
        """
        Establish a connection to a sqlite3 database. If this class is already connected to a db, then close that connection and open up a new one
        """
        try:

            if self.__connection is not None:
                logger.debug("Close connection with %s", str(self.__connection))
                self.__connection.commit()
                self.__connection.close()
                self.__connection = None

            
            self.__connection = sqlite3.connect('core/database.db')
            self.__cursor = self.__connection.cursor()
            logger.debug("Connection to database database.db was successful")

        except sqlite3.Error as e:
            logger.error("Failed to connect to db database.db")
            raise

    def execute(self, sql_query, sql_params):
        try:
            self.__cursor.execute(sql_query, sql_params)

        except sqlite3.Error as e:
            logger.error("Failed to connect to db database.db")
            raise

    def fetch_one(self, sql_query):
        try:
            result = self.__cursor.execute(sql_query).fetchone()
            if result:
                return result[0]
            return result

        except sqlite3.Error as e:
            logger.error("Failed to connect to db database.db")
            raise

    def delete_from_coinbase_historic_data_bitcoin_by_datetime(self, datetime):
        """
        Remove from the coinbase_historic_data_bitcoin by datetime
        """
        sql_params = (datetime)

        sql_query = """
            DELETE FROM coinbase_historic_data_bitcoin 
            WHERE datetime = ?
        """
        try:
            self.__cursor.execute(sql_query, (sql_params,))

        except sqlite3.Error as e:
            logger.error("Failed to connect to db database.db")
            raise

    def commit_change(self):
        self.__connection.commit()







    

