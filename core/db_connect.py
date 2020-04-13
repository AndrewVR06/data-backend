import sqlite3
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Db_Adapter(object):
    __instance = None
    __connection = None
    __cursor = None

    # Singleton class instantiation
    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super(Db_Adapter, cls).__new__(cls)
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

    def set_table(self,table_name):
        """
        Set the table name to send queries to
        """
        self.__table_name = table_name

    def insert_into_coinbase_historic_data_bitcoin(self, datetime, open, high, low, close, volume):
        """
        Insert prices into the database
        """
        sql_params = (datetime, open, high, low, close, volume)

        sql_query = """
            INSERT INTO coinbase_historic_data_bitcoin (datetime, Open, High, Low, Close, Volume) 
            VALUES (?,?,?,?,?,?)
        """
        try:
            self.__cursor.execute(sql_query, (sql_params,))
            self.__connection.commit()

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
            self.__connection.commit()

        except sqlite3.Error as e:
            logger.error("Failed to connect to db database.db")
            raise










    

