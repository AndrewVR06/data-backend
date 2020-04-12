import sqlite3
import logging

logger = logging.getLogger('database')

class Connection(object):
    __instance = None
    __connection = None

    # Singleton class instantiation
    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super(Connection, cls).__new__(cls)
        return cls.__instance

    def makeConnection(self, db_name):
        """
        Establish a connection to a sqlite3 database. If this class is already connected to a db, then close that connection and open up a new one
        """
        try:

            if self.__connection is not None:
                logger.debug("Close connection with %s", str(self.__connection))
                conn.commit()
                conn.close()
                self.__connection = None

            logger.debug("Establishing connection with database")
            self.__connection = sqlite3.connect(str(db_name) + '.db')
            return  self.__connection.cursor()

        except sqlite3.Error as e:
            print ("Failed to connect to db %s", db_name)
            raise

    

