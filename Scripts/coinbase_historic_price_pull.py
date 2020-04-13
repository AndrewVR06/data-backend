from core.controllers.coinbasepro import Coinbase
from core.db_connect import Db_Adapter
import os
"""
Pull 1 minute historic data from coinbase and populate database with values. 
Need some time between each requests so as not to hit the endpoint too hard
"""

connection = Db_Adapter()
connection.connect_to_database()
connection.insert_into_coinbase_historic_data_bitcoin('2019-01-01', 11,12,13,14,15)

connection.delete_from_coinbase_historic_data_bitcoin_by_datetime('2019-01-01')