from core.db_connect import DbAdapter


class HistoricDataBitcoin(object):
    __adapter = None

    @classmethod
    def _get_adapter(cls):
        if cls.__adapter is None:
            cls.__adapter = DbAdapter()
            cls.__adapter.connect_to_database()
        return cls.__adapter

    @classmethod
    def commit(cls):
        cls._get_adapter().commit_change()

    @classmethod
    def insert_into_coinbase_historic_data_bitcoin(cls, datetime, open, high, low, close, volume):
        sql_params = (datetime, open, high, low, close, volume)
        sql_query = """
            INSERT INTO coinbase_historic_data_bitcoin (datetime, Open, High, Low, Close, Volume) 
            VALUES (?,?,?,?,?,?)
        """
        cls._get_adapter().execute(sql_query, sql_params)

    @classmethod
    def get_last_entry_date(cls):
        """
        Get the last entry date from the datetime table
        """
        sql_query = """
            SELECT datetime FROM coinbase_historic_data_bitcoin
            ORDER BY datetime DESC
            LIMIT 1
        """
        return cls._get_adapter().fetch_one(sql_query)

    


