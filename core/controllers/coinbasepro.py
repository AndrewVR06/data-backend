from core.controllers.controller import Controller
from coinbase.wallet.client import Client
import cbpro
from datetime import datetime

class Coinbase(Controller):
    __instance = None
    __client = None
    __proclient = None
    API_KEY = 'B4lrnBZ4Fn0z7BXv'
    API_SECRET = 'xXSmddVds8RuJht02VVjmoVIO5hK82Rj'

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super(Coinbase, cls).__new__(cls)
            cls.__client = Client(cls.API_KEY, cls.API_SECRET)
            cls.__proclient = cbpro.PublicClient()
        return cls.__instance
        
    def get_historic_prices(self,product='BTC-USD'):
        """
        Return the hisotric prices using the pro client. Maximum number of candles returned is 300
        Order returned is [ time, low, high, open, close, volume ]
        """
        return self.__proclient.get_product_historic_rates(product,start=datetime(2020,4,1,8),end=datetime(2020,4,1,9),granularity=60)

    def get_spot_price(self):
        return self.__client.get_spot_price(date=datetime(2020,4,1))


if __name__ == "__main__":
    print ('name')
    
