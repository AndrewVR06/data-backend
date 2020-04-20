from core.controllers.coinbasepro import Coinbase
from core.models.coinbase.historic_data_bitcoin import HistoricDataBitcoin
from datetime import datetime, time, timedelta
from timeit import timeit
from time import sleep
"""
Script to populate the database with values. Set a begin date and end date.
Pull 1 minute historic data from coinbase and populate database with values. 
Need some time between each requests so as not to hit the endpoint too hard
"""

def update_database(begin_date, end_date, currency='BTC-USD'):

    # Coinbase API
    coinbase = Coinbase()

    # we can pull a maximum of 300 requests at a time, at 60 seconds per request we can get 5 hours per
    # request. 
    # Lets start by back dating one year. 
    # The below times are GM-0! However time will be converted to GM+2. Meaning all values in database are localtime.
   
    # used for the progress update
    total_seconds = (end_date - begin_date).total_seconds() 
    seconds_completed = 0
    seconds_added = 300*60

    current_datetime = begin_date
    time_delta = timedelta(minutes=300)
    start_time = timeit()
    requests = 0
    try:
        while current_datetime < end_date:

            if (end_date - current_datetime) < timedelta(minutes=300):
                remaining_minutes = (end_date - current_datetime).total_seconds() / 60
                time_delta = timedelta(minutes=int(remaining_minutes))
                seconds_added = int(remaining_minutes) * 60

            # return format is [ time, low, high, open, close, volume ] 
            prices = coinbase.get_historic_prices(currency, start=current_datetime, end=(current_datetime+time_delta), granularity=60) 
            requests += 1
            for result in prices[::-1] :
                HistoricDataBitcoin.insert_into_coinbase_historic_data_bitcoin(
                    datetime=datetime.fromtimestamp(result[0]).strftime('%Y-%m-%d %H:%M:%S'),
                    low=result[1],
                    high=result[2],
                    open=result[3],
                    close=result[4],
                    volume=result[5]
                )

            # check request limit. 3 per ip per second
            if requests >= 3:
                time_difference = abs(timeit() - start_time)
                if time_difference < 1:
                    sleep((1 - time_difference))
                    requests = 0
                    start_time = timeit() 

            current_datetime += timedelta(minutes=300)
            seconds_completed += seconds_added
            print ("\rPercentage done is ", '[{:>7.2%}]'.format(seconds_completed/total_seconds), end='')

    except TypeError as e:
        print ('\n%s', str(e))
        print ("\nPrices is %s", prices)

    print ('\nCompleted Successfully!')
    HistoricDataBitcoin.commit()

if __name__ == "__main__":
    # Fetch the latest datetime 
    begin_date = HistoricDataBitcoin.get_last_entry_date()
    if not begin_date:
        begin_date = datetime(2019,1,1,0,0,0)
    else:
        begin_date = datetime.strptime(begin_date, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.now()
    currency = 'BTC-USD'
    update_database(begin_date, end_date, currency)
