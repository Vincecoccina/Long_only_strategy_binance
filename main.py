import pandas as pd
import os
from binance.client import Client
from binance import ThreadedWebsocketManager
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time

# Chargement des variables d'environnement pour la sécurité des clés API
load_dotenv()
api_key = os.getenv("API_KEY_TEST")
secret_key = os.getenv("SECRET_KEY_TEST")

# Initialisation de l'API Binance avec les clés API
client = Client(api_key=api_key, api_secret=secret_key, tld='com', testnet=True)

symbol = 'BTCUSDT'

class LongOnlyTrader():
    
    def __init__(self, symbol, bar_length):
        self.symbol = symbol
        self.bar_length = bar_length
        # self.data = pd.DataFrame(columns = ["Open", "High", "Low", "Close", "Volume", "Complete"])
        self.available_intervals = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]
    
    def start_trading(self, historical_days):
        
        self.twm = ThreadedWebsocketManager()
        self.twm.start()
        
        if self.bar_length in self.available_intervals:
            self.get_most_recent(symbol = self.symbol, interval = self.bar_length,
                                 days = historical_days)
            self.twm.start_kline_socket(callback = self.stream_candles,
                                        symbol = self.symbol, interval = self.bar_length)
            
    
    def get_most_recent(self, symbol, interval, days):
        now = datetime.utcnow()
        past = str(now - timedelta(days = days))

        bars = client.get_historical_klines(symbol = symbol, interval = interval,
                                            start_str=past, end_str=None, limit=1000)
        df = pd.DataFrame(bars)
        df["Date"] = pd.to_datetime(df.iloc[:,0], unit = "ms")
        df.columns = ["Open Time", "Open", "High", "Low", "Close", "Volume",
                      "Clos Time", "Quote Asset Volume", "Number of Trades",
                      "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore", "Date"]
        df = df[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
        df.set_index("Date", inplace = True)
        for column in df.columns:
            df[column] = pd.to_numeric(df[column], errors = "coerce")
        df["Complete"] = [True for row in range(len(df)-1)] + [False]
        
        self.data = df
     
    
    def stream_candles(self, msg):
        
        event_time = pd.to_datetime(msg["E"], unit = "ms")
        start_time = pd.to_datetime(msg["k"]["t"], unit = "ms")
        first   = float(msg["k"]["o"])
        high    = float(msg["k"]["h"])
        low     = float(msg["k"]["l"])
        close   = float(msg["k"]["c"])
        volume  = float(msg["k"]["v"])
        complete=       msg["k"]["x"]
    
   
        print("Time: {} | Price: {}".format(event_time, close))
    
       
        self.data.loc[start_time] = [first, high, low, close, volume, complete]

trader = LongOnlyTrader(symbol=symbol, bar_length="1m")
trader.start_trading(historical_days = 2)

run_time = 60 

time.sleep(run_time)

trader.twm.stop()
print(trader.data)