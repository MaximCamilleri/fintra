from ibapi.client import EClient, Contract
from ibapi.wrapper import EWrapper, OrderId
import threading
import pandas as pd

class IbkrClient(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, self)
        
        self.data = [] 
        self.df = None
        self.data_ready = threading.Event()
        self.current_req_id = 0
    
    def nextValidId(self, orderId: OrderId):
        self.orderId = orderId
    
    def nextId(self):
        self.orderId += 1
        return self.orderId
    
    def error(self, reqId, errorCode, errorString, advancedOrderReject=""):
        if errorCode == 162:
            print("No more data found or pacing violation.")
            self.data_ready.set()
        else:
            print(f"reqId: {reqId}, errorCode: {errorCode}, errorString: {errorString}, orderReject: {advancedOrderReject}")
    
    def historicalData(self, reqId, bar):
        self.data.append({
            "date":   bar.date,
            "open":   bar.open,
            "high":   bar.high,
            "low":    bar.low,
            "close":  bar.close,
            "volume": bar.volume
        })
    
    def historicalDataEnd(self, reqId, start, end):
        print(f"Historical Data Ended for {reqId}. Started at {start}, ending at {end}")
        self.df = pd.DataFrame(self.data)
        self.data = []
        self.cancelHistoricalData(reqId)
        self.data_ready.set()
    
    def historicalTicksBidAsk(self, reqId: int, ticks: list, done: bool):
        for tick in ticks:
            self.data.append({
                "time": tick.time,
                "bidPrice": tick.priceBid,
                "askPrice": tick.priceAsk,
                "bidSize": tick.sizeBid,
                "askSize": tick.sizeAsk
            })
        
        if done:
            print(f"Tick request {reqId} complete.")
            self.df = pd.DataFrame(self.data)
            self.data = []
            self.data_ready.set()