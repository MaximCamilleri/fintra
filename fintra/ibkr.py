from utils.ibkr_client import IbkrClient
from ibapi.client import Contract
from datetime import datetime, timedelta
import time
import threading
import pandas as pd
import logging 

logger = logging.getLogger(__name__)

def get_ohlcv(
        endDateTime:str, duration:str, barSize:str, contract:Contract, 
        address:str="127.0.0.1", port:int=4002, timeout:int=30
    ) -> pd.DataFrame: 
    client = IbkrClient()
    client.connect(address, port, 10)

    threading.Thread(target=client.run).start()
    time.sleep(1)

    try: 
        client.reqHistoricalData(
            reqId = client.nextId(), 
            contract = contract, 
            endDateTime = endDateTime, 
            durationStr = duration, 
            barSizeSetting = barSize, 
            whatToShow = "TRADES", 
            useRTH = 1, 
            formatDate = 1, 
            keepUpToDate = False, 
            chartOptions = []
        )

        client.data_ready.wait(timeout=timeout)

        if client.data_ready.is_set():
            df = client.df
            df['notional_volume'] = df['close'] * df['volume']
            return df
        else:
            print("Timed out waiting for data")
            return pd.DataFrame()
    finally:
        client.disconnect()
    
def get_ofi(
        startDateTime:datetime, endDateTime:datetime, contract:Contract, 
        address:str="127.0.0.1", port:int=4002, timeout:int=30
    ) -> pd.DataFrame: 
    client = IbkrClient()
    client.connect(address, port, 10)

    threading.Thread(target=client.run).start()
    time.sleep(1)

    current_time = startDateTime
    all_results = []

    logger.debug(f"Starting Tick Extraction for {contract.symbol}")

    try:
        while current_time < endDateTime:
            client.data_ready.clear()
            formatted_time = current_time.strftime("%Y%m%d %H:%M:%S")
            client.reqHistoricalTicks(client.current_req_id, contract, formatted_time, "", 1000, "BidAsk", 1, True, [])
            
            if not client.data_ready.wait(timeout=timeout):
                logger.error("Request timed out.")
                break
     
            if not client.data:
                break

            last_tick_ts = client.data[-1]["time"]
            current_time = last_tick_ts + timedelta(seconds=1)
            
            all_results.extend(client.data)
            
            time.sleep(0.1) # Pacing to Avoid hitting IBKR limits

    except KeyboardInterrupt:
        logger.info("Stopped by user.")
        client.disconnect()
    
    if all_results:
        df = pd.DataFrame(all_results)
        
        # Calculate OFI (Vectorized)
        df['prev_bid_p'] = df['bidPrice'].shift(1)
        df['prev_bid_s'] = df['bidSize'].shift(1)
        df['prev_ask_p'] = df['askPrice'].shift(1)
        df['prev_ask_s'] = df['askSize'].shift(1)

        # OFI Logic
        df['e_b'] = 0
        df.loc[df['bidPrice'] > df['prev_bid_p'], 'e_b'] = df['bidSize']
        df.loc[df['bidPrice'] < df['prev_bid_p'], 'e_b'] = -df['prev_bid_s']
        df.loc[df['bidPrice'] == df['prev_bid_p'], 'e_b'] = df['bidSize'] - df['prev_bid_s']

        df['e_a'] = 0
        df.loc[df['askPrice'] < df['prev_ask_p'], 'e_a'] = -df['askSize']
        df.loc[df['askPrice'] > df['prev_ask_p'], 'e_a'] = df['prev_ask_s']
        df.loc[df['askPrice'] == df['prev_ask_p'], 'e_a'] = df['askSize'] - df['prev_ask_s']

        df['ofi'] = df['e_b'] - df['e_a']
        
        # Resample to 1-minute bars
        df.set_index('time', inplace=True)
        minute_ofi = df['ofi'].resample('1min').sum()

        return minute_ofi
        
    else:
        print("No data collected.")

    client.disconnect()
    