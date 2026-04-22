import numpy as np
import pandas as pd

def rolling_frvp(df:pd.DataFrame, window:int=50, bins:int=200, value_area:float=0.7):
    """
    Calculate the Rolling Fixed Range Volume Profile (FRVP) for a price series.

    For each bar, computes the Volume Profile over a fixed lookback window and
    derives three key levels: the Point of Control (POC), Value Area High (VAH),
    and Value Area Low (VAL). Each bar's volume is distributed evenly across all
    price bins that fall within its High-Low range.

    Parameters
    ----------
    df :         OHLCV dataframe with columns: 'High', 'Low', 'Volume'. Index should be ordered chronologically. 
                 Using a smaller bar size is recommended for more accurate results. 
    window :     Number of bars in the lookback window used to construct each volume profile.
    bins :       Number of price bins used to discretise the High-Low range when building the volume profile. 
                 Higher values give finer resolution but increase computation time. Default is 200.
    value_area : The value area is calculated using the standard CME method: starting at the POC and expanding 
                 outward one bin at a time, always taking the side with the greater volume, until the target % 
                 is reached of total window volume is captured.

    Returns
    -------
    pd.DataFrame
        A copy of the input dataframe with three additional columns:
        - ``POC`` : Point of Control — the price bin with the highest
          volume in the window.
        - ``VAH`` : Value Area High — the upper bound of the price range
          containing 70% of total volume, expanded outward from the POC.
        - ``VAL`` : Value Area Low — the lower bound of the same value area.
    """
    vahs, vals, pocs = [], [], []

    for i in range(len(df)):
        if i < window:
            vahs.append(np.nan)
            vals.append(np.nan)
            pocs.append(np.nan)
            continue

        slice_df = df.iloc[i - window:i + 1]

        price_min = slice_df['Low'].min()
        price_max = slice_df['High'].max()
        price_bins = np.linspace(price_min, price_max, bins + 1)
        bin_centers = (price_bins[:-1] + price_bins[1:]) / 2

        v_profile = np.zeros(bins)

        for _, row in slice_df.iterrows():
            lo, hi, vol = row['Low'], row['High'], row['Volume']
            mask = (bin_centers >= lo) & (bin_centers <= hi)
            n_bins = mask.sum()
            if n_bins > 0:
                v_profile[mask] += vol / n_bins 

        poc_idx = np.argmax(v_profile)

        total_vol = v_profile.sum()
        target_vol = total_vol * value_area
        va_vol = v_profile[poc_idx]
        up, lo = poc_idx, poc_idx

        while va_vol < target_vol:
            v_up = v_profile[up + 1] if up + 1 < bins else 0
            v_lo = v_profile[lo - 1] if lo > 0 else 0
            if v_up == 0 and v_lo == 0:
                break
            if v_up >= v_lo:
                up += 1
                va_vol += v_up
            else:
                lo -= 1
                va_vol += v_lo

        vahs.append(price_bins[up + 1])
        vals.append(price_bins[lo])
        pocs.append(bin_centers[poc_idx])

    df = df.copy()
    df['VAH'] = vahs
    df['VAL'] = vals
    df['POC'] = pocs
    return df