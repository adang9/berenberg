import pandas as pd

def load_data(executions_filepath, market_filepath, ref_filepath):
    executions_df = pd.read_parquet(executions_filepath)
    market_data_df = pd.read_parquet(market_filepath)
    reference_data_df = pd.read_parquet(ref_filepath)


    return executions_df, market_data_df, reference_data_df

def filter_executions(executions_df):
    return executions_df[executions_df['Phase'] == 'CONTINUOUS_TRADING']

def add_side_column(executions_df):
    executions_df['side'] = 1
    executions_df.loc[executions_df['Quantity'] < 0, 'side'] = 2
    return executions_df

def merge_with_reference_data(executions_df, reference_data_df):
    executions_df = pd.merge(executions_df, reference_data_df[['ISIN', 'id', 'primary_ticker', 'primary_mic']], on='ISIN', how='left')
    executions_df = executions_df.rename(columns={"id": "listing_id"})
    return executions_df

def convert_datetime_columns(executions_df, market_data_df):
    executions_df['TradeTime'] = pd.to_datetime(executions_df['TradeTime'])
    market_data_df["event_timestamp"] = pd.to_datetime(market_data_df["event_timestamp"])
    executions_df['TradeTime_unix'] = executions_df['TradeTime'].astype("int64")
    market_data_df["event_timestamp_unix"] = market_data_df["event_timestamp"].astype('int64')

    
    market_data_df.sort_values("event_timestamp", ascending=True).reset_index(drop=True, inplace=True)
    market_data_df = market_data_df.reset_index().rename(columns={"index": "market_id"})
    return executions_df, market_data_df

def calculate_bbo(executions_df, market_data_df):
    for index, row in executions_df.iterrows():
        trade_id = row['Trade_id']
        trade_time = row['TradeTime_unix']
        listing_id = row['listing_id']

        matching_rows = market_data_df[market_data_df['listing_id'] == listing_id]

        # Find the market_id at the execution time
        matching_rows_at_id = matching_rows[matching_rows["event_timestamp_unix"] == trade_time]["market_id"].max()

        try:
            # Get best bid and ask at the execution time
            best_bid_at_time = matching_rows[(matching_rows["listing_id"] == listing_id) & (matching_rows["market_id"] == matching_rows_at_id)]["best_bid_price"].values[0]
            best_ask_at_time = matching_rows[(matching_rows["listing_id"] == listing_id) & (matching_rows["market_id"] == matching_rows_at_id)]["best_ask_price"].values[0]
        except IndexError:
            best_bid_at_time = float('nan')
            best_ask_at_time = float('nan')

        executions_df.loc[executions_df["Trade_id"] == trade_id, "best_bid"] = best_bid_at_time
        executions_df.loc[executions_df["Trade_id"] == trade_id, "best_ask"] = best_ask_at_time

        # Find the market_id one second before execution
        matching_rows_before_id = matching_rows[matching_rows["event_timestamp_unix"] < trade_time]["market_id"].max()

        try:
            # Get best bid and ask one second before execution
            best_bid_before_time = matching_rows[(matching_rows["listing_id"] == listing_id) & (matching_rows["market_id"] == matching_rows_before_id)]["best_bid_price"].values[0]
            best_ask_before_time = matching_rows[(matching_rows["listing_id"] == listing_id) & (matching_rows["market_id"] == matching_rows_before_id)]["best_ask_price"].values[0]
        except IndexError:
            best_bid_before_time = float('nan')
            best_ask_before_time = float('nan')

        executions_df.loc[executions_df["Trade_id"] == trade_id, "best_bid_min_1s"] = best_bid_before_time
        executions_df.loc[executions_df["Trade_id"] == trade_id, "best_ask_min_1s"] = best_ask_before_time

        # Find the market_id one second after execution
        matching_rows_after_id = matching_rows[matching_rows["event_timestamp_unix"] > trade_time]["market_id"].min()

        try:
            # Get best bid and ask one second after execution
            best_bid_after_time = matching_rows[(matching_rows["listing_id"] == listing_id) & (matching_rows["market_id"] == matching_rows_after_id)]["best_bid_price"].values[0]
            best_ask_after_time = matching_rows[(matching_rows["listing_id"] == listing_id) & (matching_rows["market_id"] == matching_rows_after_id)]["best_ask_price"].values[0]
        except IndexError:
            best_bid_after_time = float('nan')
            best_ask_after_time = float('nan')

        executions_df.loc[executions_df["Trade_id"] == trade_id, "best_bid_1s"] = best_bid_after_time
        executions_df.loc[executions_df["Trade_id"] == trade_id, "best_ask_1s"] = best_ask_after_time

    return executions_df

def calculate_mid_price_and_slippage(df):
    # Calculate the Mid-Price at execution
    df['mid_price'] = (df['best_bid'] + df['best_ask']) / 2

    # Calculate the Mid-Price 1s before execution
    df['mid_price_min_1s'] = (df['best_bid_min_1s'] + df['best_ask_min_1s']) / 2

    # Calculate the Mid-Price 1s after execution
    df['mid_price_1s'] = (df['best_bid_1s'] + df['best_ask_1s']) / 2

    # Calculate Slippage
    df['slippage'] = 0.0  # Initialize the slippage column with zeros

    # Calculate slippage for SELL orders
    sell_mask = df['side'] == 2
    df.loc[sell_mask, 'slippage'] = (df.loc[sell_mask, 'Price'] - df.loc[sell_mask, 'best_bid']) / (df.loc[sell_mask, 'best_ask'] - df.loc[sell_mask, 'best_bid'])

    # Calculate slippage for BUY orders
    buy_mask = df['side'] == 1
    df.loc[buy_mask, 'slippage'] = (df.loc[buy_mask, 'best_ask'] - df.loc[buy_mask, 'Price']) / (df.loc[buy_mask, 'best_ask'] - df.loc[buy_mask, 'best_bid'])

    return df

