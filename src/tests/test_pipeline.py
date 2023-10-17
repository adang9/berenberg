import pytest
import pandas as pd
from pipeline.data_func import load_data, filter_executions, add_side_column, merge_with_reference_data, convert_datetime_columns, calculate_bbo

@pytest.fixture
def sample_data():
    executions_df = pd.DataFrame({
        'Trade_id': [1, 2, 3],
        'Phase': ['CONTINUOUS_TRADING', 'NON_TRADING', 'CONTINUOUS_TRADING'],
        'Quantity': [100, -50, 75],
        'ISIN': ['ISIN1', 'ISIN2', 'ISIN3'],
        'TradeTime': ['2023-10-16 10:00:00', '2023-10-16 10:01:00', '2023-10-16 10:02:00'],
        'listing_id': [101, 102, 103],
    })
    market_data_df = pd.DataFrame({
        'listing_id': [1, 2, 3],
        'event_timestamp': ['2023-10-16 10:00:00', '2023-10-16 10:01:00', '2023-10-16 10:02:00'],
        'market_id': [1, 2, 3],
        'best_bid_price': [10.0, 20.0, 30.0],
        'best_ask_price': [9.0, 19.0, 29.0],
    })
    reference_data_df = pd.DataFrame({
        'ISIN': ['ISIN1', 'ISIN2', 'ISIN3'],
        'id': [101, 102, 103],
        'primary_ticker': ['TICKER1', 'TICKER2', 'TICKER3'],
        'primary_mic': ['MIC1', 'MIC2', 'MIC3'],
    })
    return executions_df, market_data_df, reference_data_df

@pytest.fixture
def sample_data_bbo():
    executions_df = pd.DataFrame({
        'Trade_id': [1, 2],
        'TradeTime_unix': [1000, 2000],
        'listing_id': [101, 102],
    })
    market_data_df = pd.DataFrame({
        'listing_id': [101, 102, 101],
        'event_timestamp_unix': [1500, 2500, 1800],
        'market_id': [1, 2, 3],
        'best_bid_price': [10.0, 20.0, 30.0],
        'best_ask_price': [9.0, 19.0, 29.0],
    })
    return executions_df, market_data_df

def test_load_data(sample_data):
    executions_df, market_data_df, reference_data_df = sample_data
    assert not executions_df.empty
    assert not market_data_df.empty
    assert not reference_data_df.empty

def test_filter_executions(sample_data):
    executions_df, _, _ = sample_data
    filtered_df = filter_executions(executions_df)
    assert 'NON_TRADING' not in filtered_df['Phase'].values

def test_add_side_column(sample_data):
    executions_df, _, _ = sample_data
    executions_df_with_side = add_side_column(executions_df)
    assert 'side' in executions_df_with_side.columns
    assert executions_df_with_side['side'].nunique() == 2

def test_merge_with_reference_data(sample_data):
    executions_df, _, reference_data_df = sample_data
    merged_df = merge_with_reference_data(executions_df, reference_data_df)
    assert 'listing_id' in merged_df.columns
    assert 'primary_ticker' in merged_df.columns

def test_convert_datetime_columns(sample_data):
    executions_df, market_data_df, _ = sample_data
    converted_executions_df, converted_market_data_df = convert_datetime_columns(executions_df, market_data_df)
    assert 'TradeTime_unix' in converted_executions_df.columns
    assert 'event_timestamp_unix' in converted_market_data_df.columns

# Test the calculate_bbo function
# def test_calculate_bbo(sample_data_bbo):
#     executions_df, market_data_df = sample_data_bbo
#     updated_executions_df = calculate_bbo(executions_df, market_data_df)
