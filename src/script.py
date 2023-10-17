import pandas as pd
import time
from pipeline.data_func import load_data, filter_executions, add_side_column, merge_with_reference_data, convert_datetime_columns, calculate_bbo, calculate_mid_price_and_slippage

start_time = time.time()

# File paths
executions_filepath = "./data/executions.parquet"
market_filepath = "./data/marketdata.parquet"
ref_filepath = "./data/refdata.parquet"

# Load data
executions_df, market_data_df, reference_data_df = load_data(executions_filepath, market_filepath, ref_filepath)

# Count the number of executions
total_executions = len(executions_df)
# Count the unique number of venues
unique_venues = executions_df['Venue'].nunique()
# Date of executions
date_of_executions = pd.to_datetime(executions_df['TradeTime']).dt.date.nunique()

# Log output
print(f"Total Executions: {total_executions}")
print(f"Unique Venues: {unique_venues}")
print(f"Date of Executions: {date_of_executions}")


# Filter for CONTINUOUS_TRADING trades
executions_df = filter_executions(executions_df)

# Count the number of executions
total_executions = len(executions_df)

# Log output
print(f"Total Executions: {total_executions}")

# Add 'side' column
executions_df = add_side_column(executions_df)

# Merge with reference data
executions_df = merge_with_reference_data(executions_df, reference_data_df)

# Convert datetime columns to Unix timestamp to help with calculations
executions_df, market_data_df = convert_datetime_columns(executions_df, market_data_df)

# Perform calcs
output_df = calculate_bbo(executions_df, market_data_df)
output_df = calculate_mid_price_and_slippage(output_df)

# Save the result to a CSV file
output_filepath = './output/executions_df_output.csv'
output_df.to_csv(output_filepath, header=True)

print(f"Data saved to {output_filepath}")

# log time
end_time = time.time()
execution_time = end_time - start_time
print(f"Execution Time: {execution_time} seconds")