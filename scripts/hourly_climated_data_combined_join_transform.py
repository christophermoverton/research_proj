import pandas as pd

# Set the base directory
base_dir = "/media/christopher/Extreme SSD/research_project_data/climate_data/"

# Read the CSV files
degh_df = pd.read_csv(base_dir + "degh_filtered_aggregated.csv")
hidx_df = pd.read_csv(base_dir + "hidx_filtered_aggregated.csv")
wchl_df = pd.read_csv(base_dir + "wchl_aggregated.csv")

# Perform a left inner join on 'month', 'day', 'hour', and 'state'
joined_df = degh_df.merge(hidx_df, on=['month', 'day', 'hour', 'state'], how='inner')
joined_df = joined_df.merge(wchl_df, on=['month', 'day', 'hour', 'state'], how='inner')

# Save the joined DataFrame to a descriptive CSV file
joined_df.to_csv(base_dir + "combined_climate_data.csv", index=False)