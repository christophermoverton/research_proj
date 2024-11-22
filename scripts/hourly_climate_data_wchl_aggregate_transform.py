import pandas as pd

# Set the base directory
base_dir = "/media/christopher/Extreme SSD/research_project_data/climate_data/"

# Read the CSV file
file_path = base_dir + "wchl_filtered.csv"
df = pd.read_csv(file_path)

# Filter out rows with -9999 in the 'HLY-WCHL-NORMAL' column
df = df[df['HLY-WCHL-NORMAL'] != -9999]

# Group the data by month, day, hour, and state, then calculate the average of 'HLY-WCHL-NORMAL'
df_grouped = df.groupby(['month', 'day', 'hour', 'state'])['HLY-WCHL-NORMAL'].mean().reset_index()

# Print the aggregated DataFrame
print(df_grouped.head(20))

# Optionally, save the aggregated DataFrame to a new CSV file
df_grouped.to_csv(base_dir + "wchl_aggregated.csv", index=False)