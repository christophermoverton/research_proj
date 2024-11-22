import os
import pandas as pd

# Directory to store population and land area data
base_directory = "/media/christopher/Extreme SSD"
population_data_dir = os.path.join(base_directory, "research_project_data", "population_land_area_data")
os.makedirs(population_data_dir, exist_ok=True)  # Ensure the output directory exists

# Load the population data
population_file = os.path.join(population_data_dir, "PEPPOP2019.PEPANNRES-2024-11-17T010836.csv")
population_df = pd.read_csv(population_file, encoding='utf-8-sig')

# Remove commas from population columns to ensure correct data types
for column in population_df.columns:
    if "Population" in column:
        population_df[column] = population_df[column].str.replace(',', '').astype(float)

# Filter rows for specific states: California, Texas, Florida, New York
states_of_interest = ["California", "Texas", "Florida", "New York"]
filtered_df = population_df[population_df["Geographic Area Name (Grouping)"].isin(states_of_interest)].copy()

# Create a new DataFrame to store year, population, and state columns
population_data = []
state_code_mapping = {
    "California": "CA",
    "Texas": "TX",
    "Florida": "FL",
    "New York": "NY"
}

# Iterate over the filtered data and extract the required columns
years = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]
columns = [
    "4/1/2010 Census population!!Population",
    "7/1/2010 population estimate!!Population",
    "7/1/2011 population estimate!!Population",
    "7/1/2012 population estimate!!Population",
    "7/1/2013 population estimate!!Population",
    "7/1/2014 population estimate!!Population",
    "7/1/2015 population estimate!!Population",
    "7/1/2016 population estimate!!Population",
    "7/1/2017 population estimate!!Population",
    "7/1/2018 population estimate!!Population",
    "7/1/2019 population estimate!!Population"
]

for index, row in filtered_df.iterrows():
    state_name = row["Geographic Area Name (Grouping)"]
    state_code = state_code_mapping[state_name]
    for year, column in zip(years, columns):
        population_data.append({
            "year": year,
            "population": row[column],
            "state": state_code
        })

# Convert to DataFrame
population_df_final = pd.DataFrame(population_data)

# Save the population data to CSV
population_output_file = os.path.join(population_data_dir, "population_data_2010_2019.csv")
population_df_final.to_csv(population_output_file, index=False)

print(f"Population dataframe saved to {population_output_file}")
