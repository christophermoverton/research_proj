import pandas as pd
import os

# Define base directory and population data directory
base_directory = "/media/christopher/Extreme SSD"
population_data_dir = os.path.join(base_directory, "research_project_data", "population_land_area_data")

# Load the CSV file
file_path = os.path.join(population_data_dir, "NST-EST2023-ALLDATA.csv")
data = pd.read_csv(file_path)

# Define state codes dictionary
state_codes = {
    "California": "CA",
    "Texas": "TX",
    "Florida": "FL",
    "New York": "NY"
}

# Filter rows for specific states
selected_states = ["California", "Texas", "Florida", "New York"]
filtered_data = data[data["NAME"].isin(selected_states)]

# Add state code column
filtered_data["State Code"] = filtered_data["NAME"].map(state_codes)

# Select the relevant columns (Year and Population Estimates from 2020 to 2023)
columns_to_extract = ["NAME", "State Code", "POPESTIMATE2020", "POPESTIMATE2021", "POPESTIMATE2022", "POPESTIMATE2023"]
extracted_data = filtered_data[columns_to_extract]

# Reshape the data to have Year and Population columns
extracted_data = extracted_data.melt(id_vars=["NAME", "State Code"], 
                                     value_vars=["POPESTIMATE2020", "POPESTIMATE2021", "POPESTIMATE2022", "POPESTIMATE2023"],
                                     var_name="Year", 
                                     value_name="Population")

# Clean up the Year column to have just the year as an integer
extracted_data["Year"] = extracted_data["Year"].str.extract(r'(\d{4})').astype(int)

# Display the result
#display(extracted_data)

# Optionally, save the extracted data to a new CSV file
output_file_path = os.path.join(population_data_dir, "population_data_2020_2023.csv")
extracted_data.to_csv(output_file_path, index=False)
