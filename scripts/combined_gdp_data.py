import pandas as pd
import os

# Define the base directory and target states
base_directory = "/media/christopher/Extreme SSD/research_project_data/SAGDP/"
target_states = ["CA", "FL", "TX", "NY"]

# Define the target TableName values
target_table_names = ["All industry total", "Oil and gas extraction",
                      "Mining (except oil and gas)", "Utilities", "Manufacturing"]

# Initialize an empty dataframe to store the combined data
combined_data = pd.DataFrame()

# Loop through the target states to load and process their respective files
for state in target_states:
    file_name = f"SAGDP2N_{state}_1997_2023.csv"
    file_path = os.path.join(base_directory, file_name)
    
    # Load the data and clean column names
    data = pd.read_csv(file_path, skipinitialspace=True)
    data.columns = data.columns.str.strip()  # Remove extra spaces from column names
    
    # Strip leading/trailing whitespace in the 'Description' column
    if 'Description' in data.columns:
        data['Description'] = data['Description'].str.strip()
        
        # Display the unique values in 'Description'
        print(f"Unique values in 'Description' for state {state}:")
        print(data['Description'].unique())
    
    # Filter the data by TableName
    filtered_data = data[data['Description'].isin(target_table_names)]
    
    # Identify year columns by filtering numeric column names
    year_columns = [col for col in data.columns if col.isdigit()]
    
    # Melt the year columns into a 'year' column and their values into a 'gdp' column
    melted_data = filtered_data.melt(
        id_vars=['GeoFIPS', 'GeoName', 'Region', 'TableName', 'LineCode',
                 'IndustryClassification', 'Description', 'Unit'],
        value_vars=year_columns,
        var_name='year', 
        value_name='gdp'
    )
    
    # Add a state column
    melted_data['state'] = state
    
    # Append to the combined dataframe
    combined_data = pd.concat([combined_data, melted_data], ignore_index=True)

# Display the combined data
#import ace_tools as tools; tools.display_dataframe_to_user(name="Transformed SAGDP Data for Selected States", dataframe=combined_data)

# Save the combined data to a CSV for reference
combined_data.to_csv(os.path.join(base_directory, "combined_sagdp_data.csv"), index=False)
