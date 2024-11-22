import pandas as pd
# File paths
metadata_path = "/home/christopher/research_proj/scripts/small_station_list.txt"
cleaned_metadata_path = "/home/christopher/research_proj/scripts/cleaned_ghcn_stations.txt"

# Expected number of columns (adjust as needed)
expected_columns = 6

# Clean the file
with open(metadata_path, 'r') as file:
    with open(cleaned_metadata_path, 'w') as new_file:
        for line in file:
            # Split by whitespace to count columns
            fields = line.split()
            if len(fields) == expected_columns:
                # Write correctly formatted lines to the new file
                new_file.write(" ".join(fields) + "\n")
            else:
                print(f"Skipping malformed line: {line}")

# Now, read the cleaned file with Pandas
metadata_df = pd.read_csv(cleaned_metadata_path, delim_whitespace=True, header=None, names=column_names)
