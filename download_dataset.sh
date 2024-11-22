#!/bin/bash

# Directory to save datasets
mkdir -p datasets
cd datasets

# Open Energy Data Initiative (OEDI)
echo "Downloading OEDI datasets..."
# Residential Energy Consumption Survey (RECS)
#wget -O RECS_data.csv "https://www.eia.gov/consumption/residential/data/2020/csv/recs2020_public.csv"
# Directory to save RECS data
mkdir -p RECS_data
cd RECS_data

# Function to download a file in XLSX format only
download_xlsx() {
    local filename=$1
    local xlsx_url=$2

    echo "Downloading $filename in XLSX format..."
    wget -O "${filename}.xlsx" "$xlsx_url"
}

# Summary Statistics
download_xlsx "CE1.1_Summary_US" "https://www.eia.gov/consumption/residential/data/2020/c&e/xls/ce1.1.xlsx"
download_xlsx "CE1.2_Summary_Northeast" "https://www.eia.gov/consumption/residential/data/2020/c&e/xls/ce1.2.xlsx"
download_xlsx "CE1.3_Summary_Midwest" "https://www.eia.gov/consumption/residential/data/2020/c&e/xls/ce1.3.xlsx"
download_xlsx "CE1.4_Summary_South" "https://www.eia.gov/consumption/residential/data/2020/c&e/xls/ce1.4.xlsx"
download_xlsx "CE1.5_Summary_West" "https://www.eia.gov/consumption/residential/data/2020/c&e/xls/ce1.5.xlsx"

# By Fuel
download_xlsx "CE2.1_Fuel_Consumption_US" "https://www.eia.gov/consumption/residential/data/2020/c&e/xls/ce2.1.xlsx"
download_xlsx "CE2.2_Fuel_Consumption_Northeast" "https://www.eia.gov/consumption/residential/data/2020/c&e/xls/ce2.2.xlsx"
# Add more for CE2.3 to CE2.10 as needed

# By End Uses
download_xlsx "CE3.1_End_Use_Consumption_US" "https://www.eia.gov/consumption/residential/data/2020/c&e/xls/ce3.1.xlsx"
# Add more for CE3.2 to CE3.10 following the same URL pattern

# By End Uses by Fuel
download_xlsx "CE4.1_End_Use_By_Fuel_US" "https://www.eia.gov/consumption/residential/data/2020/c&e/xls/ce4.1.xlsx"
# Continue for CE4.2 to CE4.20 if needed

# Detailed End-Use Consumption and Expenditure Estimates
download_xlsx "CE5.1a_Detailed_Electricity_Part1" "https://www.eia.gov/consumption/residential/data/2020/c&e/xls/ce5.1a.xlsx"
# Add more for CE5.1b to CE5.8 following the same URL pattern

# Wood Consumption
download_xlsx "CE7.1_Wood_Types" "https://www.eia.gov/consumption/residential/data/2020/c&e/xls/ce7.1.xlsx"
download_xlsx "CE7.2_Household_Wood_Consumption" "https://www.eia.gov/consumption/residential/data/2020/c&e/xls/ce7.2.xlsx"

echo "All XLSX files downloaded to RECS_data directory."
cd ..
# Commercial Building Energy Consumption Survey (CBECS)
#wget -O CBECS_data.csv "https://www.eia.gov/consumption/commercial/data/2020/csv/cbecs2020_public.csv"
# List of table identifiers based on the examples provided in your screenshots
table_identifiers=("c1" "c2" "c3" "c4" "c5" "c6" "c7" "c8" "c9" "c10" "c11" "c12" \
                   "c13" "c14" "c15" "c16" "c17" "c18" "c19" "c20" "c21" "c22" \
                   "c23" "c24" "c25" "c26" "c27" "c28" "c29" "c30" "c31" "c32" \
                   "c33" "c34" "c35" "c36" "c37" \
                   "e1" "e2" "e3" "e4" "e5" "e6" "e7" "e8" "e9" "e10" "e11")

# Base URL
base_url="https://www.eia.gov/consumption/commercial/data/2018/ce/xls"

# Download directory
download_dir="./cbecs_data"

# Create the download directory if it doesn't exist
mkdir -p "$download_dir"

# Loop over each table identifier to download the corresponding file
for table_id in "${table_identifiers[@]}"; do
    # Construct the download URL for each table
    url="${base_url}/${table_id}.xlsx"
    
    # Download the file to the specified directory
    wget "$url" -P "$download_dir"
    
    # Check if download was successful
    if [ $? -ne 0 ]; then
        echo "Failed to download $url"
    else
        echo "Downloaded $url successfully"
    fi
done

# Wind Integration National Dataset (WIND)
#wget -O WIND_data.zip "

# Solar Power Data for Integration Studies (Manually specify date range as needed)
#wget -O SolarPowerData.zip "https://www.nrel.gov/grid/solar-power-data.html"

# Data.gov
echo "Downloading Data.gov datasets..."
# Hourly Energy Consumption by State
#wget -O HourlyEnergyConsumption.csv "https://www.eia.gov/opendata/qb.php?category=2122628&sdid=EBA.CONS_TOT.DC-99.H"
# Temperature and Energy Demand Data for U.S. Cities
#wget -O TempEnergyDemand_US_Cities.csv "https://datahub.io/core/global-temp/r/0.csv"
# U.S. Building Energy Data Book
#wget -O US_BuildingEnergyData.csv "https://www.energy.gov/sites/prod/files/2019/07/f64/2019_bedb_data_book_final.pdf"
# Electric Power Monthly
#wget -O ElectricPowerMonthly.csv "https://www.eia.gov/electricity/monthly/current_month/october2024.zip"
# Base URL for archived data
base_url="https://www.eia.gov/electricity/monthly/archive"

# Directory to store downloaded files
download_dir="./electric_power_monthly_data"

# Create the directory if it doesn't exist
mkdir -p "$download_dir"

# Get the current year and month
current_year=$(date +%Y)
current_month=$(date +%m)

# Loop through the past 6 years
for ((year=current_year-6; year<=current_year; year++)); do
    # Loop through each month
    for month in {01..12}; do
        # Skip future months in the current year
        if [ "$year" -eq "$current_year" ] && [ "$month" -gt "$current_month" ]; then
            break
        fi

        # Convert the month number to the full month name in lowercase (e.g., 01 -> "january")
        month_name=$(date -d "$year-$month-01" +"%B" | tr '[:upper:]' '[:lower:]')

        # Construct the download URL (e.g., https://www.eia.gov/electricity/monthly/archive/january2024.zip)
        url="${base_url}/${month_name}${year}.zip"

        # Define the file name for saving
        output_file="${download_dir}/${month_name}${year}.zip"
        
        # Download the file
        wget -O "$output_file" "$url"

        # Check if download was successful
        if [ $? -ne 0 ]; then
            echo "Failed to download $url"
        else
            echo "Downloaded $url successfully to $output_file"
        fi
    done
done

echo "Download completed. Files are saved in $download_dir"
# National Renewable Energy Laboratory (NREL)
echo "Downloading NREL datasets..."
# NSRDB (National Solar Radiation Database)
#wget -O NSRDB_data.zip "https://www.nrel.gov/grid/solar-power-data.html"
#!/bin/bash

# List of state codes to iterate over
state_codes=("al" "ak" "az" "ar" "ca" "co" "ct" "de" "fl" "ga" "hi" "id" "il" "in" "ia" "ks" "ky" "la" "me" "md" "ma" "mi" "mn" "ms" "mo" "mt" "ne" "nv" "nh" "nj" "nm" "ny" "nc" "nd" "oh" "ok" "or" "pa" "ri" "sc" "sd" "tn" "tx" "ut" "vt" "va" "wa" "wv" "wi" "wy")

# Base URL
base_url="https://www.nrel.gov/grid/assets/downloads"

# Directory to save the downloads
download_dir="./solar_radiation_data"

# Create the directory if it doesn't exist
mkdir -p "$download_dir"

# Loop over each state code
for state in "${state_codes[@]}"; do
    # Construct the download URL for each state
    url="${base_url}/${state}-pv-2006.zip"
    
    # Use wget to download the file to the specified directory
    wget "$url" -P "$download_dir"
    
    # Check if download was successful
    if [ $? -ne 0 ]; then
        echo "Failed to download $url"
    else
        echo "Downloaded $url successfully to $download_dir"
    fi
done
# Wind Toolkit Data
#wget -O WindToolkit_data.zip "https://www.nrel.gov/grid/wind-toolkit.html"
# Electricity Consumption and Renewable Energy Data for Integration Studies
#wget -O ElecRenewableIntegration.zip "https://www.nrel.gov/grid/solar-power-data.html"

# International Energy Agency (IEA)
echo "Downloading IEA datasets (Note: Some may require access permissions)..."
# World Energy Balances
wget -O WorldEnergyBalances.csv "https://www.iea.org/product/download/018779-000282-018513"
# World Energy Statistics
wget -O WorldEnergyStatistics.csv "https://www.iea.org/reports/world-energy-statistics-overview"

# NOAA National Centers for Environmental Information (NCEI)
echo "Downloading NOAA datasets..."
# Climate Normals
#wget -O Clima#!/bin/bash

# Root URL for the NOAA climate normals data
base_url="https://www.ncei.noaa.gov/pub/data/normals/1981-2010/source-datasets"

# Main datasets directory for easy recognition
main_dir="./climate_normals_datasets"
mkdir -p "$main_dir"

# Subdirectory for downloads
download_dir="${main_dir}/downloads"
mkdir -p "$download_dir"

# Subdirectory for extracted data
extracted_dir="${main_dir}/extracted"
mkdir -p "$extracted_dir"

# Array of filenames to download
files=("coop.52g.20110530.FLs.52g.tar.gz"
       "coop.52g.20110530.tad.tar.gz"
       "ghcn-all.tar.gz"
       "ghcn-stations.txt"
       "ghcn-version.txt"
       Open Energy Data Initiative (OEDI)"isdlite-normals.tar.gz"
       "mly-prcp-filled.txt"
       "mly-tmax-filled.txt"
       "mly-tmin-filled.txt")

# Loop over each file to download and extract if necessary
for file in "${files[@]}"; do
    # Construct full URL
    url="${base_url}/${file}"
    
    # Download file to the downloads directory
    wget -P "$download_dir" "$url"
    
    # Check if download was successful
    if [ $? -ne 0 ]; then
        echo "Failed to download $file"
        continue
    else
        echo "Downloaded $file successfully"
    fi

    # Move to extracted directory if file is a .tar.gz archive, otherwise copy it directly
    if [[ "$file" == *.tar.gz ]]; then
        tar -xzf "$download_dir/$file" -C "$extracted_dir"
        if [ $? -ne 0 ]; then
            echo "Failed to extract $file"
        else
            echo "Extracted $file successfully"
        fi
    else
        # Copy non-archive files directly to the extracted directory
        cp "$download_dir/$file" "$extracted_dir/"
        echo "Copied $file to extracted directory"
    fi
done

echo "All files downloaded and organized in '$main_dir'."
#teNormals.zip "https://www.ncei.noaa.gov/pub/data/normals/"

# Global Historical Climatology Network (GHCN)
#wget -O GHCN_data.zip "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/"
# Storm Events Database
#wget -O StormEventsDatabase.csv "https://www.ncdc.noaa.gov/stormevents/ftp.jsp"
# URL to the directory containing the storm event CSV files
BASE_URL="https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"

# Directory to store downloaded files
OUTPUT_DIR="./storm_events_data"

# Create the output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Use wget to download all .csv.gz files in the directory
wget -r -l1 -np -nd -A "*.csv.gz" -P "$OUTPUT_DIR" "$BASE_URL"

echo "Download completed. Files are saved in $OUTPUT_DIR"
echo "All datasets downloaded to the datasets directory."
