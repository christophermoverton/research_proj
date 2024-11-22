

# **Real-Time Energy Consumption Prediction with Weather and Climate Data**

## **Overview**
This project focuses on real-time energy consumption forecasting by integrating seasonal and climatic variability using advanced machine learning techniques. The core methodology involves the use of a Long Short-Term Memory (LSTM) model, enriched with features such as weather anomalies, storm event data, and state-specific energy demand metrics. The work aims to enhance grid resilience and support sustainable energy management during extreme weather events.

---

## **Key Features**
- **Integration of Large Datasets:**
  - **Primary Data:**
    - The necessary dataset files and Jupyter Lab file (`Energy_climate_analysis.ipynb`) are included in the `.zip` archive (`dataset.zip`) within this repository.
    - **ISD Data:** Larger data lake files are available at [NOAA ISD Archive](https://www.ncei.noaa.gov/data/global-hourly/archive/isd/), with documentation at [ISD Dataset Documentation](https://www.ncei.noaa.gov/data/global-hourly/doc/). A parsing script for ISD data is included in the repository.
    - **EIA Data:** Extracted using an API and managed via scripts in the `scripts/` folder.
  - **Automated Download:** The `download_dataset.sh` script (for Linux users) automates the retrieval of most other required datasets.

- **Modeling Framework:**
  - Feature-enriched LSTM model incorporating weather anomalies, temporal flags, and severe weather events.
  - Scalable preprocessing pipeline for integrating multi-modal datasets.
  
- **Customizable Scripts:**
  - Most scripts require the user to refactor paths to match their specific directory structure. Path adjustments may be necessary for scripts located in the `scripts/` folder.

---

## **Repository Structure**
```
project/
│
├── dataset/               # Contains processed and raw datasets
├── scripts/               # Python scripts for data extraction, transformation, and modeling
├── venv/                  # Virtual environment folder (excluded in .gitignore)
├── dataset.zip            # Archive containing primary dataset files
├── download_dataset.sh    # Script to automate data download
├── .gitignore             # Ignores large files and unnecessary local configurations
└── README.md              # Project documentation
```

---

## **How to Set Up the Virtual Environment**

This project is designed to run in an isolated Python virtual environment to ensure dependency management and reproducibility. Follow these steps:

1. **Install Python (3.8 or higher recommended)**:
   - Ensure Python is installed on your system. Check the version:
     ```bash
     python3 --version
     ```

2. **Create a Virtual Environment**:
   - In the project root directory, create a virtual environment:
     ```bash
     python3 -m venv venv
     ```

3. **Activate the Virtual Environment**:
   - On macOS/Linux:
     ```bash
     source venv/bin/activate
     ```
   - On Windows:
     ```cmd
     .\venv\Scripts\activate
     ```

4. **Install Dependencies**:
   - Install required packages using `requirements.txt` (if provided):
     ```bash
     pip install -r requirements.txt
     ```

5. **Verify Installation**:
   - Ensure all necessary packages are installed:
     ```bash
     pip list
     ```

6. **Deactivate Virtual Environment (Optional)**:
   - Exit the virtual environment when done:
     ```bash
     deactivate
     ```

---

## **Datasets**

1. **Primary Dataset:**
   - Unzip the `dataset.zip` file located in this repository:
     ```bash
     unzip dataset.zip
     ```
   - The archive contains necessary files and folders, including `cleaned_energy_weather_climate_combined.csv` and `Energy_climate_analysis.ipynb`. Place these files in their respective directories within the project structure.

2. **ISD Data:**
   - Larger data lake files can be found at [NOAA ISD Archive](https://www.ncei.noaa.gov/data/global-hourly/archive/isd/). Use the parsing script in the `scripts/` folder to preprocess this data.

3. **Other Data:**
   - The `download_dataset.sh` script automates the download and setup of most required datasets (for Linux users).

---

## **How to Run the Project**

1. **Prepare Data**:
   - Unzip `dataset.zip` and ensure the files are placed in their appropriate directories within the project structure.
   - Adjust paths in the scripts (found in the `scripts/` folder) as needed to match your local directory structure.

2. **Activate Virtual Environment**:
   - Activate the Python virtual environment:
     ```bash
     source venv/bin/activate
     ```

3. **Run Jupyter Lab**:
   - Start Jupyter Lab for interactive exploration and analysis:
     ```bash
     jupyter lab
     ```
   - Open and run the provided file `Energy_climate_analysis.ipynb` to execute the energy consumption prediction pipeline.

4. **Execute Scripts**:
   - Use the provided scripts in the `scripts/` directory for data extraction, transformation, and modeling.

---

## **Scripts Overview**
- **ISD Parsing**: Prepares ISD datasets for analysis.
- **EIA Data Extraction**: Automates energy data retrieval via API.
- **Data Transformation**: Scripts for cleaning, aggregating, and aligning datasets.

---

## **References**
1. [NOAA ISD Archive](https://www.ncei.noaa.gov/data/global-hourly/archive/isd/)
2. [ISD Dataset Documentation](https://www.ncei.noaa.gov/data/global-hourly/doc/)
3. [EIA Developer to get started](https://www.eia.gov/developer/))

---

This README provides a comprehensive guide to set up and use the project. If you encounter any issues, feel free to raise them in the project repository!
