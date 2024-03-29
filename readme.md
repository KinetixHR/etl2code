# Data Warehouse: Salesforce ETL Scripts, 2nd implementation (dw2_)

This repository contains ETL (Extract, Transform, Load) scripts for extracting data from Salesforce related to Contacts, Companies, Users, Placements, Fee Tiers, Jobs, and Submittals. The scripts also incorporate daily updates with effective and end-dating for Users, Submittals, Placements, and Jobs.

## Features

- **Data Extraction**: Fetch data from Salesforce objects including Contacts, Companies, Users, Placements, Fee Tiers, Jobs, and Submittals.

- **Daily Updates**: The scripts are designed to handle daily updates efficiently, ensuring that only the changes made since the last extraction are processed.

- **Effective Dating**: Manage historical data by incorporating effective dating for Users, Submittals, Placements, and Jobs. This allows you to track changes over time and analyze historical trends.

## Requirements

- **Python**: The scripts are written in Python, ensuring cross-platform compatibility.

- **Salesforce API Access**: You need to have Salesforce API access credentials to authenticate and fetch data.

- **Dependencies**: Install the required Python libraries if copying over locally.

## Configuration

1. #todo: **Salesforce Connection**: Update the `config.py` file with your Salesforce connection details, including the API endpoint, username, password, and security token.

2. **Data Storage**: Data is stored in an Azure Database.

## Usage

1. **Run the ETL Scripts**: Execute the ETL scripts based on your data extraction needs. You can schedule these scripts to run daily for incremental updates.

2. **Review Logs and Output**: Check the logs and output files generated by the scripts for any errors or warnings. These files will provide insights into the extraction process.

3. **Scheduling with `crontab` (On Linux VM in Azure)**: To automate daily script executions, we schedule them using `crontab` on our Linux VM hosted in Azure.

## To-do
- Make dates in Database DATE() not VARCHAR()

- make a config.py to abstract the connection details away from the individual scripts
