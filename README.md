# iRail Azure Pipeline (MVP)

## Project Overview
This project builds a simple cloud data pipeline on Microsoft Azure that fetches live train departure data from the iRail API, normalizes it, and stores it in an Azure SQL Database.

## Tech Stack
- **Language:** Python 3.11  
- **Cloud:** Microsoft Azure (Functions, SQL Database)  
- **API:** iRail (REST/JSON)  
- **Orchestration:** Trigger-based (MVP) / Timer-based 

## System Architecture
<p align="center">
  <img src="assets/Irail-db-layout.png" alt="Azure Data Pipeline Architecture" width="100%">
</p>

## Project overview

This project builds a simple cloud data pipeline on Microsoft Azure that:

- Fetches live train departure data from the iRail API (Belgium)
- Normalizes the response into structured fields
- Stores the data in an Azure SQL Database
- Runs ingestion using an Azure Function (Python)
# Repo structure

``` bash 
Irail-db
├── .venv/
├── .vscode/
├── assets/
├── .funcignore
├── .gitignore
├── function_app.py
├── host.json
├── local.settings.json
├── README.md
└── requirements.txt
```

## Must-Have deliverables inlcuding

- Azure Function (HTTP endpoint) deployed
- Azure SQL Database with at least one table filled with live data
- Documentation describing setup and testing

## Azure resources used

This MVP uses the following Azure services (created via Azure Portal):

- **Azure SQL Database (serverless, free tier when available)**
  - Stores normalized train departure data
  - Resource group: `rg-irail-mvp`
  - Region: Germany West Central

- **Azure SQL Server**
  - Hosts the database and manages authentication/firewall rules

- **Azure Function App (Python)**
  - Runs the ingestion code (HTTP-triggered function)
  - Function App name: `irail-func-bel`
  - Hosting: Flex Consumption (Azure for Students)

## Why this region

My Azure for Students subscription had limited region availability, so the resources were deployed in **Germany West Central** (closest available to Belgium in my subscription).

## Data Modeling

The database is designed using a **Star Schema** approach. This structure was chosen to follow industry best practices for analytical data, separating business **events** (facts) from descriptive **entities** (dimensions).


### DepartureFact

Stores one row per train departure (event-based fact table).

Main fields:

- `scheduled_time_utc` – planned departure time
- `delay_seconds` – delay in seconds (0 if on time)
- `is_delayed` – flag derived from delay
- `realtime_time_utc` – scheduled time + delay
- `is_cancelled` – cancellation flag when available
- `station_name`, `station_id`
- `destination_name`, `destination_id`
- `vehicle_id`, `train_type`, `platform`
- `ingested_at_utc` – ingestion timestamp

This table is intended for the future to be used in time-series analysis, dashboards, and ML delay prediction.

### StationDim

Stores station metadata (dimension table).

Main fields:

- `station_id`
- `station_uri`
- `station name / standard name`
- `longitude`, `latitude`

Station coordinates are stored once and joined to departures when needed, avoiding data duplication.

## Azure Function logic

The data ingestion is implemented using Azure Functions (Python).

### FetchLiveboardToSql

- Trigger: HTTP request
- Calls the iRail `/liveboard` API for a given station
- Parses the JSON response
- Normalizes relevant fields (time, delay, platform, train type, etc.)
- Inserts new departure records into the `DepartureFact` table
- Uses environment variables for database credentials
- Handles basic rate limits and transient errors

### LoadStations

- Trigger: HTTP request
- Calls the iRail `/stations` API
- Extracts station metadata and coordinates
- Upserts station records into the `StationDim` table
- Intended to be run once or occasionally (station data changes rarely)

## Configuration and environment variables

Sensitive information and configuration values are not hardcoded in the source code.

They are stored as **Application Settings** in the Azure Function App.

Main environment variables:

- `SQL_SERVER` – Azure SQL Server hostname
- `SQL_DATABASE` – Database name
- `SQL_USERNAME` – SQL admin login
- `SQL_PASSWORD` – SQL admin password
- `IRAIL_STATION` – Default station for liveboard requests
- `IRAIL_LANG` – Language for iRail responses
- `IRAIL_USER_AGENT` – User-Agent string recommended by iRail API

## Testing the pipeline

The Azure Functions can be tested both locally and directly in Azure.

### Local testing (VS Code)

- Possible using Azure Functions Core Tools
- Useful for testing Python logic and API parsing

### Cloud testing (Azure Portal)

- Functions were tested directly in Azure using **Code + Test**
- This ensured the code ran in the same environment as production
- Testing in Azure was more reliable for SQL connectivity and serverless database behavior

For this project, cloud-based testing was preferred once the core logic was stable.

## Current status and next steps

### Current status

- The Azure Function successfully ingests live train data from the iRail API
- Data is stored in Azure SQL tables
- Station coordinates are available for geographic analysis

### Next steps (Nice-to-Have)

- Add a Timer Trigger to automate data ingestion
- Build a live dashboard using Power BI
- Analyze delay patterns by station, time, and train type
- Prepare features for a future machine learning model to predict delays

## 👥 Contributors

This project is part of AI & Data Science Bootcamp training at **</becode** and it was done by: 

- Welederufeal Tadege [LinkedIn](https://www.linkedin.com/in/) | [Github](https://github.com/welde2001-bot) 
