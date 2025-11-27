# <img width="127" height="51" alt="Captura de Tela 2025-11-27 às 13 12 47" src="https://github.com/user-attachments/assets/d61c7c6d-79e0-48d3-a980-1ae2e2a8fd96" />
BEES Data Engineering - Breweries Case - Rachid

The goal of this test is to assess your skills in consuming data from an API, transforming and persisting it into a data lake following the medallion architecture with three layers: raw data, curated data partitioned by location, and an analytical aggregated layer.

📊 Project Status

Status: Active

Last Dataset Update: 

Maintenance: Actively maintained by owner

Dataset Size: 8,000+ breweries


🔧 Requirements

- Python v
- Docker v

# Features
- Breweries Data: Using the Open Brewery DB API to fetch data, listing breweries companies: [Open Brewery DB](https://www.openbrewerydb.org/).
- Docker Support: Easily deployable using Docker.
- Orchestration: This project will use Airflow v4.1.0.
- DataLake Architecture: Data will follow the Medallion Architecture.
- Monitoring/Alerts: 

## Breweries Data:

1. Main Goal:
   Fetch Data from the [API List Breweries](https://www.openbrewerydb.org/documentation#list-breweries)

2. Logic:

   a. The code will extract from the MetaData API the total amount of Breweries;
   b. The code will use this total amount of Breweries as variable to determinate the amount of pages for the API List Breweries;
   c. The code will run the API List Breweries and extract from each page a total of 200 Breweries;
   d. 



   The data consists in a list of JSON items with the following body:

    GET https://api.openbrewerydb.org/v1/breweries
   
    ```json
    {
        "id": "5128df48-79fc-4f0f-8b52-d06be54d0cec",
        "name": "(405) Brewing Co",
        "brewery_type": "micro",
        "address_1": "1716 Topeka St",
        "address_2": null,
        "address_3": null,
        "city": "Norman",
        "state_province": "Oklahoma",
        "postal_code": "73069-8224",
        "country": "United States",
        "longitude": -97.46818222,
        "latitude": 35.25738891,
        "phone": "4058160490",
        "website_url": "http://www.405brewing.com",
        "state": "Oklahoma",
        "street": "1716 Topeka St"
    }
   ```
    Also, we can use the Query Parameters available on the Docs
    
    <img width="1103" height="535" alt="Captura de Tela 2025-11-27 às 13 43 47" src="https://github.com/user-attachments/assets/1268bfa7-98b2-4179-91e2-c74f1e322786" />


5. 


# Walktrough

> [!WARNING]
Deploy this application into a **Linux** Operational System.

First, start the application by building the image:

    docker build -t bees-case .

And then, run it:

    docker run -d -p 8000:8000 --name beescontainer bees-case

Also, if you do not have the Docker installed in your instance, please, follow these instructions: [Install Docker Engine on Ubuntu](https://docs.docker.com/engine/install/ubuntu/).


