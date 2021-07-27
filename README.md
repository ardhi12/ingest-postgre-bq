# Ingest PostgreSQL Table to BigQuery
## Use Case
To be able to analyze large data effectively and efficiently, it is necessary to move the required data into the data warehouse. All data will be divided into some parts and carried out with a schedule, so that data can be moved smoothly and safely. In this case, I will move data from the `customer` table to BigQuery.

## Tech Stack
* Pandas : Data processing using Dataframe
* BigQuery : Data Warehouse
* Airflow : Scheduling tasks

## Prerequisite
* Make sure you have a GCP account and project
* Make sure you have Python 3.6 or above installed on your machine
* Make sure you have Airflow installed on your machine
* Make sure you have PostgreSQL installed on your machine
* Download and load sample database to PostgreSQL [[Link]](https://www.postgresqltutorial.com/postgresql-sample-database/)
* Clone this repository  
`git clone https://github.com/ardhi12/ingest-postgre-bq`
* Install the prerequisite library from requirements.txt   
`pip3 install -r requirements.txt`
* Copy ingest_postgre_bq.py files to your dags directory  
`cp ingest-postgre-bq.py <path_to_your_dags_directory>`
* Change the code according to your configuration, such as project_id, dataset_id, table_id, Google credentials, Postgre engine.

## Run
* Run `airflow webserver -p 8080` to start airflow webserver
* Run `airflow scheduler` to start airflow webserver
* Open [http://localhost:8080](http://localhost:8080) in your browser
* Activate dag by clicking the toggle

## Result
![alt text](https://raw.githubusercontent.com/ardhi12/ingest-postgre-bq/master/img/result_airflow.png)  
![alt text](https://raw.githubusercontent.com/ardhi12/ingest-postgre-bq/master/img/result_bq.png)

## Notes
* Output is a json file that will be located in `output/latest_stock_price_in_IDR.json`
* The output will update automatically according to the schedule
* Share data source comes from yahoo finance, so make sure you have an stable internet connection when running the program