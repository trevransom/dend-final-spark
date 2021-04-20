# Udacity Final Project - Spark + AWS Data Lake and ETL

## Project Summary

Sparkify, a music streaming startup, wanted to collect logs they have on user activity and song data and centralize them in a database in order to run analytics. This AWS S3 data lake, set up with a star schema, will help them to easily access their data in an intuitive fashion and start getting rich insights into their user base.

I set up an EMR instance with a Spark cluster to process their logs, reading them in from an S3 bucket. I then ran transformations on that big data, distributing it out into separate tables and then writing it back into an S3 data lake.

## Why this Database and ETL design?

My client Sparkify has moved to a cloud based system and now keeps their big data logs in an AWS S3 bucket. The end goal was to get that raw .json data from their logs into fact and dimension tables in a S3 data lake with parquet files. 

## Database structure overview

![ER Diagram](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/38715/1607614393/Song_ERD.png)
*From Udacity*

## Data Dictionary


| Field Name  | Data Type | Description |
| ------------- | ------------- | ---- |
| cicid  | Integer  | The immigrant unique id |
| origin_country | String  | The immigrant's origin country |
| arrival_city | String | The immigrant's arrival city in the USA |
| state_code | String | The immigrant's arrival state in the USA |
| temp_id | String | The identifier for each unique combination of city and state |
| City | String | The city where the temperature was taken |
| Country | String | The country where the temperature was taken |
| AverageTemperature | Float | The average temperature from 2008-2013 for that particular city | 
| dg_id | String | The unique identifier for the city and state |
| city | String | The city where the population was taken |
| total_population | Integer | The total population of a given city |


'cicid', 'origin_country', 'arrival_city', 'state_code'


| First Header  | Second Header |
| ------------- | ------------- |
| Content Cell  | Content Cell  |
| Content Cell  | Content Cell  |


## How to run

- Start by cloning this repository
- Install all python requirements from the requirements.txt
- Create an S3 bucket and fill in those details in the etl.py `main()` output_data variable
- Initialize an EMR cluster with Spark
- Fill in the dl_template with your own custom details
- SSH into the EMR cluster and upload your `dl_template.cfg` and `etl.py` files
- Run `spark-submit etl.py` to initialize the spark job and write the resultant tables to parquet files in your s3 output path





Data Dictionary 

Immigrant Table (general info on travel from immigrants to the USA)
cicid 
city from
city to
country from

temperature
id
city
avg_temperature
country

demographic (general city info - populations) (USA only - we’ll use this for population information)
id (serial)
city
state
population

fact table (foreign keys of tables + 1 metric
cicid id 
temp_to_id (this requires aggregation) (from temperature table where temperature city and country equals immigrant USA destination city and country)
Demographic id (city they moved to) (from demographic table where city and state code equals immigrant destination city and state code (in the arrive city )


