# Udacity Final Project - Spark + AWS Data Lake and ETL

## Purpose of this Project

I'd like to prepare this database for an analytics table to determine migration patterns correlation to temperature.

## Steps to clean data

I took the step of dropping duplicates in order to remove duplicate data from all my dataframes. Also there were white spaces in the state names and I was making a composite key, so I chose to clean that up by replacing whitespaces with an underscore in my composite primary key.

## Data Model and steps to ETL

I've included an ER diagram to illustrate my data model below. I chose this particular fact/dimension model in order to be able to quickly query the table for information about the immigrants relationship to weather. It's not 100% foolproof, but this should give a good indication as to how the origin country's weather correlates to their current city's weather. It was conventient to seperate the data into logical divisions and then create a fact table with references to the other tables.

To create the ETL I set up an EMR instance with a Spark cluster to process the source files, reading them in from an S3 bucket. I then ran transformations on that big data, distributing it out into separate tables and then writing it back into an S3 data lake.

## Database structure overview

![ER Diagram](https://github.com/trevransom/dend-final-spark/blob/039dd79ab998b6474b6e30ce91b0f41f32b421b3/db_er_diagram.png)

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

## Closing Thoughts

The goal would be to create an analytics table where analysts could gain insights on how weather and temperature affects immigrants migration choices. I'd like to run queries grouping immigrants by similar origin country and then seeing if they ended up in cities with similar weather or different weather. I did use Spark for this but if I was to use Airflow, I'd probably use it if there was more data incoming frequently. I'd then like to create an ongoing, scheduled task that would take in the new data, format it into my tables and then append it to the Parquet files under appropriately named folders.

If the data was increased by 100x I would probably try to programatically process it in smaller chunks and still use Spark. Perhaps I'd upgrade the Spark cluster too to be more powerful.
If the pipelines were run on a daily basis by 7am I'd use Airflow to get my process set up and then have an email trigger sent off if something went wrong.
If the database needed to be accessed by 100+ people I think my current set up would work, because S3 can be accessed by many people at the same time.