## Running the docker

To get started run ``` docker-compose up ``` in root directory.
It will create the PostgresSQL database and start generating the data.
It will create an empty MySQL database.
It will launch the analytics.py script. 

The **ETL** process here:
 - **E**xtracts data from source(Postgres)
 - **T**ransforms them, basically aggregates them as per the requirement
 - **L**oads them to the destined place(Mysql Database)

Here, the ETL process is one time run. We can run it as a scheduled job using cron or run it through workflows like
GitHub workflows.
 - As a **cronjob** we can run this job every day at 00:00AM and gather the data for a day only. This way this can decrease
   the load of data-processing.
 - Next, we can run it through **GitHub workflow** sending **start_time** and **end_time** as GitHub input params and control the
   processing of data between those dates.

This code handles the insertion into the database in chunking manner and bulk inserts the chunk data to make the process
faster and also not to overload the database. 
