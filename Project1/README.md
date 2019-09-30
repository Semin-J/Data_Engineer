>### Purpose
>A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.
>Their app logs are in json format, and the logs are needed to be store in DB through ETL(Extract, Transform, and Load).

>
>### Schema
>#### Fact table
>`songplays` records in log data associated with song plays
>
>#### Dimension tables
>`users` records in log data associated with song plays
>`songs` songs in music database
>`artists` artists in music database
>`time` timestamps of records in songplays broken down into specific units
>

>
>### Scripts
>#### sql_queries.py
>Containing SQL queries to drop, create tables and insert data into the tables, and the queries are wrapped by python wrapper.
>It doesn't have main, only used as a module to be imported.
>
>#### create_tables.py
>Opening and Closing connection to the Database and dropping and creating tables in Database, using sql_queries module.
>
>#### etl.py
>Processing json files (Songs, Logs) in Pandas DataFrame and loading the data in to Database tables.
