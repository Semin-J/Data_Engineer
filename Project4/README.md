
## Fact Tables
>`songplays` 
> records in log data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## Dimension Tables
>`users`
>users in the app
>- user_id, first_name, last_name, gender, level

>`songs`
>songs in music database
>- song_id, title, artist_id, year, duration

>`artists`
>artists in music database
>- artist_id, name, location, lattitude, longitude

>`time`
>timestamps of records in songplays broken down into specific units
>- start_time, hour, day, week, month, year, weekday

## etl.py
`process_song_data` extracts data for `songs` and `artists` tables from song_data,
drops duplicated data, makes partitions by certain criteria, and load on tables

`process_log_data` extracts data for `users`, `songplays`, and `time` tables from log_data,
drops duplicated data, wrangles data in certain formats, and load on tables

Both Functions are working in 3 steps:
    1) Grab JSON formatted files which include songs, artists, users, and time data
    2) Use Spark functions to process the JSON formatted data
    3) Load on AWS