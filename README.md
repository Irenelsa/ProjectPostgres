# Purpose
Sparkify is a music application that takes inforation from several logs to get information about Songs, Artist and when users plays these songs.

Requeriments:
- This code project is created with python3 
- Database is postgres, and before running this project the user student has to be created with grants CREATEDB


What does this project include?:
- The project has a data folder that contains json files with all the information necessary.
- create_tables.py: We have to run this file before load files in order to create (or recreate) database sparkifydb for user student. 
- etl.py Read files and loads them into the database sparkifydb

ETL Process:
- The process extract songs and logs json that are found in files in data directory. The process find recursively from both filepath and calling process_song_file or process_song_file functions that get every json and make the insert in every table.
- From songs logs you load songs and artists tables
- From logs log you load time and user tables
- In the last process, with logs json, we select songId and artistId that could be related with it and load songsPlays table.

Results:
    You can test this project and data files with test.ipynb that shows you a few rows for each