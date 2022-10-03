import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *



def process_song_file(cur, filepath):
    '''
    Process songs file, convert every file row to tuples to insert in each table

            Parameters:
                    cur: _Cursor
                    filepath: string that contains song files path

    '''
    # open song file
    df = pd.read_json(filepath, lines=True)
    df = pd.DataFrame(df)   
    
    for data in df.itertuples():
        '''with every row we have all information to insert both tables, artist and songs'''
        # insert artist record
        artist_data = (data.artist_id, data.artist_name, data.artist_location, data.artist_latitude, data.artist_longitude)
        cur.execute(artist_table_insert, artist_data)

        # insert song record
        song_data = (data.song_id, data.title, data.artist_id, data.year, data.duration)
        cur.execute(song_table_insert, song_data)
        


def process_log_file(cur, filepath):
    '''
    Process logs file. From each row, that is NextSong action, you have to insert data in time, user and songplay tables

            Parameters:
                    cur: _Cursor
                    filepath: string that contains log files path

    '''
    # open log file
    df = pd.read_json(filepath, lines=True)
    df = pd.DataFrame(df)   

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    ts = pd.to_datetime(df['ts'],unit='ms') 
    
    # insert time data records
    time_data =  [[x, x.hour, x.day, x.week, x.month, x.year, x.dayofweek] for x in ts]
    column_labels = ['start_time','hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(data=time_data, columns=column_labels)

    '''Insert every time row getting info from start_time column '''
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    '''Insert user table with file data'''
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        '''execute select to get song and artist from tables with title, artist name and duration appears in log data'''
        if results:
            songid, artistid = results
            print('Found:  ',row.song)
        else:
            songid, artistid = None, None
            #print('Not found:  ',row.song)


        # insert songplay record
        '''Insert logs data, with or without songs and artist'''
        songplay_data = (pd.to_datetime(row.ts),row.userId,row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)




def process_data(cur, conn, filepath, func):
    '''
    get all json files recursively from filepath and calling process_song_file or process_song_file functions 

            Parameters:
                    cur: _Cursor
                    conn: connection
                    filepath: string that contains files path
                    func: function name to process files (rocess_song_file or process_song_file)

    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''sparkifydb Database connection'''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    '''Process first songs files data '''
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    '''After loading songs, you have to load logs '''
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    '''close database connection'''
    conn.close()


if __name__ == "__main__":
    main()