import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
%run create_tables.py

conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
cur = conn.cursor()
conn.set_session(autocommit=True)


def process_song_file(cur, filepath):
    """
    This function will open song files and read it into Dataframe which will be 
    then inserted into song and artist table
    
    Input:
    filepath: location of song file
    
    Output:
    tables: It will create song and artist dimension table along in postgres from song files.
    """
    # open song file
    song_temp=open(filepath,"r")
    song_data=pd.read_json(song_temp,lines=True)
    # insert song record & artist record
    for index,row in song_data.iterrows():
        cur.execute(song_table_insert, (row.song_id,row.title,row.artist_id,row.year,row.duration))
        cur.execute(artist_table_insert,(row.artist_id,row.artist_name,row.artist_location,row.artist_latitude,row.artist_longitude))
    song_temp.close()


def process_log_file(cur, filepath):
    """
    This function will open log files and read it into Dataframe which will be 
    then inserted into time, user and songplay table
    
    Input:
    filepath: location of log file
    
    Output:
    tables: It will create time and user dimension table along with songplay fact table 
            in postgres from log file.
    """
    # open log file
    log_temp = open(filepath,"r")
    log_data=pd.read_json(log_temp,lines=True)

    # filter by NextSong action and create date and time variables
    log_data['start_time']=pd.to_datetime(log_data['ts'],unit='ms')
    nextsong_row_indices = log_data[log_data['page']=='NextSong'].index
    df2=log_data.loc[nextsong_row_indices, :]
    df2['hour'] = (df2['start_time'].dt.hour)
    df2['day'] =  (df2['start_time'].dt.date)
    df2['week'] = (df2['start_time'].dt.week)
    df2['month'] = (df2['start_time'].dt.month)
    df2['year'] = (df2['start_time'].dt.year)
    df2['weekday'] = (df2['start_time'].dt.weekday)
    
    # insert time data records
    time_df = df2[['start_time','hour','day','week','month','year','weekday']]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df2[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, list(row))

    # insert songplay records
    for index, row in df2.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        # insert songplay record
        songplay_data = (index,row.start_time,row.userId,row.level,song_id,artist_id,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
    log_temp.close()


def process_data(cur, conn, filepath, func):
    """
    This function will put list of all json files with directory path
    into list. 
    
    Input:
    filepath: location of song file or log file
    func: call song or log data processinng function
    
    Output:
    all_files: list output with all files along with their path details
    num_files: total number of files for each i.e. song, log
    tables: by calling func function, it will create output tables in postgres
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    print(all_files)

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()