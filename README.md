# Overview:

Sparkify is a music streaming app used by users to listen songs. Sparkify is collecting data about user activity and songs through their app. Now they want to use this collected data to create a data warehouse which will help them analyse songs and user activity and in turn provide better user experience.

# Source Files:

1) Song Files - which holds information about song metadata along with artist details
2) Log Files - which holds details about user activity 

# Requirements:

Use source files to create a star schema datawarehouse for Sparkify.
Details about source files and final star schema data model is shown in separate tab in Postgres Data Modelling Data Model.xlsx

# Specifications:

- Create Database
- Read Song files and create Songs and Artists dimension tables from it.
- Read log files, Derive time related fields and create Users and Time dimension tables along with songplay fact table

# Project Details:
Following is the script order to run this project:

1) sql_queries.py - This script includes queries as variables for drop,create and insert tables. No need to run this.
2) create_tables.py - This script creates and calls functions for creating database, droping tables and creating tables. This script will be called from etl.ipynb and etl.py
3) etl.ipynb - This script is for testing purpose which only process single file for song and log. After successfully running this script, can see test results using test.ipynb script.
4) etl.py - This is the main script which is similar to etl.ipynb with minor changes so that it can process all the song and log files for creating song and log tables.


# Following Analysis queries can be run with designed datawarehouse to answer key questions about songs and artists

## Most Listened Songs

- select sp.song_id,s.title,count(*) as most_listened_songs from songplays as sp left join songs as s on sp.song_id=s.song_id group by 1,2 order by 3 desc

## Most Listened Artists

- select sp.artist_id,a.artist_name,count(*) as most_famous_artist from songplays as sp left join artists as a on sp.artist_id=a.artist_id group by 1,2 order by 3 desc

## Most famous songs by year (this queries will need row_number function to get max value to filter)

- select t.year,s.title,count(*) as most_famour_song_by_year from songplays as sp left join songs as s on sp.song_id=s.song_id left join time as t on sp.start_time=t.start_time group by 1,2 order by 3 desc


Similar queries can be written to get:

1) Most famous artist by year/month/week etc.

2) Most popular song by users

3) Most popular song by artist

