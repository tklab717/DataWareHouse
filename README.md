Purpose of this database for Sparkify and their analytical goals
=================================================================
Sparkify has grown their user base.
Therefore, they wnat to move their processes and data onto the cloud database which have scalability and flexibility.
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
I will load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

Their analytical goals are to get insights in what songs their users are listening to.
Therefore I create the database which offers analytical tables to ralize it.

Database schema design 
========================
This database in the redshift is organized by star schema which have good consistency. Fact table is songplays. Dimention tables are users, songs, artists, time.
The ETL pipeline makes some table in this database by using songs_data and logs_data in the S3. Their data are extracted from S3 and copy to staging table in the redshift. The staging tables are finally transformed to fact table and dimention tables.  
Following are table designs.

- Fact Table
 1.songplays  
     songplay_id integer,  
     start_time TIMESTAMP,  
     user_id integer,  
     level varchar,  
     song_id varchar,  
     artist_id varchar,  
     session_id integer,  
     location varchar,  
     user_agent varchar  
     
- Demension Table
 2.users  
     user_id integer,  
     first_name varchar,  
     last_name varchar,  
     gender varchar,  
     level varchar  
 3.songs  
     song_id varchar,  
     title varchar,  
     artist_id varchar,  
     year integer,  
     duration float8  
 4.artists  
     artist_id varchar,  
     name varchar,  
     location varchar,  
     lattitude float8,  
     longitude float8  
 5.time  
     start_time TIMESTAMP,  
     hour integer,  
     day integer,  
     week integer,  
     month integer,  
     year integer  
     
- Staging Table
 6.staging_songs  
     staging_songs_id integer IDENTITY(0,1) PRIMARY KEY NOT NULL,  
     artist_id varchar,  
     artist_latitude float8,  
     artist_location varchar,  
     artist_longitude float8,  
     artist_name varchar,  
     duration FLOAT8,  
     num_songs FLOAT8,  
     song_id varchar,  
     title varchar,  
     year FLOAT8  
 7.staging_events  
     staging_events_id" integer IDENTITY(0,1) PRIMARY KEY NOT NULL,  
     artist" varchar,  
     auth" varchar,  
     firstName varchar,  
     gender character,  
     itemInSession integer,  
     lastName varchar,  
     length float8,  
     level varchar,  
     location varchar,  
     method varchar,  
     page varchar,  
     registration float8,  
     sessionId integer,  
     song varchar,  
     status integer,  
     ts BIGINT,  
     userAgent varchar,  
     userId integer  

Provide example queries and results for song play analysis.
===========================================================
+ How many people downloaded songs?
    + Query
        SET search_path TO {};
        SELECT t2.year, t2.month, COUNT(DISTINCT t1.user_id) FROM songplays AS t1
        LEFT JOIN time AS t2
        ON t1.start_time = t2.start_time
        GROUP BY year, month
    + Answer
        96
+ Does some heavy user exist?
    + Query
        SELECT user_id, COUNT(user_id) AS count FROM songplays
        GROUP BY user_id
        ORDER BY count DESC LIMIT 5
    + Answer
        user_id 49 is heavy user who downloaded songs which about 10% in all.
        
+ What is many downloaded song?
    + Query
        SELECT t1.song_id, t2.title, t3.name, COUNT(t1.song_id) AS count FROM songplays AS t1
        LEFT JOIN songs AS t2
        ON t1.song_id = t2.song_id
        LEFT JOIN artists AS t3
        ON t2.artist_id = t3.artist_id
        GROUP BY t1.song_id, t2.title, t3.name
        ORDER BY count DESC LIMIT 5
    + Answer
        The most downloaded song is You're The On. It was downloaded 37.


How to use this programs
========================
1.run IaC.ipynb to create redshift cluster and IAM role which Redshift uses for accessing S3.
2.write ARN and HOST(ENDPOINT of redshift)
2.run create_tables.py on the terminal
3.run etl.py on the terminal
4.You can analyze by using query(You can see the example on the Sample_queries.ipynb)
5.run redshift cluster delete command

About files in repositry
=========================
1.IaC.ipynb
Code for creating redshift cluster and IAM role.
You can get ENDPOINT of the redshift cluster and IAM role.

2.sql_queries.py
Code for sql which include creating, droping, copy to redshift from S3, insert into redshift for tables.

3.create_tables.py
Code for creating new schema and tables which don't include values.

4.etl.py
Code for copying S3 data to staging tables in the redshift and inserting analytical tables from staging tables.

5.Sample_queries.ipynb
Jupyter notebook for the contents of tables which are created

6.dwh.cfh
Information for using AWS