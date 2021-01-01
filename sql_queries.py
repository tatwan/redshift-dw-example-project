import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP SCHEMA
staging_schema_drop = "DROP SCHEMA IF EXISTS staging_area CASCADE;"
sparkify_schema_drop = "DROP SCHEMA IF EXISTS sparkify CASCADE;"

# CREATE SCHEMA
staging_schema_create = "CREATE SCHEMA IF NOT EXISTS staging_area;"
sparkify_schema_create = "CREATE SCHEMA IF NOT EXISTS sparkify;"

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_area.staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_area.staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS sparkify.fact_songplays;"
user_table_drop = "DROP TABLE IF EXISTS sparkify.dim_users; "
song_table_drop = "DROP TABLE IF EXISTS sparkify.dim_songs;"
artist_table_drop = "DROP TABLE IF EXISTS sparkify.dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS sparkify.dim_time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_area.staging_events_table
(
    artist VARCHAR(250),
    auth VARCHAR(10),
    firstName VARCHAR(50),
    gender VARCHAR(1),
    itemInSession SMALLINT,
    lastName VARCHAR(50),
    length DECIMAL(9,5),
    level VARCHAR(4),
    location VARCHAR(50),
    method VARCHAR(3),
    page VARCHAR(16),
    registration DOUBLE PRECISION,
    sessionId SMALLINT,
    song VARCHAR(250),
    status SMALLINT,
    ts BIGINT, 
    userAgent VARCHAR(150),
    userId VARCHAR(3)
);

""")

staging_songs_table_create = ("""
CREATE TABLE staging_area.staging_songs_table
(
    artist_id VARCHAR(250),
    artist_latitude DECIMAL(9,6),
    artist_location VARCHAR(250),
    artist_longitude DECIMAL(9,6),
    artist_name VARCHAR(250),
    duration DECIMAL(9,5),
    num_songs SMALLINT,
    song_id VARCHAR(20),
    title VARCHAR(250),
    year SMALLINT
);
""")

songplay_table_create = ("""
CREATE TABLE sparkify.fact_songplays (
    songplay_id bigint IDENTITY(1,1),
    start_time timestamp NOT NULL,
    user_id smallint NOT NULL,
    level varchar(4),
    song_id varchar(20),
    artist_id varchar(250),
    session_id SMALLINT NOT NULL,
    location varchar(50),
    user_agent varchar(150),
    PRIMARY KEY(songplay_id),
    FOREIGN KEY(user_id) references sparkify.dim_users(user_id),
    FOREIGN KEY(song_id) references sparkify.dim_songs(song_id),
    FOREIGN KEY(artist_id) references sparkify.dim_artist(artist_id),
    FOREIGN KEY(start_time) references sparkify.dim_time(start_time)
)
DISTSTYLE KEY
DISTKEY(start_time)
SORTKEY(start_time, user_id, song_id, artist_id)
;
""")

user_table_create = ("""
CREATE TABLE sparkify.dim_users (
    user_id smallint,
    first_name varchar(50) NOT NULL,
    last_name varchar(50) NOT NULL,
    gender varchar(1),
    level varchar(4) NOT NULL,
    PRIMARY KEY(user_id)
)
DISTSTYLE all
SORTKEY(user_id);
""")

song_table_create = ("""
CREATE TABLE sparkify.dim_songs (
    song_id varchar(20),
    title varchar(250) NOT NULL,
    artist_id varchar(250)  NOT NULL,
    year smallint,
    duration DECIMAL(9,5),
    PRIMARY KEY(song_id),
    FOREIGN KEY(artist_id) references sparkify.dim_artist(artist_id)
)
DISTSTYLE KEY
DISTKEY(song_id)
SORTKEY(song_id, artist_id)
;
""")

artist_table_create = ("""
CREATE TABLE sparkify.dim_artist (
    artist_id varchar(250),
    name varchar(250) NOT NULL,
    location varchar(250), 
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    PRIMARY KEY(artist_id)
)
DISTSTYLE KEY
DISTKEY(artist_id)
SORTKEY(artist_id);
""")

time_table_create = ("""
CREATE TABLE sparkify.dim_time(
    start_time timestamp,
    hour smallint,
    day smallint,
    week smallint,
    month smallint, 
    year smallint, 
    weekday smallint,
    PRIMARY KEY(start_time)
)
DISTSTYLE KEY
DISTKEY(start_time)
SORTKEY(start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_area.staging_events_table 
from '{}'
credentials 'aws_iam_role={}'
region '{}'
format as json 'auto ignorecase';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('CLUSTER', 'REGION'))

staging_songs_copy = ("""
copy staging_area.staging_songs_table 
from '{}'
credentials 'aws_iam_role={}'
region '{}'
format as json 'auto ignorecase';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('CLUSTER', 'REGION'))

# FINAL TABLES

songplay_table_insert = ("""
insert into sparkify.fact_songplays
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select distinct
timestamp 'epoch' + sev.ts/1000 * interval '1 second' AS start_time,
cast(sev.userid as smallint) as id,
level,
son.song_id ,
son.artist_id ,
sev.sessionid,
sev.location,
sev.useragent
from staging_area.staging_events_table sev
left join staging_area.staging_songs_table son 
on sev.artist = son.artist_name 
	and sev.length = son.duration
	and sev.song = son.title
where sev.userid != '' 
	and sev.page = 'NextSong';
""")

user_table_insert = ("""
insert
	into
	sparkify.dim_users
SELECT distinct
	cast(f.userid as smallint) as id,
	f.firstname,
	f.lastname,
	f.gender,
	f."level"
from
	(
	SELECT
		se.userid,
		se.firstname ,
		se.lastname ,
		se.gender,
		se."level",
		se.ts
	from
		staging_area.staging_events_table se
	join(
		SELECT
			userid,
			max(ts) as mts
		from
			staging_area.staging_events_table
		where
			userid != ''
		group by
			1 ) level_latest on
		se.userid = level_latest.userid
		and se.ts = level_latest.mts) f
""")

song_table_insert = ("""
insert into sparkify.dim_songs 
select distinct
song_id,
title,
artist_id,
year,
duration 
from staging_area.staging_songs_table;
""")

artist_table_insert = ("""
insert into sparkify.dim_artist 
select distinct
artist_id ,
artist_name ,
artist_location ,
artist_latitude ,
artist_longitude 
from staging_area.staging_songs_table ;
""")

time_table_insert = ("""
insert into sparkify.dim_time
select distinct
timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
EXTRACT(hour from start_time) as "hour",
EXTRACT(day from start_time) as "day",
EXTRACT(week from start_time) as "week",
EXTRACT(month from start_time) as "month",
EXTRACT(year from start_time) as "year",
EXTRACT(weekday from start_time) as "weekday"
from staging_area.staging_events_table where page='NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, time_table_create, song_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
drop_schemas = [staging_schema_drop, sparkify_schema_drop]
create_schemas = [staging_schema_create, sparkify_schema_create]
