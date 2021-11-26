import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS factSongplays;"
user_table_drop = "DROP TABLE IF EXISTS dimUsers;"
song_table_drop = "DROP TABLE IF EXISTS dimSongs;"
artist_table_drop = "DROP TABLE IF EXISTS dimArtists;"
time_table_drop = "DROP TABLE IF EXISTS dimTime;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events 
    (
        artist        VARCHAR(250),
        auth          VARCHAR(250),
        firstName     VARCHAR(250),
        gender        VARCHAR(1),
        itemInSession INTEGER,
        lastName      VARCHAR(250),
        length        DOUBLE PRECISION,
        level         VARCHAR(250),
        location      VARCHAR(250),
        method        VARCHAR(250),
        page          VARCHAR(250),
        registration  DOUBLE PRECISION,
        sessionid     INTEGER,
        song          VARCHAR(250),
        status        INTEGER,
        ts            BIGINT,
        userAgent     VARCHAR(250),
        userId        INTEGER
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs        INTEGER,
        artist_id        VARCHAR(250),
        artist_latitude  VARCHAR(250),
        artist_longitude VARCHAR(250),
        artist_location  VARCHAR(250),
        artist_name      VARCHAR(250),
        song_id          VARCHAR(250),
        title            VARCHAR(250),
        duration         DOUBLE PRECISION,
        year             INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS factSongplays
    (
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY ,
        start_time  TIMESTAMP NOT NULL,
        user_id     INTEGER NOT NULL,
        level       VARCHAR(10) NOT NULL,
        song_id     VARCHAR(250) NOT NULL,
        artist_id   VARCHAR(250) NOT NULL,
        session_id  INTEGER NOT NULL,
        location    VARCHAR(250),
        user_agent  VARCHAR(250)
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimUsers
    (
        user_key   INTEGER IDENTITY(0,1),
        user_id    INTEGER PRIMARY KEY,
        first_name VARCHAR(40),
        last_name  VARCHAR(40),
        gender     VARCHAR(1),
        level      VARCHAR(10)
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimSongs
    (
        song_key  INTEGER IDENTITY(0,1),
        song_id   VARCHAR(250) PRIMARY KEY,
        title     VARCHAR(250) NOT NULL,
        artist_id VARCHAR(250) NOT NULL,
        year      INTEGER NOT NULL,
        duration  DOUBLE PRECISION
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimArtists
    (
        artist_id  VARCHAR(250) PRIMARY KEY,
        name       VARCHAR(250),
        location   VARCHAR(250),
        lattitude  VARCHAR(250),
        longitude  VARCHAR(250)
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimTime
    (
        time_start TIMESTAMP PRIMARY KEY,
        hour     INTEGER NOT NULL,
        day      INTEGER NOT NULL,
        month    INTEGER NOT NULL,
        year     INTEGER NOT NULL,
        weekday  INTEGER NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    iam_role '{}'
    json {}
    region 'us-west-2'  
    timeformat as 'epochmillisecs';
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json 'auto'
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO factSongplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1second') as start_time,
        se.userId as user_id,
        se.level,
        ss.song_id,
        ss.artist_Id as artist_id,
        se.sessionId as session_id,
        se.location,
        se.userAgent as user_agent
    FROM
        staging_events se
    JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name )
    WHERE
        page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO  dimUsers (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userid as user_id,
        firstName as first_name,
        lastName as last_name,
        gender,
        level
    FROM
        staging_events se1
    WHERE userId IS NOT NULL
        AND ts = (SELECT 
                      MAX(ts) 
                  FROM 
                      staging_events se2
                  WHERE
                      se1.userId = se2.userId)
    ORDER BY se1.userId
    
""")

song_table_insert = ("""
    INSERT INTO dimSongs (song_id, title, artist_id, year, duration)
    SELECT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM
        staging_songs
""")

artist_table_insert = ("""
    INSERT INTO dimArtists (artist_id , name, location, lattitude, longitude)
    SELECT DISTINCT
        artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as lattitude,
        artist_longitude as longitude
    FROM
        staging_songs
""")

time_table_insert = ("""
    INSERT INTO dimTime (time_start, hour, day, month, year, weekday)
    SELECT DISTINCT
        start_time,
        EXTRACT(HOUR FROM start_time) as hour,
        EXTRACT(DAY FROM start_time) as day,
        EXTRACT(MONTH FROM start_time) as month,
        EXTRACT(YEAR FROM start_time) as year,
        EXTRACT(DOW FROM start_time) as weekday
    FROM
        (SELECT distinct TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1second' as start_time FROM staging_events)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
