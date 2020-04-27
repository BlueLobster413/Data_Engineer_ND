import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
song_path = config.get('S3', 'SONG_DATA')
log_path = config.get('S3', 'LOG_DATA')
my_iam_role = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS "staging_events" (
                                    artist         VARCHAR(50),
                                    auth           VARCHAR(10),
                                    firstName      VARCHAR(20),
                                    gender         VARCHAR(5),
                                    itemInSession  INTEGER,
                                    lastName       VARCHAR(20),
                                    length         FLOAT,
                                    level          VARCHAR(5),
                                    location       VARCHAR(100),
                                    method         VARCHAR(5),
                                    page           VARCHAR(10),
                                    registration   BIGINT,
                                    sessionID      INTEGER,
                                    song           VARCHAR(20),
                                    status         INTEGER,
                                    ts             TIMESTAMP,
                                    userAgent      VARCHAR(50),
                                    userId         INTEGER)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS "staging_songs" (
                                    num_songs        INTEGER,
                                    artist_id        VARCHAR(20),
                                    artist_latitude  FLOAT,
                                    artist_longitude FLOAT,
                                    artist_location  VARCHAR(100),
                                    artist_name      VARCHAR(50),
                                    song_id          VARCHAR(20),
                                    title            VARCHAR(20),
                                    duration         FLOAT,
                                    year             INTEGER)
""")

# DISTKEY groups the columns having same number to 1 CPU.
# SORTKEY sorts the slice as per the mentioned column before placing each slice on CPU.

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS "songplay" (
                                songplay_id      INTEGER IDENTITY(0,1)  NOT NULL,
                                start_time       TIMESTAMP      NOT NULL,
                                user_id          INTEGER        NOT NULL,
                                level            VARCHAR(5)     NOT NULL,
                                song_id          VARCHAR(20)    NOT NULL,
                                artist_id        VARCHAR(20)    NOT NULL,
                                session_id       INTEGER        NOT NULL,
                                location         VARCHAR(100),
                                user_agent       VARCHAR(100)   ) SORTKEY("start_time") """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS "users" (
                                user_id     INTEGER PRIMARY KEY,
                                first_name  VARCHAR(20) NOT NULL,
                                last_name   VARCHAR(20) NOT NULL,
                                gender      VARCHAR(5)  NOT NULL,
                                level       VARCHAR(5)  NOT NULL) SORTKEY("user_id") """) 

song_table_create = ("""CREATE TABLE IF NOT EXISTS "song" (
                                song_id     VARCHAR(20) PRIMARY KEY,
                                title       VARCHAR(20) NOT NULL,
                                artist_id   VARCHAR(20) NOT NULL,
                                year        INTEGER,
                                duration    FLOAT) SORTKEY("song_id")    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS "artist" (
                                artist_id    VARCHAR(20)  PRIMARY KEY,
                                name         VARCHAR(50)  NOT NULL,
                                location     VARCHAR(100),
                                lattitude    FLOAT,
                                longitude    FLOAT) SORTKEY("artist_id") """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS "time"(
                                start_time TIMESTAMP PRIMARY KEY,
                                hour       INTEGER,
                                day        INTEGER,
                                week       INTEGER,
                                month      INTEGER,
                                year       INTEGER,
                                weekday    INTEGER) SORTKEY("start_time") """)



# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM log_path
                            credentials = 'aws_iam_role=my_iam_role'
                            gzip region = 'us-west-2' """)

staging_songs_copy = ("""COPY staging_songs FROM song_path
                            credentials = 'aws_iam_role=my_iam_role'
                            gzip region = 'us-west-2' """)

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
                                            SELECT se.ts, se.userId, se.level, ss.song_id, ss.artist_id, se.sessionID, se.location, se.userAgent
                                            FROM staging_events se, staging_songs ss
                                            WHERE se.artist = ss.artist_name
                                            AND   se.song = ss.title """)

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level) \
                                    SELECT userId, firstName, lastName, gender, level
                                    FROM staging_events """)

song_table_insert = (""" INSERT INTO song (song_id, title, artist_id, year, duration)
                                    SELECT song_id, title, artist_id, year, duration
                                    FROM staging_songs """)

artist_table_insert = (""" INSERT INTO artist (artist_id, name, location, latitude, longitude)
                                        SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                                        FROM staging_songs """)

time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                                        SELECT ts AS start_time
                                        EXTRACT(hour from ts) as hour
                                        EXTRACT(day from ts)  as day
                                        EXTRACT(week from ts) as week
                                        EXTRACT(month from ts) as month
                                        EXTRACT(year from ts) as year
                                        EXTRACT(weekday from ts) as weekday
                                        FROM staging_events """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
