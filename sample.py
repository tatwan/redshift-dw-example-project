import configparser
import psycopg2


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    query= """
    SELECT u.first_name || ' ' || u.last_name as user, s.title as song_title, t.weekday, t.year, t.hour, f.level, s.duration
        FROM sparkify.fact_songplays f
        JOIN sparkify.dim_users u
            on f.user_id = u.user_id
        JOIN sparkify.dim_songs s
            on f.song_id = s.song_id
        JOIN sparkify.dim_time t
            on f.start_time = t.start_time
        LIMIT 20
    """
    print('First 20 records ....')
    results = cur.execute(query)
    results = cur.execute(query)
    for r in cur.fetchall():
        print(r)
    conn.close()


if __name__ == "__main__":
    main()