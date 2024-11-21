"""
    - This DAG must be triggered by specifying a JSON including the desired music genre and the number of songs
    to be fetched: {"genre":"pop","limit":"50"}
    - We have to define two variables: spotify_client_secret and spotify_client_id_secret with our Spotify credentials.
"""

import spotipy
import logging
import pandas as pd
import time
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime


from airflow.models import DAG
from airflow.utils import dates
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    "owner":"angela",
    "start_date": dates.days_ago(1),
    "client_id": "{{ var.value.spotify_client_id_secret }}",
    "client_secret": "{{ var.value.spotify_client_secret }}",
    "genre": "{{ dag_run.conf['genre'] }}",
    "limit": "{{ dag_run.conf['limit']}}"
}

def get_top_songs(**context):
    client_id = context["templates_dict"]["client_id"]
    client_secret = context["templates_dict"]["client_secret"]
    genre = context["templates_dict"]["genre"]
    limit = context["templates_dict"]["limit"]
    try:
        credentials_manager = SpotifyClientCredentials(
            client_id=client_id,
            client_secret=client_secret)
        spotify = spotipy.Spotify(auth_manager=credentials_manager)
    except Exception as error:
        raise Exception(error)
    try:
        results = spotify.search(q=f"genre:{genre}", type="track", limit=limit)
        logging.info(f"Process songs fetched for genre {genre}")
    except Exception as error:
        raise Exception(error)
    if(len(results)>0):
        tracks = results["tracks"]["items"]
        song_features = []
        for track in tracks:
            features = spotify.audio_features(track["id"])[0]
            features["song"] = track["name"]
            track_artist = ""
            for artist in track["artists"]:
                if(track_artist==""):
                    track_artist  = artist["name"]
                else:
                    track_artist += f",{artist['name']}"
            features["artist"] = track_artist
            features["explicit"] = track["explicit"]
            song_features.append(features)
        df = pd.DataFrame(song_features)
        df["artist"] = df["artist"].str.normalize("NFKD").str.encode("ascii", errors="ignore").str.decode("utf-8").str.lower()
        df["song"] = df["song"].str.normalize("NFKD").str.encode("ascii", errors="ignore").str.decode("utf-8").str.lower()
        df["date"] = datetime.now()
        df["genre"] = genre
    else:
        raise Exception("No results have been obtained. Check that the specified gender is correct")
    epoch = int(time.time())
    csv_name = f"./dags/outputs/{genre}_top_{limit}_{str(epoch)}.csv"
    df.to_csv(csv_name, sep=";", index=False)
    task_instance = context["task_instance"]
    task_instance.xcom_push(key="csv_file", value=csv_name)
    
def upload_csv_to_postgres(**context):
    conn = PostgresHook(postgres_conn_id="dummy_postgres")
    ti = context["task_instance"]
    csv_file = ti.xcom_pull(task_ids="top_songs_to_csv", key="csv_file")
    try:
        sql = """COPY public.top_songs_spotify FROM stdin WITH CSV HEADER DELIMITER as ';' """
        conn.copy_expert(sql, csv_file)
        logging.info("CSV uploaded to top_songs_spotify table.")
    except Exception as error:
        raise Exception(error)


with DAG(dag_id="spotify_top_by_genre", default_args=default_args, schedule_interval="@weekly") as dag:
    top_songs_to_csv = PythonOperator(task_id="top_songs_to_csv", python_callable=get_top_songs, 
                                    templates_dict=default_args, do_xcom_push=True, provide_context=True)
    top_songs_to_postgres = PythonOperator(task_id="top_song_to_postgres", python_callable=upload_csv_to_postgres,
                                        provide_context=True)
    
    dag.doc_md = __doc__
    
top_songs_to_csv >> top_songs_to_postgres
