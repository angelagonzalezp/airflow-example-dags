# airflow-example-dags

This scripts must be located in the dags volume for Airflow to be able to import them.

## [spotify-top-by-genre](https://github.com/angelagonzalezp/airflow-example-dags/blob/main/spotify-top-by-genre.py)

### Tasks

* `top_songs_to_csv`:  makes a request to the Spotify API to retrieve the most popular songs for a specific genre and saves the result to a CSV file.
* `top_songs_to_postgres`: uploads the aforementioned CSV to a table in a PostgreSQL DB.

### Required Variables and Connections

* Spotify credentials: spotify_client_id_secret and spotify_client_secret
* PostgreSQL connection

### How to trigger the dag?

Our PostgreSQL table must be created before triggering the DAG for the first time[^1].
We can pass a `dag_run.conf` in the following format: {"genre": "pop", "limit": "25"}. Default values are {"genre":"reggaeton","limit":"50"}

[^1]: Create script can be found at [utils](utils) directory.

## [nifi-pg-monitoring](https://github.com/angelagonzalezp/airflow-example-dags/blob/main/nifi-pg-monitoring.py)

### Tasks

* `get_pg_stats`: makes a request to Nifi API to monitor a Process Group. and stores data to temporary JSON file.
* `upload_to_mongo`: inserts JSON to MongoDB collection[^2].
* `remove_json_file`: bash command to remove temp JSON file.

[^2]: Apache Airflow 2.8.0 and higher versions include MongoHook.

### Required Variables and Connections

* Apache Nifi credentials: {"user":"nifi_user","pass":"nifi_password"}, nifi_url
* Nifi Process Group ID
* MongoDB connection