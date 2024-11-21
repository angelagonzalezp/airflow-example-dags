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

Our PostgreSQL table must be created before triggering the DAG for the first time.[^1]
We must pass a `dag_run.conf` in the following format: {"genre": "pop", "limit": "25"}

[^1] Create script can be found at [utils](utils) directory.

