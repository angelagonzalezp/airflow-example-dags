import requests

def get_token(api_url, username, password):
    url = f"{api_url}"
    data = {'username': username, 'password': password}
    response = requests.post(url=url, data=data, verify=False)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)
    return response.text

def get_process_group_stats(api_url, auth_token, pg_id):
    url = f"{api_url}process-groups/{pg_id}"
    HEADERS = {'Authorization': f"Bearer {auth_token}"}
    response = requests.get(url=url, headers=HEADERS, verify=False)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)
    return response.text