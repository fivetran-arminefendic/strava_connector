import requests
import logging
import json
import flask
import time
from calendar import timegm
import gsheet_data
# Disable warning for HTTPS requests being made without certificate verifiication
from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings
disable_warnings(InsecureRequestWarning)

def main(request):
    try:
        # --- Parse the request from Fivetran --- #
        request = request.get_json()
        refresh_tokens = gsheet_data.main(request)

        # --- Fivetran response varaibles --- #
        data = []
        state = {}
        page = 1
        is_paginated = []
        has_more = False

        # --- Grab the user info --- #
        users = get_user_info(request, refresh_tokens)

        # --- Handle a full resync where 'state': {} --- #
        if 'pagination' in request['state']:
            has_more = request['state']['pagination']['hasMore']

        # --- Check for pagination and make the appropriate calls to Strava --- #
        if has_more == True: 
            page = request['state']['pagination']['page']
            data = get_paginated_data(request, page)
            state = request['state']['current_state']
            is_paginated = check_for_pagination(data, request['state']['pagination']['is_pag'])
        
        else:
            data_and_cursors = get_user_data(request, refresh_tokens, page)
            data = data_and_cursors[0]
            cursors = data_and_cursors[1]
            state = build_the_state(refresh_tokens, cursors)
            is_paginated = check_for_pagination(data, refresh_tokens)
        
        # --- Prepare data for Fivetran --- #
        data = flatten_2d_list(data)

        # --- Set pagination variables --- #
        has_more = True if len(is_paginated) > 0 else False
        if len(is_paginated) > 0: page += 1

        # --- Assemble response for Fivetran --- #
        response = assemble_response(users, data, state, is_paginated, has_more, page)

        # --- Send to Fivetran --- #
        headers = {'Content-Type': 'application/json'}
        return flask.make_response(response,200,headers)

    except Exception as e:
        logging.error('Exception occurred', exc_info=True)

def get_user_info(request, refresh_tokens):
    users = []
    if len(request['state']) == 0:
        for token in refresh_tokens:
            user = user_info_api_call(request, token)
            users.append(user)
    else:
        for token in refresh_tokens:
            
            if token not in list(request['state']['current_state'].keys()):
                user = user_info_api_call(request,token)
                users.append(user)
    
    return users

def user_info_api_call(request, token):
    auth_url = 'https://www.strava.com/oauth/token' # base url to get a new access token using the refersh token
    athlete_url = 'https://www.strava.com/api/v3/athlete'
    payload = {
        'client_id': request['secrets']['client_id'],
        'client_secret': request['secrets']['client_secret'],
        'refresh_token': token,
        'grant_type': 'refresh_token',
        'f': 'json'
    }

    get_access_token = requests.post(auth_url, data=payload, verify=False)
    if get_access_token.status_code != 200:
        print(f'{get_access_token.status_code} Error: There was an issue with getting the access token - check the POST request')
        return

    access_token = get_access_token.json()['access_token']

    header = {'Authorization': 'Bearer ' + access_token}
    get_athlete_info = requests.get(athlete_url, headers=header)
    if get_athlete_info.status_code != 200:
        print(f'{get_athlete_info.status_code} Error: There was an issue with getting the username - check the GET request to the athlete endpoint')
        return
    
    user = get_athlete_info.json()

    return user

def get_user_data(request, refresh_tokens, page):
    data = []
    for token in refresh_tokens:
        after = None
        if len(request['state']) != 0:
            after = request['state']['current_state'].get(token) 
        
        user_data = user_data_api_call(request, token, page, after)
        data.append(user_data)

    cursors = build_the_cursors(request, user_data, refresh_tokens)

    return data, cursors

def get_paginated_data(request, page):
    data = []
    for token in request['state']['pagination']['is_pag']:
        user_data = user_data_api_call(request, token, page)
        data.append(user_data)

    return data

def user_data_api_call(request, token, page = 1, after = None, events_per_page = 200):
    auth_url = 'https://www.strava.com/oauth/token' # base url to get a new access token using the refersh token
    activites_url = 'https://www.strava.com/api/v3/athlete/activities'
    payload = {
        'client_id': request['secrets']['client_id'],
        'client_secret': request['secrets']['client_secret'],
        'refresh_token': token,
        'grant_type': 'refresh_token',
        'f': 'json'
    }

    get_access_token = requests.post(auth_url, data=payload, verify=False)
    if get_access_token.status_code != 200:
        print(f'{get_access_token.status_code} Error: There was an issue with getting the access token - check the POST request')
        return

    access_token = get_access_token.json()['access_token']

    header = {'Authorization': 'Bearer ' + access_token}
    param = {'after': after, 'per_page': events_per_page, 'page': page}
    get_user_data = requests.get(activites_url, headers=header, params=param)
    if get_user_data.status_code != 200:
        print(f'{get_user_data.status_code} Error: There was an issue with getting the user data - check the GET request to the activities endpoint')
        return

    user_data = get_user_data.json()
    
    return user_data

def check_for_pagination(data, refresh_tokens): # pass in request as well
    is_paginated = []
    for i in range(0, len(data)):
        if len(data[i]) == 1:
            is_paginated.append(refresh_tokens[i])
    
    return is_paginated

def build_the_cursors(request,user_data, refresh_tokens):
    cursors = []
    for token in refresh_tokens:
        # --- Handling of a user with no data during historical/initial sync -- #
        if len(request['state']) == 0 and len(user_data) == 0:
            continue

        # --- Historical sync for the user(s) - api returns latest activity first, DESC --- #
        # Initial/historical sync OR detect that a new user is added
        elif len(request['state']) == 0 or request['state']['current_state'].get(token) == None:
            # Account for a new user not having any data
            if len(user_data) == 0:
                continue
            user_cursor = time.strptime(user_data[0]['start_date'], '%Y-%m-%dT%H:%M:%SZ')
            epoch_user_cursor = timegm(user_cursor)
            cursors.append(epoch_user_cursor)

        # --- User has no new activities - retain the existing cursor --- #
        elif len(user_data) == 0:
            cursors.append(request['state']['current_state'][token])

        # --- Incremental sync - api returns latest activity last, ASC --- #
        else:
            user_cursor = time.strptime(user_data[-1]['start_date'], '%Y-%m-%dT%H:%M:%SZ')
            epoch_user_cursor = timegm(user_cursor)
            cursors.append(epoch_user_cursor)

    return cursors

def build_the_state(refresh_tokens, cursors):
    state = {}
    # Handle if cursors is empty
    try:
        for key, value in zip(refresh_tokens, cursors):
            state[key] = value
    except TypeError:
        pass
    
    return state

def flatten_2d_list(list_2d):
    flat_list = []
    for element in list_2d:
        if type(element) is list:
            for item in element:
                flat_list.append(item)
        else:
            flat_list.append(element)

    return flat_list

def assemble_response(users, data, state, is_paginated, has_more, page):
    response_dict = {
        'state': {
            'current_state': state,
            'pagination': {'is_pag': is_paginated, 'page': page, 'hasMore': has_more}
        },
        'schema': {
            'users_raw': {
                'primary_key': ['id']
            },
            'activities_raw': {
                'primary_key': ['id']
            }
        },
        'insert': {
            'users_raw': users,
            'activities_raw': data
        },
        'hasMore': has_more
    }

    return json.dumps(response_dict)