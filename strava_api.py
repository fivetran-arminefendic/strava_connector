from pprint import pprint
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import datetime
import logging
import json
import flask
from refresh_tokens import refresh_tokens
import time
from calendar import timegm

def main(request):
    try:
        # Parse the request from Fivetran
        # request = request.get_json()

        # Request data from Strava
        all_data = get_data(request)
        data = flatten_data(all_data[0]) # flatten_data returns a single list that Fivetran can accept
        state = all_data[1]
        is_paginated = all_data[2]
        has_more = all_data[3]

        # Assemble response for Fivetran
        response = assemble_response(data, state, is_paginated, has_more)
        print(response)
        print(' --------------- ')
        print(response['insert']['strava_data'][0][0]['name'])

        # Send to Fivetran
        headers = {'Content-Type': 'application/json'}
        #return flask.make_response(response,200,headers)

    except Exception as e:
        logging.error('Exception occurred', exc_info=True)

def get_data(request):
    data = []
    cursors = []
    state = {}
    # NEW CODE
    page = 1
    events_per_page = 1
    is_paginated = {}
    has_more = False

    # --- Handling of paginated data --- #
    if request['hasMore'] == True: 
        for token in request['secrets']['is_pag']:
            # *! Page is important here so pass it to make_api_call!
            page = request['secrets']['is_pag'].get(token)
            user_data = make_api_call(request, token, page, events_per_page)
            
            # NEW CODE
            if len(user_data) == events_per_page:
                is_paginated[token] = page + 1
                has_more = True
            data.append(user_data)
            
        # Since the cursor does not update with paginated data - keep the same cursors
        state = request['state']
    
    else:
        for token in refresh_tokens:
            # *! After is important here due to incremental syncs so pass it to make_api_call!
            after = request['state'].get(token)
            user_data = make_api_call(request, token, page, events_per_page, after)

            if len(user_data) == events_per_page:
                is_paginated[token] = 2 # move to the next page
                has_more = True
            data.append(user_data)

            cursors = build_the_cursors(request,user_data,token,cursors)
    
        state = build_the_state(refresh_tokens, cursors)
                
    return data, state, is_paginated, has_more

def make_api_call(request, token, page = 1, events_per_page = 200, after = None):
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
    # pro tip: Holding your left
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

    # pro tip: Happy path comes last
    user_data = get_user_data.json()
    return user_data

def build_the_cursors(request,user_data,token,cursors):
    # --- User has no new activities - retain the existing cursor --- #
    if len(user_data) == 0:
        cursors.append(request['state'][token])

    # --- Historical sync for the user - api returns latest activity first --- #
    elif request['state'].get(token) == None:
        user_cursor = time.strptime(user_data[0]['start_date'], '%Y-%m-%dT%H:%M:%SZ')
        epoch_user_cursor = timegm(user_cursor)
        cursors.append(epoch_user_cursor)

    # --- Incremental sync - api returns latest activity last --- #
    else:
        user_cursor = time.strptime(user_data[-1]['start_date'], '%Y-%m-%dT%H:%M:%SZ')
        epoch_user_cursor = timegm(user_cursor)
        cursors.append(epoch_user_cursor)

    return cursors

def build_the_state(refresh_tokens, cursors):
    state={}
    for key, value in zip(refresh_tokens, cursors):
        state[key] = value
    return state

def flatten_data(data):
    # Each users data is in a list, merge those lists in to one large list
    # LB
    if len(data) == 1:
        return data
    else:
        temp = data[0] + data[1]
        i = 2
        while i < len(data):
            temp = temp + data[i]
            i += 1
    return temp

# LB
# Function definition cleaner takes precendence over main/upper scope cleaner
# def assemble_response(data, state, is_assembled): 
def assemble_response(data, state, is_paginated, has_more):
    response_dict = {
        'secrets': {
            'is_pag': is_paginated
        },
        'state': state,
        'schema': {
            'strava_data': {
                'primary_key': ['id']
            }
        },
        'insert': {
            'strava_data': data
        },
        'hasMore': has_more
    }
    return response_dict
    # *! Make sure to return a json here
    #return json.dumps(response_dict)

if __name__ == '__main__':
    main(
        {
            'state': {
                '5dc2bef4b13f135c5ddab9563e74742eec1fa3df': 1658880473
            },
            'secrets': {
                'client_id': '91382',
                'client_secret': '20a49adb714cc2314bbdf5f1514a6b86cd4838eb',
                'is_pag': {
                    '5dc2bef4b13f135c5ddab9563e74742eec1fa3df': 2
                }
            },
            'hasMore': True
        })


# --- New State using epoch --- #
# {
#     '1e415ed72533c20dc67bc32d080a2f48cb120079': 1660431992,
#     '39f6445fe8cd4c6b543ee0db70d3fa2dfd1fa289': 1660600401,
#     '5dc2bef4b13f135c5ddab9563e74742eec1fa3df': 1660266675
# }