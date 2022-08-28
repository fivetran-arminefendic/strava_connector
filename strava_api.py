from pprint import pprint
import requests
import urllib3
#urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import logging
import json
import flask
import time
from calendar import timegm
import gsheet_data

# global var for testing pagination
events_per_page = 1

def main(request):
    try:
        # Parse the request from Fivetran
        # request = request.get_json()

        refresh_tokens = gsheet_data.main(request)

        # Fivetran response varaibles
        data = []
        state = {}
        page = 1
        is_paginated = []
        has_more = False

        # Check for pagination and make the appropriate calls to Strava
        if request['hasMore'] == True: 
            # *! Page is important here so pass it thru to make_api_call!
            page = request['secrets']['page']
            data = get_paginated_data(request, page)
            state = request['state']
            is_paginated = check_for_pagination(data, request['secrets']['is_pag'])
        
        else:
            data_and_cursors = get_data(request, refresh_tokens, page)
            data = data_and_cursors[0]
            cursors = data_and_cursors[1]
            state = build_the_state(refresh_tokens, cursors)
            is_paginated = check_for_pagination(data, refresh_tokens)
        
        # Prep data for Fivetran
        data = flatten_2d_list(data)

        has_more = True if len(is_paginated) > 0 else False
        if len(is_paginated) > 0: page += 1

        # Assemble response for Fivetran
        response = assemble_response(data, state, is_paginated, has_more, page)
        pprint(response, sort_dicts=False)

        # Send to Fivetran
        headers = {'Content-Type': 'application/json'}
        #return flask.make_response(response,200,headers)

    except Exception as e:
        logging.error('Exception occurred', exc_info=True)

def get_data(request, refresh_tokens, page):
    global events_per_page
    data = []
    cursors =[]
    for token in refresh_tokens:
        # *! After is important here due to incremental syncs so pass it to make_api_call!
        after = None
        if len(request['state']) != 0:
            after = request['state'].get(token) 
        
        user_data = make_api_call(request, token, page, events_per_page, after)
        data.append(user_data)

        cursors = build_the_cursors(request, user_data, token, cursors)

    return data, cursors

def get_paginated_data(request, page):
    global events_per_page
    data = []
    for token in request['secrets']['is_pag']:
        user_data = make_api_call(request, token, page, events_per_page)
        data.append(user_data)

    return data

def check_for_pagination(data, refresh_tokens): # pass in request as well
    is_paginated = []
    for i in range(0, len(data)):
        if len(data[i]) == 1:
            is_paginated.append(refresh_tokens[i])
    
    return is_paginated

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

def build_the_cursors(request,user_data,token, cursors):
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
    state = {}
    for key, value in zip(refresh_tokens, cursors):
        state[key] = value

    return state

def flatten_2d_list(list_2d):
    flat_list = []
    # Iterate through the outer list
    for element in list_2d:
        if type(element) is list:
            # If the element is of type list, iterate through the sublist
            for item in element:
                flat_list.append(item)
        else:
            flat_list.append(element)
    return flat_list

def assemble_response(data, state, is_paginated, has_more, page):
    response_dict = {
        'secrets': {
            'is_pag': is_paginated,
            'page': page
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
    # return json.dumps(response_dict)

if __name__ == '__main__':
    main(
        {
            'state': {
            },
            'secrets': {
                'client_id': '91382',
                'client_secret': '20a49adb714cc2314bbdf5f1514a6b86cd4838eb',
                "is_pag": [],
                "page": 1,
            },
            'hasMore': False
        })


# --- New State using epoch --- #
# {
#     '1e415ed72533c20dc67bc32d080a2f48cb120079': 1660431992,
#     '39f6445fe8cd4c6b543ee0db70d3fa2dfd1fa289': 1660600401,
#     '5dc2bef4b13f135c5ddab9563e74742eec1fa3df': 1660266675
# }