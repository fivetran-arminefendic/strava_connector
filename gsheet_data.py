from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
import re
import requests
# Disable warning for HTTPS requests being made without certificate verifiication
from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings
disable_warnings(InsecureRequestWarning)

def main(request):
    # --- Authentication and Authorization for service acct to google sheet --- #
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    service_account_file = 'service_acct_key.json'
    creds = service_account.Credentials.from_service_account_file(service_account_file, scopes=scopes)

    # --- Google sheet variables --- #
    spreadsheet_id = '1wv9tncWB4yF8PxxEcyvHXFHxdEDqMUiTbUCYA8A4KXE'
    range = 'codes'

    # --- Grab google sheet values --- #
    try:
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                    range=range).execute()
        values = result.get('values')
        values.pop(0) # Remove header of google sheet

    except HttpError as err:
        print(err)

    # --- Get refresh tokens and update google sheet --- #
    refresh_tokens = rw_gsheet(request, values, sheet, spreadsheet_id, range)

    # --- Prepare the data for post processing - remove duplicates and null values --- #
    refresh_tokens = flatten_list(refresh_tokens)
    refresh_tokens = list(dict.fromkeys(refresh_tokens)) # fromkeys() removes duplicates; using set() changes the order of the tokens
    refresh_tokens = list(filter(None, refresh_tokens))

    return(refresh_tokens)

def rw_gsheet(request, values, sheet, spreadsheet_id, range):
    row_num = 1 # offset for the header row in the sheet
    row_numbers = []
    new_refresh_tokens = []
    refresh_tokens = []

    # --- Check each "row" (value) if it contains a refresh token i.e. ['url', 'refresh_token'] --- #
    for value in values:
        row_num += 1
        if len(value) == 2:
            refresh_tokens.append(value[1])

        if len(value) == 1:
            row_numbers.append(row_num)
            refresh_token = get_refresh_token(request,value[0])
            new_refresh_tokens.append(refresh_token)
            refresh_tokens.append(refresh_token)

    # --- Update the google sheet --- #
    if len(new_refresh_tokens) > 0:
        post = sheet.values().append(spreadsheetId=spreadsheet_id, range=f'{range}!B'+str(row_numbers[0]), 
                                        valueInputOption='RAW', body={'values':new_refresh_tokens}).execute()

    return refresh_tokens

def get_refresh_token(request,url):
    auth_url = 'https://www.strava.com/oauth/token'
    if re.findall('(?<=code=)(.*)(?=&scope)', url):
        code = re.findall('(?<=code=)(.*)(?=&scope)', url)
        payload = {
            'client_id': request['secrets']['client_id'],
            'client_secret': request['secrets']['client_secret'],
            'code': code,
            'grant_type': 'authorization_code',
            'f': 'json'
        }
        get_refresh_token = requests.post(auth_url, data=payload, verify=False)
        if get_refresh_token.status_code != 200:
            print(f'{get_refresh_token.status_code} Error: There was an issue with getting the refresh token using code {code} - check the POST request')
            return
        refresh_token = [get_refresh_token.json()['refresh_token']]
        return refresh_token

def flatten_list(_2d_list):
    flat_list = []
    for element in _2d_list:
        if type(element) is list:
            for item in element:
                flat_list.append(item)
        else:
            flat_list.append(element)
    return flat_list