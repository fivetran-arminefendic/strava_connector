import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
import re
import requests

def main(request):
    # the type of service we need access too (spreadsheets with readonly)
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


    SERVICE_ACCOUNT_FILE = '/Users/armin.efendic/Documents/strava_api/service_acct_key.json'


    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    SPREADSHEET_ID = '1wv9tncWB4yF8PxxEcyvHXFHxdEDqMUiTbUCYA8A4KXE'
    RANGE = 'codes'

    try:
        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                    range=RANGE).execute()
        values = result.get('values')
        # Remove header
        values.pop(0)

        # refresh_tokens = get_refresh_tokens()
    except HttpError as err:
        print(err)


    row_num = 1 # set to 1 to offset for the header row
    row_numbers = []
    # google needs a list of lists for the values
    new_refresh_tokens = []
    refresh_tokens = []
    # grad the cells that do not have a refresh token
    for value in values:
        row_num += 1 # "iterates" thru the rows of the sheet
        if len(value) == 2:
            refresh_tokens.append(value[1])

        if len(value) == 1:
            row_numbers.append(row_num) # mark the row number
            refresh_token = get_refresh_token(request,value[0]) # pass in the url
            new_refresh_tokens.append(refresh_token)
            refresh_tokens.append(refresh_token)

    if len(new_refresh_tokens) > 0:
        post = sheet.values().append(spreadsheetId=SPREADSHEET_ID, range='codes!B'+str(row_numbers[0]), 
                                        valueInputOption='RAW', body={'values':new_refresh_tokens}).execute() # values requires to be a list of lists
    
    refresh_tokens = flatten_list(refresh_tokens)

    # Filter out all None values and remove duplicates for post processing purposes
    # None values will get added to the if the post request doesn't return a new refresh token
    refresh_tokens = [*set((filter(None, refresh_tokens)))]
    # print(refresh_tokens)
    return(refresh_tokens)

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
    # Iterate through the outer list
    for element in _2d_list:
        if type(element) is list:
            # If the element is of type list, iterate through the sublist
            for item in element:
                flat_list.append(item)
        else:
            flat_list.append(element)
    return flat_list

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