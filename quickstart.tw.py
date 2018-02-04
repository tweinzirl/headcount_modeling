
from __future__ import print_function
import httplib2
import os

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json

#SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'

#https://stackoverflow.com/questions/38534801/google-spreadsheet-api-request-had-insufficient-authentication-scopes
SCOPES = 'https://www.googleapis.com/auth/spreadsheets' #want write access
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart'


def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def main():
    """Shows basic usage of the Sheets API.

    Creates a Sheets API service object and prints the names and majors of
    students in a sample spreadsheet:
    https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit
    """
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discoveryUrl)

    spreadsheetId = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'
    #rangeName = 'Class Data!A2:E'
    rangeName = 'Class Data!A1:C'
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId, range=rangeName).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
    else:
        print('Name, Major:')
        for row in values:
            # Print columns A and E, which correspond to indices 0 and 4.
            #print('%s, %s' % (row[0], row[4]))
            print('%s, %s' % (row[0], row[2]))

    #'''
    ### twmod below: now write this data to a new personal sheet
    spreadsheetId = '1D5UC6tTuCKECsw0hLJHfFKQzI_iXaYAzB_jwKqVX-Ic'
    rangeName = 'Sheet1!A1:C'

    #example write: https://stackoverflow.com/questions/9690138/how-do-i-access-read-write-to-google-sheets-spreadsheets-with-python

    print(values)
    values[13][2] = 'booger'
    values[19][1] = 'booger'

    body = {'values':values}
    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheetId, range=rangeName, valueInputOption='RAW', 
        body=body).execute()

    #change single cell
    rangeName = 'Sheet1!F5'
    body = {'values':[['weeweewee']]}
    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheetId, range=rangeName, valueInputOption='RAW', 
        body=body).execute()

    #retrieve named range SomeBullshit
    rangeName = 'Sheet1!SomeBullshit'
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId, range=rangeName).execute()
    bs = result.get('values', [])

    print('named range: SomeBullshit: ', bs)
    #'''

    #retrieve the whole spreadsheet to understand how jagged data is represented
    #note the columns are truncated after the column containing last val
    # if row has 3 values in succession, only those three rows show.  if 3 values are spread
    # over 6 col, then just the 6 col show even though we asked for 8 columns (A:H) total
    rangeName = 'Sheet1!A1:H31'
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId, range=rangeName).execute()
    jagged = result.get('values', [])

    for i in range(len(jagged)): print(jagged[i])
    #'''

if __name__ == '__main__':
    main()


