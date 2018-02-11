#original imports from quickstart example
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

#tw imports
import pandas as pd
import os
import re


# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json

#SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'

#https://stackoverflow.com/questions/38534801/google-spreadsheet-api-request-had-insufficient-authentication-scopes
SCOPES = 'https://www.googleapis.com/auth/spreadsheets' #want write access
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart'

BI_ENGINE_SHEET = '1TM6LcvN3yf_1zn9XBQwztmVP01Q89u6xZWIMaCzzHA4'
#EMPLOYEE_ACTIVE_SHEET = '1QXey8yWery_tKlixXtvmQxHBq8RDPPldD9ZOhSqzzJQ'
#EMPLOYEE_START_SHEET = '1YUdi-poZTWYG__Ks6ADVWxWbi35SCK2XCkvZo1XSXFo'
#EMPLOYEE_EXIT_SHEET = '1YlYLU8yIRLaIcLJkdzA_mjzGffZ1aDEjYcXBZKqfyPk'

INSTRUCTION_SHEET_NAME = 'INSTRUCTIONS_V2'
STEPS_RANGE_NAME = 'STEPS_v2'

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
                                   'sheets.googleapis.com-headcount_planning.json')

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

def get_service():
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discoveryUrl)

    return service

def mk_local_output(foutname):
    outdir = os.path.dirname(foutname)
    if not os.path.exists(outdir): os.mkdir(outdir)
    if not os.path.exists(foutname): 
        fout = open(foutname,'w')
        fout.write('#test local out\n')
        fout.close()

def read_sheet(service, rangeName=None, spreadsheetId=None):
    '''Uses specified `service` object to read sheet`spreadsheetId`  over the specified
       `rangeName`.  Outputs a pandas dataframe.  Assumes the first record are column headings
    '''
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId, range=rangeName).execute()
    values = result.get('values', [])
    rrange = result.get('range', [])

    columns = [h for h in values[0]]
    df = pd.DataFrame(columns=columns)

    for col in range(len(values[0])):
        for row in range(1,len(values)):
            #only need to add the row,column combination if it exists
            if (col+1) <= len(values[row]): 
                if values[row][col] != '':
                    df.loc[row,columns[col]] = values[row][col]
                else: 
                    df.loc[row,columns[col]] = pd.np.nan #replace '' with NaN

    row_number = int(re.findall('[0-9]+',result['range'].split('!')[1].split(':')[0])[0])
    row_number += 1 #because first row is header
    df.index = pd.np.arange(row_number, len(df)+row_number)

    return df, rrange

def get_date_attribute(df, sourceField, func):
    datecol = pd.to_datetime(df[sourceField], format = '%m/%d/%y')
    new = datecol.map(func)
    return new

def update_status_processed(service, Steps_df, row, spreadsheetId):
    if unicode.find(Steps_df.loc[row,'Processing Status(% complete)'], 'Processed') ==- 1:
        spreadsheetId=BI_ENGINE_SHEET
        rangeName = '%s!D%d'%(INSTRUCTION_SHEET_NAME,row) #status in row D
        body = {'values':[['Processed']]}
        result = service.spreadsheets().values().update(
               spreadsheetId=spreadsheetId, range=rangeName, valueInputOption='RAW', 
               body=body).execute()

def update_status_skipped(service, Steps_df, row, spreadsheetId):
    if unicode.find(Steps_df.loc[row,'Processing Status(% complete)'], 'Processed') ==- 1:
        spreadsheetId=BI_ENGINE_SHEET
        rangeName = '%s!D%d'%(INSTRUCTION_SHEET_NAME,row) #status in row D
        body = {'values':[['Skipped']]}
        result = service.spreadsheets().values().update(
               spreadsheetId=spreadsheetId, range=rangeName, valueInputOption='RAW', 
               body=body).execute()

def get_tenureGroup(df,tenure_df):
    delta = pd.to_datetime(df['PA_Data_Effective_Date'], format='%m/%d/%y') - \
		pd.to_datetime(df['Client_Date_Official_Job_Current_Job_Start'], format='%m/%d/%y')
    delta = delta.map(lambda x: x.days)/365.25

    tenureGroup = []
    tenure = []

    for i in range(len(delta)):
        delta2 = abs(tenure_df['PA_TENURE_YRS'] - delta.iloc[i])
        am = pd.Series.idxmin(delta2)
        tenureGroup.append(tenure_df.loc[am,'Custom_Tenure_Group'])
        tenure.append(delta.iloc[i])

    return tenureGroup, tenure

def get_fin_unit(df,fin_df,newField):
    map_dict = fin_df[['PA_ORG_FINANCIAL_LL_UNIT_CODE','CUSTOM_FIN1','CUSTOM_FIN2']].to_dict('list')
    mapper = {}

    if newField == 'PA_CUSTOM_FIN1':
        for a,b in zip(map_dict['PA_ORG_FINANCIAL_LL_UNIT_CODE'], map_dict['CUSTOM_FIN1']): mapper[a] = b


    if newField == 'PA_CUSTOM_FIN2':
        for a,b in zip(map_dict['PA_ORG_FINANCIAL_LL_UNIT_CODE'], map_dict['CUSTOM_FIN2']): mapper[a] = b
    
    print(df['PA_ORG_FINANCIAL_LL_UNIT_CODE'].map(mapper))
    #print(df.columns)
    return df['PA_ORG_FINANCIAL_LL_UNIT_CODE'].map(mapper)

def crosstab_concat(df,cols,sep='__'):
    c = df[cols[0]] 
    for i in range(1,len(cols)):
        c = c + sep + df[cols[i]]
    return c

def main():
    #get the service
    service = get_service()

    #read the instructions from the BI engine and print
    rangeName = '%s!%s'%(INSTRUCTION_SHEET_NAME,STEPS_RANGE_NAME)
    spreadsheetId=BI_ENGINE_SHEET
    Steps_df, Steps_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadsheetId)

    ###can parse the steos_range to track which cells to update for each step
    #print(Steps_df.head(15))
    #print(steps_range)

    #detect the starting row where the steps are stored (probably 16 but will autodetect)
    #use this variable to track which row in the engine to communicate step updates to
    #assume StepName is stored in column E
    print('Steps discovered over range ', Steps_range)

    ### can move this later into a stepHandler function
    foutname = None #placeholder for local file output
    EmployeeActiveFiles = []
    EmployeeStartFiles = []
    EmployeeExitFiles = []

    FieldNameMapping = {'Active':None, 'Start':None, 'Exit':None} #filename of the dataframes of the Mappings to apply

    Active_df = None #main data frame to hold Active employee data
    Start_df = None #main data frame to hold Start employee data
    Exit_df = None #main data frame to hold Exit employee data

    for row in Steps_df.index: 
        step = Steps_df.loc[row,'StepName'].split(':')[1].strip()
        print('Step type `%s` discovered at E%d'%(step, row))

        #Need to capitalize File in the original sheet
        #`Create Output File` step to create output file and store name, or else keep track of output file name
        if step == 'Create Output File': 

            #define output file
            foutname = os.path.join('localOutput', Steps_df.loc[row,'Field2'].split(':')[1].strip())

            ###check the output file is there anyway
            try: assert os.path.exists(foutname)
            except AssertionError:
                print('local output does not exist, creating')
                mk_local_output(foutname)   

            #update sheet if step not already completed
            if unicode.find(Steps_df.loc[row,'Processing Status(% complete)'], 'Processed') ==- 1:
                update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)


        #load file into memory
        if step == 'Load File Into Memory' and unicode.find(Steps_df.loc[row,'Processing Status(% complete)'], 'Processed') ==- 1:
            #spreadsheetId of data
            spreadSheetId = Steps_df.loc[row,'Field1'].split(':')[1].strip()

            datatype = Steps_df.loc[row,'Field4'].split(':')[1].strip()
            output_location = Steps_df.loc[row,'Field5'].split(':')[1].strip()
            output_filename = os.path.join('localOutput',Steps_df.loc[row,'Field6'].split(':')[1].strip())

            if datatype == 'EmployeeActive': #placeholder in lieu of talking to real endpoint
                startdate = Steps_df.loc[row,'Field2'].split(':')[1].strip()
                mapdate = Steps_df.loc[row,'Field3'].split(':')[1].strip()
                toks = mapdate.split('/')
                end_mm,end_dd,end_yyyy = toks[0], toks[1], toks[2] 
                toks = startdate.split('/')
                start_mm,start_dd,start_yyyy = toks[0], toks[1], toks[2] 
                #update output file name
                output_filename = output_filename.replace('YYYY',end_yyyy)
                output_filename = output_filename.replace('MM',end_mm)
                output_filename = output_filename.replace('DD',end_dd)
                print('process unloaded data, type = %s, mm/dd/yyyy = %s/%s/%s, output = %s'%(datatype, end_mm,end_dd,end_yyyy, output_filename))

                #retrieve the data
                rangeName = 'Sheet1' 
                #print(tmp_df.head(1))
                #apply date change - note mm/dd/yy format
                date1 = '%d/%d/%s'%(int(start_mm),int(start_dd),start_yyyy[-2:])
                date2 = '%d/%d/%s'%(int(end_mm),int(end_dd),end_yyyy[-2:])

                #print('search for %s, replace with %s'%(date1, date2))
                #print(tmp_df['Effective Date'])
                #print(tmp_df['Effective Date']==date1)

                #append file name
                EmployeeActiveFiles.append(output_filename)

                #update Active_df if data not already stored locally
                if Active_df is not None: #exists, so append to it
                    tmp_df = pd.read_csv(output_filename)
                    Active_df = Active_df.append(tmp_df, ignore_index=True)
                else: #read in the initial file of this data frame
                    #Active_df = pd.read_csv(output_filename)
                    tmp_df, tmp_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadSheetId)
                    tmp_df.loc[tmp_df['Effective Date']==date1,'Effective Date']=date2
                    Active_df = tmp_df.copy()

            elif datatype == 'EmployeeStartList':
                enddate = Steps_df.loc[row,'Field2'].split(':')[1].strip()
                toks = mapdate.split('/')
                end_mm,end_dd,end_yyyy = toks[0], toks[1], toks[2] 

                rangeName = 'Sheet1' 
                #print(tmp_df.head(1))

                #append file name
                EmployeeStartFiles.append(output_filename)

                #update Start_df if data not already stored locally
                if Start_df is not None: #exists, so append to it
                    tmp_df = pd.read_csv(output_filename)
                    Start_df = Start_df.append(tmp_df, ignore_index=True)
                else: #read in the initial file of this data frame
                    #Start_df = pd.read_csv(output_filename)
                    tmp_df, tmp_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadSheetId)
                    Start_df = tmp_df.copy()

            elif datatype == 'EmployeeExitList':
                rangeName = 'Sheet1' 
                tmp_df, tmp_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadSheetId)
                #print(tmp_df.head(1))

                #append file name
                EmployeeExitFiles.append(output_filename)

                #update Exit_df if data not already stored locally
                if Exit_df is not None: #exists, so append to it
                    tmp_df = pd.read_csv(output_filename)
                    Exit_df = Exit_df.append(tmp_df, ignore_index=True)
                else: #read in the initial file of this data frame
                    #Exit_df = pd.read_csv(output_filename)
                    Exit_df = tmp_df.copy()

            #cache the file not matter what type it is
            tmp_df.to_csv(output_filename, index=False)

            #update the cell with Processed
            spreadsheetId=BI_ENGINE_SHEET
            rangeName = '%s!D%d'%(INSTRUCTION_SHEET_NAME,row) #status in row D

            body = {'values':[['Processed']]}
            result = service.spreadsheets().values().update(
                spreadsheetId=spreadsheetId, range=rangeName, valueInputOption='RAW', 
                body=body).execute()


        #now case where data already exists locally
        elif step == 'Load File Into Memory':
            datatype = Steps_df.loc[row,'Field4'].split(':')[1].strip()
            input_location = Steps_df.loc[row,'Field5'].split(':')[1].strip()
            input_filename = os.path.join('localOutput',Steps_df.loc[row,'Field6'].split(':')[1].strip())

            if datatype == 'EmployeeActive': #placeholder in lieu of talking to real endpoint
                startdate = Steps_df.loc[row,'Field2'].split(':')[1].strip()
                mapdate = Steps_df.loc[row,'Field3'].split(':')[1].strip()
                toks = mapdate.split('/')
                end_mm,end_dd,end_yyyy = toks[0], toks[1], toks[2] 
                toks = startdate.split('/')
                start_mm,start_dd,start_yyyy = toks[0], toks[1], toks[2] 
                #update input file name
                input_filename = input_filename.replace('YYYY',end_yyyy)
                input_filename = input_filename.replace('MM',end_mm)
                input_filename = input_filename.replace('DD',end_dd)

                EmployeeActiveFiles.append(input_filename)

                if Active_df is not None: #exists, so append to it
                    tmp_df = pd.read_csv(input_filename)
                    Active_df = Active_df.append(tmp_df, ignore_index=True)
                else: #read in the initial file of this data frame
                    Active_df = pd.read_csv(input_filename)

            elif datatype == 'EmployeeStartList':
                EmployeeStartFiles.append(input_filename)
                if Start_df is not None: #exists, so append to it
                    tmp_df = pd.read_csv(input_filename)
                    Start_df = Start_df.append(tmp_df, ignore_index=True)
                else: #read in the initial file of this data frame
                    Start_df = pd.read_csv(input_filename)

            elif datatype == 'EmployeeExitList':
                EmployeeExitFiles.append(input_filename)
                if Exit_df is not None: #exists, so append to it
                    tmp_df = pd.read_csv(input_filename)
                    Exit_df = Exit_df.append(tmp_df, ignore_index=True)
                else: #read in the initial file of this data frame
                    Exit_df = pd.read_csv(input_filename)


        #column names to remap
        if step == 'Load Field Names':
            recordType = Steps_df.loc[row,'Field1'].split(':')[1].strip()
            rangeName = Steps_df.loc[row,'Field2'].split(':')[1].strip()
            spreadsheetId=BI_ENGINE_SHEET
            tmp_df, tmp_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadsheetId)
            output_filename = os.path.join('localOutput','field_names_%s.csv'%recordType)
            FieldNameMapping[recordType] = output_filename

            #if step not already processed, save the data locally in the right file and update the sheet
            if unicode.find(Steps_df.loc[row,'Processing Status(% complete)'], 'Processed') ==- 1:
                tmp_df.to_csv(output_filename, index=False)
                #update the cell with Processed
                spreadsheetId=BI_ENGINE_SHEET
                rangeName = '%s!D%d'%(INSTRUCTION_SHEET_NAME,row) #status in row D

                body = {'values':[['Processed']]}
                result = service.spreadsheets().values().update(
                    spreadsheetId=spreadsheetId, range=rangeName, valueInputOption='RAW', 
                    body=body).execute()

        if step == 'MapFields_ObfuscateData': #this step happens every time since it happens in memory
            datatype = Steps_df.loc[row,'Field1'].split(':')[1].strip()
            map_dict = pd.read_csv(FieldNameMapping[datatype])[['Source Field (in WorkDay)','Destination Field (in our model)']].to_dict('list')
            mapper = {}
            #Some of the mappings are fucked up because the names in the source spreadsheet are wrong. They can fix this
            for a,b in zip(map_dict['Source Field (in WorkDay)'], map_dict['Destination Field (in our model)']):
                mapper[a] = b

            if datatype == 'Active':
                Active_df = Active_df.rename(columns=mapper, index=str)
                #Active_df = MapFields(Active_df, FieldNameMapping[datatype])
                #print(mapper)
                #print('Before', Active_df.columns)
                #print('after', Active_df.columns)
            elif datatype == 'Start':
                Start_df = Start_df.rename(columns=mapper, index=str)
            elif datatype == 'Exit':
                Exit_df = Exit_df.rename(columns=mapper, index=str)

            #skipping the obsfucate data part since it makes no sense

            #if step not already processed, mark the sheet to show it has been processed
            if unicode.find(Steps_df.loc[row,'Processing Status(% complete)'], 'Processed') ==- 1:
                spreadsheetId=BI_ENGINE_SHEET
                rangeName = '%s!D%d'%(INSTRUCTION_SHEET_NAME,row) #status in row D

                body = {'values':[['Processed']]}
                result = service.spreadsheets().values().update(
                    spreadsheetId=spreadsheetId, range=rangeName, valueInputOption='RAW', 
                    body=body).execute()

        if step == 'CreateNewField_FromMap': #this step happens every time since it happens in memory
            newField = Steps_df.loc[row,'Field1'].split(':')[1].strip()
            rangeName = Steps_df.loc[row,'Field2'].split(':')[1].strip()
            spreadsheetId=BI_ENGINE_SHEET
            tmp_df, tmp_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadsheetId)
            #output_filename = os.path.join('localOutput','field_names_%s.csv'%recordType)
            #FieldNameMapping[recordType] = output_filename
            map_dict = tmp_df[['Source Field Data','Destination Segment Grouping']].to_dict('list')
            mapper = {}
            for a,b in zip(map_dict['Source Field Data'], map_dict['Destination Segment Grouping']):
                mapper[a] = b

            #print(Active_df['Client_Job_Family_Detailed'])
            #print(Active_df['Client_Job_Family_Detailed'].map(mapper))
            Active_df[newField] = Active_df['Client_Job_Family_Detailed'].map(mapper)
            #Start_df['Client_Job_Family_Detailed'] = Start_df['Client_Job_Family_Detailed'].map(mapper) #transformation not valid for starts apparently, recheck later
            Exit_df[newField] = Exit_df['Client_Job_Family_Detailed'].map(mapper)

 
            #if step not already processed, mark the sheet to show it has been processed
            if unicode.find(Steps_df.loc[row,'Processing Status(% complete)'], 'Processed') ==- 1:
                spreadsheetId=BI_ENGINE_SHEET
                rangeName = '%s!D%d'%(INSTRUCTION_SHEET_NAME,row) #status in row D

                body = {'values':[['Processed']]}
                result = service.spreadsheets().values().update(
                    spreadsheetId=spreadsheetId, range=rangeName, valueInputOption='RAW', 
                    body=body).execute()

        if step == 'CreateNewField_FromExpression': #this step happens every time since it happens in memory
            newField = Steps_df.loc[row,'Field1'].split(':')[1].strip()
            sourceField = Steps_df.loc[row,'Field3'].split(':')[1].strip()

            if newField == 'Year':
                f = lambda x: x.year
                Active_df[newField] = get_date_attribute(Active_df, sourceField, f)
                Start_df[newField] = get_date_attribute(Start_df, sourceField, f)
                Exit_df[newField] = get_date_attribute(Exit_df, sourceField, f)
            elif newField == 'Month':
                f = lambda x: x.month
                Active_df[newField] = get_date_attribute(Active_df, sourceField, f)
                Start_df[newField] = get_date_attribute(Start_df, sourceField, f)
                Exit_df[newField] = get_date_attribute(Exit_df, sourceField, f)
            elif newField == 'Calendar_Year_Month':
                f = lambda x: '%d-%d'%(x.year,x.month)
                Active_df[newField] = get_date_attribute(Active_df, sourceField, f)
                Start_df[newField] = get_date_attribute(Start_df, sourceField, f)
                Exit_df[newField] = get_date_attribute(Exit_df, sourceField, f)

            #if step not already processed, mark the sheet to show it has been processed
            if unicode.find(Steps_df.loc[row,'Processing Status(% complete)'], 'Processed') ==- 1:
                spreadsheetId=BI_ENGINE_SHEET
                rangeName = '%s!D%d'%(INSTRUCTION_SHEET_NAME,row) #status in row D

                body = {'values':[['Processed']]}
                result = service.spreadsheets().values().update(
                    spreadsheetId=spreadsheetId, range=rangeName, valueInputOption='RAW', 
                    body=body).execute()

        if step == 'CreateNewField_FromData': #this step happens every time since it happens in memory
            newField = Steps_df.loc[row,'Field1'].split(':')[1].strip()
            
            if newField == 'XYZ_Period':
                rangeName = Steps_df.loc[row,'Field2'].split(':')[1].strip()

                #retrieve ActivityPeriod named range and store as dict
                spreadsheetId=BI_ENGINE_SHEET
                tmp_df, tmp_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadsheetId)

                #map from Calendar_Year_Month to new XYZ_Period
                map_dict = tmp_df[['Year-Month','Period']].to_dict('list')
                mapper = {}
                for a,b in zip(map_dict['Year-Month'], map_dict['Period']):
                    mapper[a] = b

                Active_df[newField] = Active_df['Calendar_Year_Month'].map(mapper)
                Start_df[newField] = Start_df['Calendar_Year_Month'].map(mapper)
                Exit_df[newField] = Exit_df['Calendar_Year_Month'].map(mapper)
                #print(Active_df[newField])

                #if step not already processed, mark the sheet to show it has been processed
                update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

            elif newField == 'Active_Start_Period': #not sure what this column is supposed to mean
                #status update
                update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

            elif newField == 'Active_End_Period': #not sure what this column is supposed to mean
                #status update
                update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

            elif newField == 'PA_CUSTOM_TENURE_GRP':
                rangeName = Steps_df.loc[row,'Field2'].split(':')[1].strip()
                spreadsheetId=BI_ENGINE_SHEET
                tmp_df, tmp_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadsheetId)
                tmp_df['PA_TENURE_YRS'] = tmp_df['PA_TENURE_YRS'].astype(float)

                tenureGroup, tenure = get_tenureGroup(Active_df, tmp_df)
                Active_df['Custom_Tenure_Group'] = tenureGroup
                Active_df['Tenure'] = tenure
                #print(Active_df[['PA_Data_Effective_Date','Client_Date_Official_Job_Current_Job_Start','Custom_Tenure_Group']].head())
                
                tenureGroup, tenure = get_tenureGroup(Start_df, tmp_df)
                Start_df['Custom_Tenure_Group'] = tenureGroup
                Start_df['Tenure'] = tenure

                tenureGroup, tenure = get_tenureGroup(Exit_df, tmp_df)
                Exit_df['Custom_Tenure_Group'] = tenureGroup
                Exit_df['Tenure'] = tenure

                #if step not already processed, mark the sheet to show it has been processed
                update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

            elif newField == 'PA_CUSTOM_FIN1' or newField == 'PA_CUSTOM_FIN2': #!!!!!!!!!need data with the right column in order to test this!!!!!!1
                rangeName = Steps_df.loc[row,'Field2'].split(':')[1].strip()
                spreadsheetId=BI_ENGINE_SHEET
                tmp_df, tmp_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadsheetId)

                #if newField == 'PA_CUSTOM_FIN1':
                #    Active_df['PA_CUSTOM_FIN1'] = get_fin_unit(Active_df,tmp_df,newField)
                #    Start_df['PA_CUSTOM_FIN1'] = get_fin_unit(Start_df,tmp_df,newField)
                #    Exit_df['PA_CUSTOM_FIN1'] = get_fin_unit(Exit_df,tmp_df,newField)

                #if newField == 'PA_CUSTOM_FIN2':
                #    Active_df['PA_CUSTOM_FIN2'] = get_fin_unit(Active_df,tmp_df,newField)
                #    Start_df['PA_CUSTOM_FIN2'] = get_fin_unit(Start_df,tmp_df,newField)
                #    Exit_df['PA_CUSTOM_FIN2'] = get_fin_unit(Exit_df,tmp_df,newField)

                ''' #rm this after successful test
                map_dict = tmp_df[['PA_ORG_FINANCIAL_LL_UNIT_CODE','CUSTOM_FIN1','CUSTOM_FIN2']].to_dict('list')
                mapper = {}

                if newField == 'PA_CUSTOM_FIN1':
                    for a,b in zip(map_dict['PA_ORG_FINANCIAL_LL_UNIT_CODE'], map_dict['CUSTOM_FIN1']): mapper[a] = b

                    #print(Active_df['PA_ORG_FINANCIAL_LL_UNIT_CODE'].map(mapper))
                    print(Active_df.columns)

                if newField == 'PA_CUSTOM_FIN2':
                    for a,b in zip(map_dict['PA_ORG_FINANCIAL_LL_UNIT_CODE'], map_dict['CUSTOM_FIN2']): mapper[a] = b
                '''

                #if step not already processed, mark the sheet to show it has been processed
                #update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)
                update_status_skipped(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

            elif newField == 'PA_CUSTOM_JOB_FAMILY':
                rangeName = Steps_df.loc[row,'Field2'].split(':')[1].strip()
                spreadsheetId=BI_ENGINE_SHEET
                tmp_df, tmp_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadsheetId)

                #map job family
                map_dict = tmp_df[['PA_Job_Official_Name','CUSTOM_JOB_FAMILY']].to_dict('list')
                mapper = {}
                for a,b in zip(map_dict['PA_Job_Official_Name'], map_dict['CUSTOM_JOB_FAMILY']):
                    mapper[a] = b

                Active_df[newField] = Active_df['PA_Job_Official_Name'].map(mapper)
                #Start_df[newField] = Start_df['PA_Job_Official_Name'].map(mapper) #deactive for Start_df for now because test data sucks
                Exit_df[newField] = Exit_df['PA_Job_Official_Name'].map(mapper)
                #print(Active_df[newField])

                #if step not already processed, mark the sheet to show it has been processed
                update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

            elif newField == 'PA_CUSTOM_JOB_LEVEL':
                rangeName = Steps_df.loc[row,'Field2'].split(':')[1].strip()
                spreadsheetId=BI_ENGINE_SHEET
                tmp_df, tmp_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadsheetId)

                #map job level
                map_dict = tmp_df[['PA_Job_LEVEL','Client_Management_Level']].to_dict('list')
                mapper = {}
                for a,b in zip(map_dict['PA_Job_LEVEL'], map_dict['Client_Management_Level']):
                    mapper[a] = b

                Active_df[newField] = Active_df['Client_Management_Level'].map(mapper)
                #Start_df[newField] = Start_df['PA_Job_Official_Name'].map(mapper) #deactive for Start_df for now because test data sucks
                Exit_df[newField] = Exit_df['PA_Job_Official_Name'].map(mapper)
                #print(Active_df[newField])
                #print(Exit_df[newField])

                #if step not already processed, mark the sheet to show it has been processed
                update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

            elif newField == 'PA_CUSTOM_REGION':
                rangeName = Steps_df.loc[row,'Field2'].split(':')[1].strip()
                spreadsheetId=BI_ENGINE_SHEET
                tmp_df, tmp_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadsheetId)

                #map region
                map_dict = tmp_df[['PA_LOCATION','CUSTOM_REGION']].to_dict('list')
                mapper = {}
                for a,b in zip(map_dict['PA_LOCATION'], map_dict['CUSTOM_REGION']):
                    mapper[a] = b

                Active_df[newField] = Active_df['Client_REGION']
                #Start_df[newField] = Start_df['Client_REGION'] #deactive for Start_df for now because test data sucks
                Exit_df[newField] = Exit_df['Client_REGION']
                #Active_df[newField] = Active_df['Client_REGION'].map(mapper)
                #Start_df[newField] = Start_df['Client_REGION'].map(mapper) #deactive for Start_df for now because test data sucks
                #Exit_df[newField] = Exit_df['Client_REGION'].map(mapper)
                #print(Active_df[newField])

                #if step not already processed, mark the sheet to show it has been processed
                update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

            elif newField == 'PA_CROSSTAB_REGION_LOCATION':

                cols=['Client_Location','Client_REGION']
                Active_df[newField] = crosstab_concat(Active_df,cols )
                #Start_df[newField] = crosstab_concat(Start_df,cols) #deactivating because test data sucks
                Exit_df[newField] = crosstab_concat(Exit_df,cols)
                #print(Active_df[newField])
                #print(Start_df[newField])

                #if step not already processed, mark the sheet to show it has been processed
                update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

            elif newField == 'PA_CROSSTAB_FIN1_JOBLEVEL':

                pass #until FIN1 cols made with better test data
                '''
                cols=['PA_CUSTOM_FIN1','PA_CUSTOM_JOB_LEVEL']
                Active_df[newField] = crosstab_concat(Active_df,cols, sep='_')
                #Start_df[newField] = crosstab_concat(Start_df,cols, sep='_') #deactivating because test data sucks
                Exit_df[newField] = crosstab_concat(Exit_df,cols, sep='_')
                print(Active_df[newField])
                #print(Start_df[newField])

                #if step not already processed, mark the sheet to show it has been processed
                update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)
                '''

            elif newField == 'PA_CROSSTAB_FIN1_REGION_JOBLEVEL':
                
                pass #until FIN1 colas made with better test data
                '''
                #cols = ['Client_Region', 'PA_CUSTOM_FIN1', 'PA_CUSTOM_JOB_LEVEL']
                cols = ['Client_REGION', 'PA_CUSTOM_JOB_LEVEL']
                Active_df[newField] = crosstab_concat(Active_df,cols, sep='_')
                #Start_df[newField] = crosstab_concat(Start_df,cols, sep='_') #deactivating because test data sucks
                Exit_df[newField] = crosstab_concat(Exit_df,cols, sep='_')
                #print(Active_df[newField])

                #if step not already processed, mark the sheet to show it has been processed
                update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)
                '''

            elif newField == 'PA_CROSSTAB_FIN1_JOBLEVEL_TENURE':

                pass #until FIN1 colas made with better test data

                '''
                #cols = ['PA_CUSTOM_FIN1', 'PA_CUSTOM_JOB_LEVEL', 'Custom_Tenure_Group']
                cols = ['PA_CUSTOM_JOB_LEVEL', 'Custom_Tenure_Group']
                Active_df[newField] = crosstab_concat(Active_df,cols, sep='_')
                #Start_df[newField] = crosstab_concat(Start_df,cols, sep='_') #deactivating because test data sucks
                Exit_df[newField] = crosstab_concat(Exit_df,cols, sep='_')
                #print(Active_df[newField])

                #if step not already processed, mark the sheet to show it has been processed
                update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)
                '''

            elif newField == 'PA_CROSSTAB_FIN1_REGION_JOBLEVEL_TENURE':

                pass #until FIN1 colas made with better test data

                '''
                #cols = ['PA_CUSTOM_FIN1', 'PA_CUSTOM_JOB_LEVEL', 'Custom_Tenure_Group']
                cols = ['Client_REGION', 'PA_CUSTOM_JOB_LEVEL', 'Custom_Tenure_Group']
                Active_df[newField] = crosstab_concat(Active_df,cols, sep='_')
                #Start_df[newField] = crosstab_concat(Start_df,cols, sep='_') #deactivating because test data sucks
                Exit_df[newField] = crosstab_concat(Exit_df,cols, sep='_')
                print(Active_df[newField])

                #if step not already processed, mark the sheet to show it has been processed
                update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)
                '''

    print('output file name', foutname)
    print('EmployeeActiveFiles', EmployeeActiveFiles)
    print('EmployeeStartFiles', EmployeeStartFiles)
    print('EmployeeExitFiles', EmployeeExitFiles)
    print('FieldNameMapping', FieldNameMapping)
    print('Active_df row count: ', len(Active_df))
    print('Start_df row count: ', len(Start_df))
    print('Exit_df row count: ', len(Exit_df))

    #creating fields from Data

if __name__ == '__main__':
    main()
