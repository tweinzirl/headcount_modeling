#!/usr/bin/env python2

#original imports from quickstart example
from __future__ import print_function
import httplib2
import os

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

#tw imports
import pandas as pd
import numpy as np
import os
import re
import argparse
import time

import helpers

'''
usage:
    >>> ./prototype_v2.py  --instruct_sheet [instruction sheet] --step_range [instruction steps]
    >>> ./prototype_v2.py  --instruct_sheet INSTRUCTIONS_V2 --step_range 'C15:L55'
    [instruction sheet] is the sheet name of the instruction set
    [instruction steps] are the steps to process; this range MUST include the column headings as row 0
'''

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json

#https://stackoverflow.com/questions/38534801/google-spreadsheet-api-request-had-insufficient-authentication-scopes
SCOPES = 'https://www.googleapis.com/auth/spreadsheets' #want write access
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart'

#try to set instruction sheet and instruction step range from commandline flags, else use defaults below
INSTRUCTION_SHEET_NAME = 'INSTRUCTIONS_V2'
STEPS_RANGE_NAME = 'STEPS_v2'
BI_ENGINE_SHEET = '1TM6LcvN3yf_1zn9XBQwztmVP01Q89u6xZWIMaCzzHA4'

TableauInputFile = None

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
    datecol = pd.to_datetime(df[sourceField], format = '%m/%d/%Y') #assume year is YYYY
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
    if unicode.find(Steps_df.loc[row,'Processing Status(% complete)'], 'Skipped') ==- 1:
        spreadsheetId=BI_ENGINE_SHEET
        rangeName = '%s!D%d'%(INSTRUCTION_SHEET_NAME,row) #status in row D
        body = {'values':[['Skipped']]}
        result = service.spreadsheets().values().update(
               spreadsheetId=spreadsheetId, range=rangeName, valueInputOption='RAW', 
               body=body).execute()

def get_tenureGroup(df,tenure_df=None, effective_date='PA_Data_Effective_Date', start_date='Client_Date_Official_Job_Current_Job_Start'):
    ''' Custom tenure group.  Calculates time delta as the as effective_date minus start_date. Then the corresponding tenure group bin is selected.
    '''

    delta = pd.to_datetime(df[effective_date], format='%m/%d/%Y') - \
		pd.to_datetime(df[start_date], format='%m/%d/%Y')
    delta = delta.map(lambda x: x.days)/365.25

    tenureGroup = []
    #tenure = []

    for i in range(len(delta)):
        #delta2 = abs(tenure_df['PA_TENURE_YRS'] - delta.iloc[i])
        delta2 = abs(tenure_df[tenure_df.columns[0]] - delta.iloc[i])
        am = pd.Series.idxmin(delta2)
        tenureGroup.append(tenure_df.loc[am,tenure_df.columns[1]])
        #tenure.append(delta.iloc[i])

    return tenureGroup#, tenure

def get_fin_unit(df,fin_df,newField):
    map_dict = fin_df[['PA_ORG_FINANCIAL_LL_UNIT_CODE','CUSTOM_FIN1','CUSTOM_FIN2']].to_dict('list')
    mapper = {}

    if newField == 'PA_CUSTOM_FIN1':
        for a,b in zip(map_dict['PA_ORG_FINANCIAL_LL_UNIT_CODE'], map_dict['CUSTOM_FIN1']): mapper[a] = b


    if newField == 'PA_CUSTOM_FIN2':
        for a,b in zip(map_dict['PA_ORG_FINANCIAL_LL_UNIT_CODE'], map_dict['CUSTOM_FIN2']): mapper[a] = b
    
    return df['PA_ORG_FINANCIAL_LL_UNIT_CODE'].map(mapper)

def crosstab_concat(df,cols=[],sep='__'):
    c = df[cols[0]] 
    for i in range(1,len(cols)):
        c = c + sep + df[cols[i]]
    return c

def get_hierarchy(df):
    map_df = df[['Client_ID','Client_ID_MANAGER']].set_index('Client_ID')
    emp = map_df.index.values.copy()
    mappers = []

    for i in range(1,8):
        #http://pandas.pydata.org/pandas-docs/stable/indexing.html#deprecate-loc-reindex-listlike
        man = map_df.loc[map_df.index.intersection(emp),'Client_ID_MANAGER'].reindex(emp).dropna().unique()
        #print('level %d - num employees: %d  num managers: %d'%(i,len(emp),len(man)))

        emp = man.copy()

        #retrieve emp with these man
        rel_df = map_df[map_df.Client_ID_MANAGER.isin(man)]
        map_dict = rel_df.to_dict()
        mappers.append(map_dict['Client_ID_MANAGER'])

    management_chain = {}
    for emp, man in mappers[0].items():
        chain = [man] #chain is constructed from the bottom up
        for rd in mappers[1:]:
            if man in rd and rd[man]!=man:
                next_man = rd[man]
                chain.append(next_man)
                man = next_man
            else:
                break
        management_chain[emp] = chain

    management_chain_topdown = management_chain.copy()
    for key, chain in management_chain.items()[:]:
        chain2 = chain[::-1] + [np.nan]*(7-len(chain)) #reverse the chain to get top down view and pad undefined levels with nan
        #print(chain, chain2)
        management_chain_topdown[key] = chain2

    df = pd.DataFrame.from_dict(management_chain_topdown, orient='index')
    df = df.rename({0:'PA_Leadership_Level_1',1:'PA_Leadership_Level_2',2:'PA_Leadership_Level_3',\
        3:'PA_Leadership_Level_4',4:'PA_Leadership_Level_5',5:'PA_Leadership_Level_6',6:'PA_Leadership_Level_7'},axis='columns')

    #print(df.head())

    return df

def metric_output_actual(Active_df, Start_df, Exit_df, pivot_seg_period, pivot_seg_cat='Total', segment_name='Total', output_columns=None, segments=None):

    if pivot_seg_period == 'Year': 
        periods = Active_df['Year'].unique()
        start_end_collection = [helpers.get_date_endpoints(year=year,kind='year') for year in periods]

    elif pivot_seg_period == 'XYZ_Period': 
        periods = Active_df['XYZ_Period'].unique()
        yq = [tok.split('-Q') for tok in periods]
        start_end_collection = [helpers.get_date_endpoints(q=int(x[1]), year=int(x[0]), kind='quarter') for x in yq]

    #something for total 

    if pivot_seg_cat!='Total':
        grped_active = Active_df.groupby(pivot_seg_cat)
        grped_start = Start_df.groupby(pivot_seg_cat)
        grped_exit = Exit_df.groupby(pivot_seg_cat)

        segments=grped_active.groups.keys()

    hc_con1 = 'Client_Date_Official_Job_Current_Job_Start.apply(@pd.to_datetime) <=@start'
    hc_con2 = 'Client_Date_Official_Job_Current_Job_Start.apply(@pd.to_datetime) <=@end'

    start_con1 = 'Client_Date_Official_Job_Current_Job_Start.apply(@pd.to_datetime) >= @start'
    start_con2 = 'Client_Date_Official_Job_Current_Job_Start.apply(@pd.to_datetime) < @end'
    start_con3 = 'Client_Date_Official_Job_Current_Job_Start.apply(@pd.to_datetime) > @end'
    start_con4 = 'Client_Date_Hire.apply(@pd.to_datetime) >= @start'
    start_con5 = 'Client_Date_Hire.apply(@pd.to_datetime) < @end'

    exit_con1 = 'Client_Exit_Action_Date.apply(@pd.to_datetime) >= @start'
    exit_con2 = 'Client_Exit_Action_Date.apply(@pd.to_datetime) < @end'
    exit_con3 = 'Client_Exit_Action_Date.apply(@pd.to_datetime) > @end'

    output_lists = []
    for p, sec in zip(periods,start_end_collection):
        start, end = sec

        if pivot_seg_cat!='Total':
            for segment_name in segments:
                segment_con = '%s=="%s"'%(pivot_seg_cat,segment_name)

                adf=grped_active.get_group(segment_name)
                a_start = adf.query(hc_con1)['Client_ID'].unique()
                begin_hc = a_start.size

                a_end = adf.query(hc_con2)['Client_ID'].unique()
                end_hc = a_end.size
                growth = end_hc - begin_hc
                #move in/out numbers
                set_start = set(a_start)
                set_end = set(a_end)
                move_in = len(set_end.difference(set_start)) #present at end of period but not at beginning
                move_out = len(set_start.difference(set_end)) #present at start of period but not at end

                try:
                    sdf=grped_start.get_group(segment_name)
                    nstart = sdf.query('%s & %s'%(start_con1,start_con2))['Client_ID'].unique().size
                    pending_nstart=sdf.query('%s & %s & %s'%(start_con3,start_con4,start_con5))['Client_ID'].unique().size
                except KeyError:
                    nstart=0
                    pending_nstart=0

                try:
                    edf=grped_exit.get_group(segment_name)
                    nexit = edf.query('%s & %s'%(exit_con1,exit_con2))['Client_ID'].unique().size
                    pending_nexit = edf.query('%s & %s'%(exit_con1,exit_con3))['Client_ID'].unique().size
                except KeyError:
                    nexit=0
                    pending_nexit=0

                denom = begin_hc > 0 and float(begin_hc) or 1. #set denom to 1. if begin_hc is 0
                start_pct = nstart/denom
                exit_pct = nexit/denom
                growth_pct = growth/denom
                move_in_pct = move_in/denom
                move_out_pct = move_out/denom

                metrics_dict = {'Headcount_Begin_#':begin_hc, 'Headcount_End_#':end_hc, 'Start_#':nstart, 'Exit_#':nexit, 'Pending_Start_#':pending_nstart, 'Pending_Exit_#': pending_nexit, 'Growth_#': growth, 'Move_In_#': move_in, 'Move_Out_#': move_out, 'Start_%': start_pct, 'Exit_%': exit_pct, 'Growth_%': growth_pct, 'Move_In_%': move_in_pct, 'Move_Out_%':move_out_pct}

                for ms in metrics_dict.keys():
                    #follow order in output_metrics_columns: 'Segment_Category','Segment_Filter', 'Segment_Name', 'Segment_Period','Metric_Name', 'ACTUAL','PLAN'
                    output_lists.append([pivot_seg_cat, segment_name, p, ms, metrics_dict[ms], np.nan])

        else: #pivot_seg_cat==Total

            a_start = Active_df.query(hc_con1)['Client_ID'].unique()
            begin_hc = a_start.size
            a_end = Active_df.query(hc_con2)['Client_ID'].unique()

            end_hc = a_end.size
            growth = end_hc - begin_hc
            #move in/out numbers
            set_start = set(a_start)
            set_end = set(a_end)
            move_in = len(set_end.difference(set_start)) #present at end of period but not at beginning
            move_out = len(set_start.difference(set_end)) #present at start of period but not at end

            nstart = Start_df.query('%s & %s'%(start_con1,start_con2))['Client_ID'].unique().size
            pending_nstart=Start_df.query('%s & %s & %s'%(start_con3,start_con4,start_con5))['Client_ID'].unique().size

            nexit = Exit_df.query('%s & %s'%(exit_con1,exit_con2))['Client_ID'].unique().size
            pending_nexit = Exit_df.query('%s & %s'%(exit_con1,exit_con3))['Client_ID'].unique().size

            denom = begin_hc > 0 and float(begin_hc) or 1. #set denom to 1. if begin_hc is 0
            start_pct = nstart/denom
            exit_pct = nexit/denom
            growth_pct = growth/denom
            move_in_pct = move_in/denom
            move_out_pct = move_out/denom

            metrics_dict = {'Headcount_Begin_#':begin_hc, 'Headcount_End_#':end_hc, 'Start_#':nstart, 'Exit_#':nexit, 'Pending_Start_#':pending_nstart, 'Pending_Exit_#': pending_nexit, 'Growth_#': growth, 'Move_In_#': move_in, 'Move_Out_#': move_out, 'Start_%': start_pct, 'Exit_%': exit_pct, 'Growth_%': growth_pct, 'Move_In_%': move_in_pct, 'Move_Out_%':move_out_pct}

            for ms in metrics_dict.keys():
                #follow order in output_metrics_columns: 'Segment_Category','Segment_Filter', 'Segment_Name', 'Segment_Period','Metric_Name', 'ACTUAL','PLAN'
                output_lists.append([pivot_seg_cat, segment_name, p, ms, metrics_dict[ms], np.nan])

    return pd.DataFrame(output_lists,columns=output_columns)


def main():
    #get the service
    service = get_service()

    #read the instructions from the BI engine and print
    rangeName = '%s!%s'%(INSTRUCTION_SHEET_NAME,STEPS_RANGE_NAME)
    spreadsheetId=BI_ENGINE_SHEET
    Steps_df, Steps_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadsheetId)

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

    Active_dfh = None #main data frame to hold Active employee data after hierarchy addition
    Start_dfh = None #main data frame to hold Start employee dat after hierarchy additiona
    Exit_dfh = None #main data frame to hold Exit employee dat after hierarchy additiona

    #output metrics df
    output_metrics_columns = ['Segment_Category', 'Segment_Name', 'Segment_Period', 'Metric_Name', 'ACTUAL','PLAN']
    output_metrics_df = pd.DataFrame(columns=output_metrics_columns)

    for row in Steps_df.index: 
        step = Steps_df.loc[row,'StepName'].split(':')[1].strip()
        print('Step type `%s` discovered at E%d'%(step, row))

        #Need to capitalize File in the original sheet
        #`Create Output File` step to create output file and store name, or else keep track of output file name
        if step == 'Create Output File': 

            #define output file
            foutname = os.path.join('localOutput', Steps_df.loc[row,'Field1'].split(':')[1].strip())
            #set local output file
            TableauInputFile = foutname

            ###check the output file is there anyway
            try: assert os.path.exists(foutname)
            except AssertionError:
                print('local output does not exist, creating')
                mk_local_output(foutname)   

            #update sheet if step not already completed
            update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)


        #load file into memory
        if step == 'Load File Into Memory':
            spreadSheetId = Steps_df.loc[row,'Field1'].split(':')[1].strip() #spreadsheetId of data

            datatype = Steps_df.loc[row,'Field4'].split(':')[1].strip()
            filename = os.path.join('localOutput',Steps_df.loc[row,'Field5'].split(':')[1].strip())

            if datatype == 'EmployeeActive': #placeholder in lieu of talking to real endpoint
                EmployeeActiveFiles.append(filename) #append file name

                startdate = Steps_df.loc[row,'Field2'].split(':')[1].strip()
                mapdate = Steps_df.loc[row,'Field3'].split(':')[1].strip()
                toks = mapdate.split('/')
                end_mm,end_dd,end_yyyy = toks[0], toks[1], toks[2] 
                toks = startdate.split('/')
                start_mm,start_dd,start_yyyy = toks[0], toks[1], toks[2] 
                #update output file name
                filename = filename.replace('YYYY',end_yyyy)
                filename = filename.replace('MM',end_mm)
                filename = filename.replace('DD',end_dd)
                print('process unloaded data, type = %s, mm/dd/yyyy = %s/%s/%s, output = %s'%(datatype, end_mm,end_dd,end_yyyy, filename))

                #update Active_df if data not already stored locally
                if os.path.exists(filename):  #empty df but cache exists: read_csv
                    tmp_df = pd.read_csv(filename)

                elif not os.path.exists(filename): 
                    #Active_df is empty and cache file does not exist: pull from Sheet, set to Active_df and cache
                    rangeName = 'Sheet1' 
                    tmp_df, tmp_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadSheetId)

                    #apply date change - note mm/dd/yyyy format
                    date1 = '%d/%d/%d'%(int(start_mm),int(start_dd),int(start_yyyy))
                    date2 = '%d/%d/%d'%(int(end_mm),int(end_dd),int(end_yyyy))
                    tmp_df.loc[tmp_df['Effective Date']==date1,'Effective Date']=date2

                    #cache file
                    tmp_df.to_csv(filename, index=False, encoding='utf-8')

                #initialize or append to Active_df
                if Active_df is None:
                    Active_df = tmp_df.copy()
                elif Active_df is not None:
                    Active_df = Active_df.append(tmp_df, ignore_index=True)

            elif datatype == 'EmployeeStartList':
                EmployeeStartFiles.append(filename) #append file name

                if os.path.exists(filename):  #empty df but cache exists: read_csv
                    tmp_df = pd.read_csv(filename)
                elif not os.path.exists(filename): 
                    #Active_df is empty and cache file does not exist: pull from Sheet, set to Active_df and cache
                    rangeName = 'Sheet1' 
                    tmp_df, tmp_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadSheetId)

                    #cache file
                    tmp_df.to_csv(filename, index=False, encoding='utf-8')

                #initialize or append to Start_df
                if Start_df is None:
                    Start_df = tmp_df.copy()
                elif Start_df is not None:
                    Start_df = Start_df.append(tmp_df, ignore_index=True)

            elif datatype == 'EmployeeExitList':
                EmployeeExitFiles.append(filename) #append file name

                if os.path.exists(filename):  #empty df but cache exists: read_csv
                    tmp_df = pd.read_csv(filename)
                elif not os.path.exists(filename): 
                    #Active_df is empty and cache file does not exist: pull from Sheet, set to Active_df and cache
                    rangeName = 'Sheet1' 
                    tmp_df, tmp_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadSheetId)

                    #cache file
                    tmp_df.to_csv(filename, index=False, encoding='utf-8')

                #initialize or append to Exit_df
                if Exit_df is None:
                    Exit_df = tmp_df.copy()
                elif Exit_df is not None:
                    Exit_df = Exit_df.append(tmp_df, ignore_index=True)

            #if first time processing this step, cache the output and update the spreadsheet
            update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

        #column names to remap
        if step == 'Load Field Names':
            recordType = Steps_df.loc[row,'Field1'].split(':')[1].strip()
            rangeName = Steps_df.loc[row,'Field2'].split(':')[1].strip()
            spreadsheetId=BI_ENGINE_SHEET
            tmp_df, tmp_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadsheetId)
            output_filename = os.path.join('localOutput','field_names_%s.csv'%recordType)
            FieldNameMapping[recordType] = output_filename

            if not os.path.exists(output_filename): tmp_df.to_csv(output_filename, index=False) #cache if not already present

            #if step not already processed, save the data locally in the right file and update the sheet
            update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

        if step == 'MapFields_ObfuscateData': #this step happens every time since it happens in memory
            datatype = Steps_df.loc[row,'Field1'].split(':')[1].strip()
            map_dict = pd.read_csv(FieldNameMapping[datatype])[['Source Field (in WorkDay)','Destination Field (in our model)']].to_dict('list')
            mapper = {}
            #Some of the mappings are fucked up because the names in the source spreadsheet are wrong. They can fix this
            for a,b in zip(map_dict['Source Field (in WorkDay)'], map_dict['Destination Field (in our model)']):
                mapper[a] = b

            if datatype == 'Active':
                Active_df = Active_df.rename(columns=mapper, index=str)
            elif datatype == 'Start':
                Start_df = Start_df.rename(columns=mapper, index=str)
            elif datatype == 'Exit':
                Exit_df = Exit_df.rename(columns=mapper, index=str)

            #skipping the obsfucate data part

            #if step not already processed, mark the sheet to show it has been processed
            update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

        if step == 'CreateNewField_FromMap': #this step happens every time since it happens in memory
            newField = Steps_df.loc[row,'Field1'].split(':')[1].strip()
            rangeName = Steps_df.loc[row,'Field2'].split(':')[1].strip()
            oldField = Steps_df.loc[row,'Field3'].split(':')[1].strip()
            namedRangeColumns = [col.strip() for col in Steps_df.loc[row,'Field4'].split(':')[1].split(',')]

            spreadsheetId=BI_ENGINE_SHEET
            tmp_df, tmp_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadsheetId)
            map_dict = tmp_df[namedRangeColumns].to_dict('list')
            mapper = {}
            for a,b in zip(map_dict[namedRangeColumns[0]], map_dict[namedRangeColumns[1]]):
                mapper[a] = b

            Active_df[newField] = Active_df[oldField].map(mapper)
            Start_df[newField] = Start_df[oldField].map(mapper) 
            Exit_df[newField] = Exit_df[oldField].map(mapper)

            #if step not already processed, mark the sheet to show it has been processed
            update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

        if step == 'CreateNewField_FromExpression': #this step happens every time since it happens in memory
            newField = Steps_df.loc[row,'Field1'].split(':')[1].strip()
            sourceField = Steps_df.loc[row,'Field2'].split(':')[1].strip()

            if newField == 'Year':
                f = lambda x: x.year
                func_to_apply = get_date_attribute
            elif newField == 'Month':
                f = lambda x: x.month
                func_to_apply = get_date_attribute
            elif newField == 'Calendar_Year_Month':
                f = lambda x: '%d-%d'%(x.year,x.month)
                func_to_apply = get_date_attribute

            Active_df[newField] = func_to_apply(Active_df, sourceField, f)
            Start_df[newField] = func_to_apply(Start_df, sourceField, f)
            Exit_df[newField] = func_to_apply(Exit_df, sourceField, f)

            #if step not already processed, mark the sheet to show it has been processed
            update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

        if step=='PopulateNamedRange':

            #only do anything if this step not already processed
            if unicode.find(Steps_df.loc[row,'Processing Status(% complete)'], 'Processed') ==- 1:

                sheet = Steps_df.loc[row,'Field1'].split(':')[1].strip()
                column = Steps_df.loc[row,'Field2'].split(':')[1].strip()
                colname = Steps_df.loc[row,'Field3'].split(':')[1].strip()

                #get unique values and reshape
                val = [[colname]]
                for v in sorted(set(Active_df[colname].dropna().tolist() + Start_df[colname].dropna().tolist() + Exit_df[colname].dropna().tolist())): #remove duplicates
                    val.append([v]) #1 row x N columns

                #parameters for upload to sheet
                rangeName = '%s!%s1:%s%d'%(sheet,column,column,1+len(val))
                body = {'values':val}
                result = service.spreadsheets().values().update(
                    spreadsheetId=spreadsheetId, range=rangeName, valueInputOption='RAW', 
                    body=body).execute()

                #if step not already processed, mark the sheet to show it has been processed
                update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

        if step == 'CreateNewField_FromData': #this step happens every time since it happens in memory
            newField = Steps_df.loc[row,'Field1'].split(':')[1].strip()
            rangeName = Steps_df.loc[row,'Field2'].split(':')[1].strip()
            sourceField = Steps_df.loc[row,'Field3'].split(':')[1].strip()
            namedRangeColumns = [col.strip() for col in Steps_df.loc[row,'Field4'].split(':')[1].split(',')]
            func_to_apply = Steps_df.loc[row,'Field5'].split(':')[1].strip()
            extra_field1 = Steps_df.loc[row,'Field6'].split(':')[1].strip()
            extra_field2 = Steps_df.loc[row,'Field7'].split(':')[1].strip()
            
            if newField == 'Active_Start_Period' or newField == 'Active_End_Period': #not sure what these columns are supposed to mean, skipping
                update_status_skipped(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

            else: #newField == 'XYZ_Period':

                #retrieve ActivityPeriod named range and store as dict
                spreadsheetId=BI_ENGINE_SHEET
                if rangeName != 'None':
                    tmp_df, tmp_range = read_sheet(service, rangeName=rangeName, spreadsheetId=spreadsheetId)

                if func_to_apply == 'None':

                    #######temporary hack
                    if 'PA_ORG_FINANCIAL_LL_UNIT_CODE' in tmp_df.columns: 
                        tmp_df['PA_ORG_FINANCIAL_LL_UNIT_CODE'] = tmp_df['PA_ORG_FINANCIAL_LL_UNIT_CODE'].str.replace('LL','L2')
                    #######temporary hack

                    #map from Calendar_Year_Month to new XYZ_Period
                    print(namedRangeColumns, tmp_df.columns)
                    map_dict = tmp_df[namedRangeColumns].to_dict('list')
                    print('map_dict:',map_dict)
                    mapper = {}
                    for a,b in zip(map_dict[namedRangeColumns[0]], map_dict[namedRangeColumns[1]]):
                        mapper[a] = b

                    print('mapper:',mapper)

                    Active_df[newField] = Active_df[sourceField].map(mapper)
                    Start_df[newField] = Start_df[sourceField].map(mapper)
                    Exit_df[newField] = Exit_df[sourceField].map(mapper)

                    print(Active_df[[sourceField,newField]].head())
                    print(Active_df[newField].unique())

                else: #call the function 
                    if func_to_apply == 'get_tenureGroup':
                        kwargs = {'tenure_df':tmp_df, 'effective_date':sourceField, 'start_date':extra_field1}
                        tmp_df[namedRangeColumns[0]] = tmp_df[namedRangeColumns[0]].astype(float)
                    elif func_to_apply == 'crosstab_concat':
                        kwargs = {'cols':namedRangeColumns, 'sep':extra_field1}

                    func_to_apply = eval(func_to_apply)
                    print('kwargs',kwargs)

                    func_output = func_to_apply(Active_df, **kwargs)
                    Active_df[namedRangeColumns[1]] = func_output
                
                    func_output = func_to_apply(Start_df, **kwargs)
                    Start_df[namedRangeColumns[1]] = func_output

                    func_output = func_to_apply(Exit_df, **kwargs)
                    Exit_df[namedRangeColumns[1]] = func_output

                    print(Active_df[namedRangeColumns[1]].head())

                #if step not already processed, mark the sheet to show it has been processed
                update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

        if step == 'Calc Hierachy': #this should happen once and be cached
            #if hierarchy cache not exist
            #check if hierarchy cache exist

            #calculate if not exist
            grped = Active_df.groupby('PA_Data_Effective_Date')
            for ts, df_ts in grped:
                print('seeking hierarchy from Active_df for timestamp:',ts)
                Active_hier = get_hierarchy(df_ts)
                #print(Active_hier.head())

                #join the hierarchy with the timestamp df; then append the result to Active_dfh, initializing or appending as needed
                if Active_dfh is None:
                    Active_dfh = pd.merge(df_ts, Active_hier, left_on = 'Client_ID',right_index=True, how='left')
                else:
                    Active_dfh = Active_dfh.append(pd.merge(df_ts, Active_hier, left_on = 'Client_ID',right_index=True, how='left'),ignore_index=True)

                print('Active_dfh.shape:', len(Active_dfh), len(Active_dfh.columns))

            #update status
            update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

            #calculate for starts, exits
            Start_dfh = Start_df.copy()
            Exit_dfh = Exit_df.copy()

            #for each row in Start_dfh find the nearest match to min effective date in Active_dfh
            for i in range(len(Start_dfh)):
                cid = Start_dfh.iloc[i]['Client_ID']

                sub_df = Active_dfh[Active_dfh.Client_ID == cid][['PA_Data_Effective_Date', 'PA_Leadership_Level_1', 'PA_Leadership_Level_2', 'PA_Leadership_Level_3', 'PA_Leadership_Level_4', 'PA_Leadership_Level_5', 'PA_Leadership_Level_6', 'PA_Leadership_Level_7']]
                if len(sub_df) ==0: 
                    continue
                #else: print(cid, len(sub_df))
                dates = sub_df['PA_Data_Effective_Date'].apply(pd.to_datetime).values
                amin = np.argmax(dates)

                mindate = sub_df.iloc[amin]['PA_Data_Effective_Date']
                for j in range(1,8):
                    col = 'PA_Leadership_Level_%d'%j
                    Start_dfh.loc[i,col] = Active_dfh[(Active_dfh.PA_Data_Effective_Date==mindate) & (Active_dfh.Client_ID==cid)][col].values[0]

            #for each row in Exit_dfh find the nearest match to min effective date in Active_dfh
            for i in range(len(Exit_dfh)):
                cid = Exit_dfh.iloc[i]['Client_ID']

                sub_df = Active_dfh[Active_dfh.Client_ID == cid][['PA_Data_Effective_Date', 'PA_Leadership_Level_1', 'PA_Leadership_Level_2', 'PA_Leadership_Level_3', 'PA_Leadership_Level_4', 'PA_Leadership_Level_5', 'PA_Leadership_Level_6', 'PA_Leadership_Level_7']]
                if len(sub_df) ==0: 
                    continue
                #else: print(cid, len(sub_df))
                dates = sub_df['PA_Data_Effective_Date'].apply(pd.to_datetime).values
                amin = np.argmin(dates)

                mindate = sub_df.iloc[amin]['PA_Data_Effective_Date']
                for j in range(1,8):
                    col = 'PA_Leadership_Level_%d'%j
                    Exit_dfh.loc[i,col] = Active_dfh[(Active_dfh.PA_Data_Effective_Date==mindate) & (Active_dfh.Client_ID==cid)][col].values[0]

        if step == 'CreateMetricOutput':
            pivot_seg_type = Steps_df.loc[row,'Field1'].split(':')[1].strip() #actual, plan, forecast
            pivot_seg_period = Steps_df.loc[row,'Field2'].split(':')[1].strip()
            pivot_seg_cat = Steps_df.loc[row,'Field3'].split(':')[1].strip() #such as `total`

            #algo is calculated start and end dates of the period, whether year or year-quarter
            #people at start: Client_Date_Official_Job_Current_Job_Start < lower end point
            #people at end: "" < upper end point
            #then find unique people with no exits

            #if pivot_seg_cat == 'Total', the other two flags are not used
            #otherwise the other two flags need to be specified

            #look up segment categories if not pivot_seg_cat != 'Total' and run the function for each segment categories
            if pivot_seg_cat != 'Total':
                segment_names = Active_dfh[pivot_seg_cat].unique()
                tmp_df = metric_output_actual(Active_dfh, Start_dfh, Exit_dfh, pivot_seg_period, pivot_seg_cat, output_columns = output_metrics_columns, segments=segment_names)
                output_metrics_df = output_metrics_df.append( tmp_df, ignore_index=True)
            else:
                tmp_df = metric_output_actual(Active_dfh, Start_dfh, Exit_dfh, pivot_seg_period, pivot_seg_cat=pivot_seg_cat, output_columns = output_metrics_columns)
                output_metrics_df = output_metrics_df.append( tmp_df, ignore_index=True)

            #update sheet if step not already completed
            update_status_processed(service,Steps_df,row,spreadsheetId=BI_ENGINE_SHEET)

    print('output file name', foutname)
    print('EmployeeActiveFiles', EmployeeActiveFiles)
    print('EmployeeStartFiles', EmployeeStartFiles)
    print('EmployeeExitFiles', EmployeeExitFiles)
    print('FieldNameMapping', FieldNameMapping)
    print('Active_df row count: ', len(Active_df))
    print('Start_df row count: ', len(Start_df))
    print('Exit_df row count: ', len(Exit_df))
    print('Output metrics df:\n',output_metrics_df)

    #save active_df as test set for hierarchy calc
    Active_df.to_csv('localOutput/adf.csv',index=False)
    Start_df.to_csv('localOutput/sdf.csv',index=False)
    Exit_df.to_csv('localOutput/edf.csv',index=False)

    if TableauInputFile is not None:
        output_metrics_df.to_csv(TableauInputFile,index=False)
    else:
        output_metrics_df.to_csv('localOutput/output_metrics_df.csv',index=False)

    #print(Active_df[['Client_Management_Level','PA_CUSTOM_JOB_FAMILY','PA_CUSTOM_JOB_LEVEL','PA_CUSTOM_REGION','PA_CROSSTAB_REGION_LOCATION','Client_REGION','PA_CROSSTAB_FIN1_JOBLEVEL','PA_CROSSTAB_FIN1_REGION_JOBLEVEL','PA_CROSSTAB_FIN1_JOBLEVEL_TENURE','PA_CROSSTAB_FIN1_REGION_JOBLEVEL_TENURE']].sort_values('Client_REGION',ascending=True).head(15))

    #creating fields from Data

if __name__ == '__main__':

    t0 = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument("--instruct_sheet", action='store', dest='instruct_sheet', help="name of instruction sheet")
    parser.add_argument("--step_range", action='store', dest='step_range', help="named range of steps to execute, first row must be column headers")
    parser.add_argument("--bi_engine", action='store', dest='bi_engine', help="google sheet defining the BI engine")
    args = parser.parse_args()
    if args.instruct_sheet: 
        INSTRUCTION_SHEET_NAME = args.instruct_sheet
        print('setting instruct_sheet to ', args.instruct_sheet)
    if args.step_range: 
        STEPS_RANGE_NAME = args.step_range
        print('setting step_range to ', args.step_range)
    if args.bi_engine: 
        BI_ENGINE_SHEET = args.bi_engine
        print('setting bi_engine to ', args.bi_engine)

    main()
    t1 = time.time()
    print('execution time: %.2f minutes'%((t1-t0)/60))
