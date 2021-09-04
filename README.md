Documentation

# Preliminary python setup

This script was devleoped in Python 2.  It may or may not work as is with Python 3.

Install these packages:
pandas
numpy (required by Pandas)
google-api-python-client

# Google API Credentials:
Follow instructions here (https://developers.google.com/sheets/api/quickstart/python)
for setting up the Google API w/ python in the directory where this project was git cloned.
You will create a project through the Google Developers Console and setup authentication for 
the project.  At the end of the process, a client_secret.json will be placed in your current 
directory.

Setting this up correctly allows files quickstart.py, quickstart.tw.py, and prototype_v2.py
to run in bash shell with

```
>>> /path_to_python2/python2 [file.py] 
```


# Main Script = prototype_v2.py

usage:
    >>> ./prototype_v2.py  --bi_engine [google spreadsheet ID] --instruct_sheet [instruction sheet] --step_range [instruction steps]
    >>> ./prototype_v2.py  --bi_engine 1TM6LcvN3yf_1zn9XBQwztmVP01Q89u6xZWIMaCzzHA4 --instruct_sheet INSTRUCTIONS_V2 --step_range 'C15:L55'
    [instruction sheet] is the sheet name of the instruction set within the Google sheet
    [instruction steps] are the steps to process; this range MUST include the column headings as row 0; Values can be a named range ('Steps') or a cell range ('C15:L93')
    [google sheet ID] is the identifier for the Google sheet

At start up, the script looks for optional commandline arguments to override the Google 
sheet BI engine, the instruction sheet within the engine, and the name range defining the 
steps to execute.  If none are presented, the script uses values hard coded in the script 
(BI_ENGINE_SHEET, INSTRUCTION_SHEET_NAME, STEPS_RANGE_NAME).

The default BI Enging is outlined in my personal, viewable (but not writable) here:
https://drive.google.com/open?id=1TM6LcvN3yf_1zn9XBQwztmVP01Q89u6xZWIMaCzzHA4

Step handling happens in the main() function.  All steps in STEPS_RANGE_NAME are pulled and 
stored in memory as a Pandas dataframe.  Steps are iterated in order.  If the script 
recognizes the step, it will call the appropriate function using the inputs from the
field names if needed. The step will be skipped if it is not recognized.

Table of StepName in sequential order as appears in Spreadsheet and the Spreadsheet inputs after reworking:

|StepName                     | Field inputs                                   |
| --------------------------- | ---------------------------------------------- |
|**Create Output File**           | Field 1: output Tableau file            |
|                             |                                                |
|**Load File Into Memory**        | Field 1: SpreadsheetId of input                |
| [load active emp,           | Field 2: Startdate                             |
|  starts, exits              | Field 3: Enddate                               |
|  from Google Sheets]        | Field 4: RecordType                            |
|                             | Field 5: Output file name                     |
|                             |                                                |
|**Load Field Names**             | Field 1: RecordType                            |
| [get column names for       | Field 2: NamedRange (for reassigning col names)|
|  remapping]                 |                                                |
|                             |                                                |
|**MapFields_ObfuscateData**      | Field 1: DataType (Active, Start, Exit)       | 
| [remap column names]        |                                               | 
|                             |                                               | 
|**CreateNewField_FromMap**       | Field 1: New field name                       | 
| [creat new column           | Field 2: UseNamedRange                        | 
|  according to inputs]       | Field 3: Old field name to remap              | 
|                             | Field 4: comma-delimited column titles in UseNamedRange |
|                             |                                               | 
|**CreateNewField_FromExpression**| Field 1: one of Year, Month, or Calendar_Year_Month| 
| [different lambda function      | Field 2: Source field name to operate on |
|  logic for each option          |                                          | 
|  in Field 1]                    |                                          | 
|                             |                                              | 
|**PopulateNamedRange**       |  Field 1: Sheet name to write to             | 
|                             |  Field 2: Column in Field 1 sheet to write to | 
|                             |  Field 3: Dataframe column to process and upload|
|                             |                                               | 
|**CreateNewField_FromData**  | Field 1: new field to create                |
| [different logic for each   | Field 2: UsedNameRange                      |
|  possible Field 1]          | Field 3: field to map from in creating new columns    |
|                             | Field 4: columns in named range (Field 2)             |
|                             | Field 5: optional python function to apply 
|                             |      (currently supported: get_tenureGroup,)           |
|                             |      (crosstab_concat)                                |
|                             | Field 6: optional parameter to pass to function       |
|                             | Field 7: optional parameter to pass to function       |
|                             |                                                       |
|**Calc Hierarchy**             | Field 1, Field 2, etc. not used                       |
|                             |                                                       |
|**CreateMetricOutput**           | Field 1: PIVOT_SEG_TYPE (e.g., Actual)                |
| [creates all metrics        | Field 2: PIVOT_SEG_PERIOD (Year, Year-Quarter)    |
|  for PIVOT_SEG_CAT, minus   | Field 3: PIVOT_SEG_CAT (column to aggregate over) |
|  planning, forecasting]     |                                                   |

Loading Active/Start/Exit data from Google Sheet is slow (~minutes).  THe `Load File Into Memory` step always first checks if the data is cached locally on disk. If so, it is read into
memory.  If no, it is pulled from the Google Sheet and cached so that subsequent runs
are faster.  

All steps from `Load Field Names` onward are repeated in full every time. The final output
are the metrics stored in the specified csv filename in the localOutput directory
(e.g., localOutput/TableauInput.txt)

On Feb 19, the runtime to create all output metrics by all segments and time periods (Year, XYZ_Period), as listed in range C15:L93,  was 41 minutes (25% CPU usage, <175 MB memory usage.)

