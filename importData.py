# -*- coding: utf-8 -*-
"""
Created on Sat Oct  3 11:22:19 2020

@author: Steven
"""

import io

import requests

import pandas as pd

import pickle

import numpy as np

import ssl

from datetime import date

import datetime

import shutil

# This handles ssl certificates of urls we are downloading data from.

ssl._create_default_https_context = ssl._create_unverified_context





# Save saves Object as filename.pkl in "Pickle files"

def Save(Object, filename):
    
    file = open( 'Pickle files/' + filename + ".pkl", "wb")
    
    pickle.dump(Object, file)

    file.close()
    

# Open opens filename.pkl from "Pickle files"

def Open(filename):

    file = open( 'Pickle files/' + filename + ".pkl", "rb")

    return pickle.load(file)    

# The function mergeData does an outer merge of all the data sets, filling in
# with missing value wherever appropriate.

def mergeFrames(df1, df2):

    df = pd.merge(df1, df2, how='outer')

    # Sort by date

    df= df.sort_values('Date')
    
    # Make sure everything that isn't the date is stored as a float
    
    df.iloc[:, 1:] = df.iloc[:, 1:].astype(float)
    
    # Make the row index equal to row number
    
    df.index = np.arange( len(df) )
    
    return df



# Stacks data for different potentially overlapping time periods

def stackData(old, new, precedence):
    
    # endDate is where the old data will get cut off, and the new data will start
    
    if precedence == 'old':
        
        endDate = old['Date'].dropna().max()
    
        new = new[  new['Date'] > endDate  ]
    
    elif precedence == 'new':
        
        endDate = new['Date'].dropna().min()
    
        # Cut off old at the date where new starts
        
        old = old[  old['Date'] < endDate ]
        
        # Stacks old and new vertically
        
    df = pd.concat( [ old, new ], axis=0)
    
    # Make the row index equal to row number
    
    df.index = np.arange( len(df) )
    
    return df
    


# importMonthlyHosp imports monthly hospital data, puts it into a 
# more useful format and saves it.

def importMonthlyHosp():
    
    # This url contains a link to hospital admissions data.

    url = "https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2021/01/Covid-Publication-14-01-2021.xlsx"
    
    oldHospAd = pd.read_excel(url, sheet_name='Admissions Total')
    
    # Drop everything except rows 11 and 12, and all columns from 5 onwards.
    # Row 11 has the date
    # Row 12 has the number of hospital admissions
    # The actual numbers start from column 5 onwards
    
    oldHospAd  = oldHospAd .iloc[11:13, 5:   ].T
    
    # This takes the columns whose values are pandas timestamps, and makes the 
    # column labels the corresponding date.
    
    oldHospAd .columns = ['Date', 'Daily hospital admissions with Covid-19 England']
    
    # Make the row index equal to row number
    
    oldHospAd .index = np.arange( len(oldHospAd ) )
    
    # Save the dataframe as a pickle object
    
    Save(oldHospAd , 'oldHospAd')
    
    
    
    # import and format the monthlyBedsOcc data
    
    monthlyBedsOcc = pd.read_excel (url, sheet_name='Total Beds Occupied').T
       
    # Keep rows and columns of interest 
   
    monthlyBedsOcc = monthlyBedsOcc.iloc[2:, 11:]
    
    monthlyBedsOcc = monthlyBedsOcc.drop( np.arange(12,22), axis = 1 )
    
    monthlyBedsOcc.columns = monthlyBedsOcc.loc['Unnamed: 3',]
    
    # Create a dataframe that is a hospital region lookup table
    
    hospMeta = monthlyBedsOcc.iloc[0, 1:].T
    
    Save(hospMeta, 'hospMeta')
    
    

    # Drop some more rows
    
    monthlyBedsOcc = monthlyBedsOcc.iloc[3:, ]
    
    monthlyBedsOcc = monthlyBedsOcc.rename(columns={"Code": "Date"})
    
    # For reasons unknown, the December 2020 data spuriously starts in March
    # with zeroes. Drop any of that data
    
    drop = monthlyBedsOcc.index[  ~(monthlyBedsOcc['Date'] >= pd.Timestamp(2020, 4, 2, 0))
                                & ~(monthlyBedsOcc['Date'].isna())  ]
    
    monthlyBedsOcc = monthlyBedsOcc.drop( drop, axis=0  )
    
    # Make the row index equal to row number
    
    monthlyBedsOcc.index = np.arange( len(monthlyBedsOcc) )
    
    # Save the dataframe as a pickle object
    
    Save(monthlyBedsOcc, 'monthlyBedsOcc')
    
    
    
    # Import and format the monthlyBedsOccCovid data
    
    monthlyBedsOccCovid = pd.read_excel(url, sheet_name='Total Beds Occupied Covid').T
    
    # Keep rows and columns of interest 
    
    monthlyBedsOccCovid = monthlyBedsOccCovid.iloc[2:, 11:]
    
    monthlyBedsOccCovid = monthlyBedsOccCovid.drop( np.arange(12,22), axis = 1 )
    
    monthlyBedsOccCovid.columns = monthlyBedsOccCovid.loc['Unnamed: 3',]
    
    monthlyBedsOccCovid = monthlyBedsOccCovid.iloc[3:, ]
    
    
    monthlyBedsOccCovid = monthlyBedsOccCovid.rename(columns={"Code": "Date"})
    
    # Make the row index equal to row number
    
    monthlyBedsOccCovid.index = np.arange( len(monthlyBedsOccCovid) )
    
    # Save the dataframe as a pickle object
    
    Save(monthlyBedsOccCovid, 'monthlyBedsOccCovid')
    
    

    # Import data on mechanical ventilations beds
    
    monthlyMVbedsOcc = pd.read_excel (url, sheet_name='MV Beds Occupied')
    
    monthlyMVbedsOcc = monthlyMVbedsOcc.iloc[[11,12], 5:].T
    
    monthlyMVbedsOcc.columns = ['Date', 'Mechanical ventilation beds occupied England']
    
     # Make the row index equal to row number
    
    monthlyMVbedsOcc.index = np.arange( len(monthlyMVbedsOcc) )
    
    # Save the dataframe as a pickle object
        
    Save(monthlyMVbedsOcc, 'monthlyMVbedsOcc')
        
 
   
    monthlyMVbedsOccCovid = pd.read_excel (url, sheet_name='MV Beds Occupied Covid-19')
    
    monthlyMVbedsOccCovid = monthlyMVbedsOccCovid.iloc[[11,12], 5:].T
    
    monthlyMVbedsOccCovid.columns = ['Date', 'Mechanical ventilation beds occupied Covid-19 England']
    
     # Make the row index equal to row number
    
    monthlyMVbedsOccCovid.index = np.arange( len(monthlyMVbedsOccCovid) )
    
     # Save the dataframe as a pickle object
    
    Save(monthlyMVbedsOccCovid, 'monthlyMVbedsOccCovid')
    
    
    
    
    admissionsDict = { '0-5': 'Admissions 0-5',
                 '6-17':   'Admissions 6-17',
                 '18-64': 'Admissions 18-64',
                 '65-84': 'Admissions 65-84',
                 '85+': 'Admissions 85+'}
    
    admissionsByAge= pd.DataFrame(columns = ['Date'])
    
    for sheet in admissionsDict:
    
        admissions = pd.read_excel (url, sheet_name=admissionsDict[sheet] ).T
        
        admissions= admissions.iloc[5:, 11:13]
        
        admissions.columns = ['Date', 'Daily hospital admissions with Covid-19 England age ' + sheet]
        
        admissionsByAge = pd.merge(admissionsByAge, admissions, how = 'outer')
    
    
    admissionsByAge.index = np.arange( len(admissionsByAge) )
    
    # Save the dataframe as a pickle object
    
    Save(admissionsByAge, 'admissionsByAge')
    
    
    return





# importWeeklyHosp imports the dailt Hospital data, puts it into 
# a more useful format and saves it.


def importWeeklyHosp():
    
    url = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2021/02/Weekly-covid-admissions-and-beds-publication-210204-1.xlsx'
    
    # Import and format the weeklyGABedsOccCovid data
    
    weeklyGABedsOccCovid = pd.read_excel(url, sheet_name='Adult G&A Beds Occupied COVID').T

    
    # Keep rows and columns of interest 
    
    weeklyGABedsOccCovid= weeklyGABedsOccCovid.iloc[2:, 13:]
    
    weeklyGABedsOccCovid = weeklyGABedsOccCovid.drop( np.arange(14,23), axis = 1 )
    
    weeklyGABedsOccCovid.columns = weeklyGABedsOccCovid.loc['Unnamed: 2',]
    
    weeklyGABedsOccCovid = weeklyGABedsOccCovid.iloc[2:, ]
    
    
    weeklyGABedsOccCovid= weeklyGABedsOccCovid.rename(columns={"Code": "Date"})
    
    # Make the row index equal to row number
    
    weeklyGABedsOccCovid.index = np.arange( len(weeklyGABedsOccCovid) )
    
    # Save the dataframe as a pickle object
    
    Save(weeklyGABedsOccCovid, 'weeklyGABedsOccCovid')
    
    
    # Import and format the weeklyBedsOcc data
    
    weeklyGABedsOccNonCovid = pd.read_excel(url, sheet_name='Adult G&A Bed Occupied NonCOVID').T

    
    # Keep rows and columns of interest 
    
    weeklyGABedsOccNonCovid= weeklyGABedsOccNonCovid.iloc[2:, 13:]
    
    weeklyGABedsOccNonCovid = weeklyGABedsOccNonCovid.drop( np.arange(14,23), axis = 1 )
    
    weeklyGABedsOccNonCovid.columns = weeklyGABedsOccNonCovid.loc['Unnamed: 2',]
    
    weeklyGABedsOccNonCovid = weeklyGABedsOccNonCovid.iloc[2:, ]
    
    
    weeklyGABedsOccNonCovid= weeklyGABedsOccNonCovid.rename(columns={"Code": "Date"})
    
    # Make the row index equal to row number
    
    weeklyGABedsOccNonCovid.index = np.arange( len(weeklyGABedsOccNonCovid) )
    
    # Save the dataframe as a pickle object
    
    Save(weeklyGABedsOccNonCovid, 'weeklyGABedsOccNonCovid')
    
    
    
   # Import and format the weeklyBedsOccCovid data
    
    weeklyBedsOccCovid = pd.read_excel(url, sheet_name='All beds COVID').T

    # Keep rows and columns of interest 
    
    weeklyBedsOccCovid = weeklyBedsOccCovid.iloc[2:, 13:]
    
    weeklyBedsOccCovid = weeklyBedsOccCovid.drop( np.arange(14,23), axis = 1 )
    
    weeklyBedsOccCovid = weeklyBedsOccCovid.dropna(axis=1, how='all')
    
    weeklyBedsOccCovid.columns = weeklyBedsOccCovid.loc['Unnamed: 2',]
    
    weeklyBedsOccCovid = weeklyBedsOccCovid.iloc[2:, ]
    
    weeklyBedsOccCovid = weeklyBedsOccCovid.rename(columns={"Code": "Date"})
    
    # Make the row index equal to row number
    
    weeklyBedsOccCovid.index = np.arange( len(weeklyBedsOccCovid) )
    
    # Save the dataframe as a pickle object
    
    Save(weeklyBedsOccCovid, 'weeklyBedsOccCovid')
    
    return





# importDailyHosp imports the dailt Hospital data, puts it into 
# a more useful format and saves it.

def importDailyHosp():
    
    # Get yesterday's date, because it is used in the PHE url
    
    yesterday = date.today() - datetime.timedelta(1)
    
    dateStr = str(yesterday).replace("-", "")
    
    # Create url of hospital admissions data
    
    url = ('https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/'  + str(yesterday.year) + "/" + str('{:02}'.format(yesterday.month)) + '/' "COVID-19-daily-admissions-and-beds-" + dateStr + '.xlsx')
       
    df = pd.read_excel(url)   
    
    
    
    # Pick out relevant rows and columns for admissions

    admissions = df.iloc[11:13, 2:   ].T
    
    # Get rid of any NaN/NaT rows, which happened once for some reason
    
    admissions = admissions.dropna(axis=0)
    
    # Rename columns appropriately
    
    admissions.columns = ['Date', 'Daily hospital admissions plus hospital diagnoses with Covid-19 England']
    
    # Make the row index equal to row number
    
    admissions.index = np.arange( len(admissions) )
    
    # Save the dataframe as a pickle object
    
    # Save dataframe as a pickle object.
    
    Save(admissions, 'newHospAd')
    
    
    
    # Pick out relevant rows and columns for mechanical ventilator beds (MVB)

    dailyMVbedsOccCovid = df.iloc[102:104, 2:   ].T
    
    # Rename columns appropriately
    
    dailyMVbedsOccCovid.columns = ['Date', 'Mechanical ventilation beds occupied Covid-19 England']
    
     # Make the row index equal to row number
    
    dailyMVbedsOccCovid.index = np.arange( len(dailyMVbedsOccCovid) )
    
    # Save the dataframe as a pickle object
    
    Save(dailyMVbedsOccCovid, 'dailyMVbedsOccCovid')
    
    
    
    # Pick out beds occupid with covid patients data
    
    dailyBedsOccCovid = df.iloc[87:89, 2:   ].T
    
    # Rename columns appropriately
    
    dailyBedsOccCovid.columns = ['Date', 'Hospital beds occupied Covid-19 England']
    
     # Make the row index equal to row number
    
    dailyBedsOccCovid.index = np.arange( len(dailyBedsOccCovid) )
    
    # Save the dataframe as a pickle object
    
    Save(dailyBedsOccCovid, 'dailyBedsOccCovid')
    
    
    return
    

# importMort imports the mortality data, puts it into a more useful format
# and saves it.


def importMort():
    
    # This url contains link to frequently updated data
    
    url = 'https://www.mortality.org/Public/STMF/Outputs/stmf.csv'
    
    # Import is using np.array because there are some weird formatting issues
    # if you try with pandas dataframe
    
    array = np.genfromtxt(url, delimiter = ',', dtype =str)
    
    # Importing mortality.org data automatically creates a new folder for some
    # reason. Delete it.
    
    shutil.rmtree('www.mortality.org')
    
    # Now stick it into a dataframe
    
    df = pd.DataFrame(data = array[1:, :] , columns = array[0, :])
    
    #  EW is England and Wales, NIR is Northern Ireland, SC is Scotland
    
    dfEW = df[ df['CountryCode']  == 'GBRTENW'  ]
    
    dfNI = df[ df['CountryCode']  == 'GBR_NIR'  ]
    
    dfSC = df[ df['CountryCode']  == 'GBR_SCO'  ]
    
    
    def formatMort(df):
    
        df.insert(0, 'Date', pd.to_datetime(df.Year.astype(str), format='%Y') + pd.to_timedelta(  (df.Week.astype(int)).mul(7).astype(str) + ' days') )
    
        # Keep only entries that have 'b' in the 'Sex' field. 'b' stands for 'both,
        # and it means that both male and female deaths are counted
    
        df = df[ df['Sex'] == 'b' ][ ['Date', 'DTotal'] ]
        
        # Right now everything is strings. Coerce to integers.
        
        df.iloc[:, 1] = df.iloc[:, 1].astype(int)
        
        return df
    
    
    dfEW = formatMort(dfEW)
    
    dfNI = formatMort(dfNI)
    
    dfSC = formatMort(dfSC)
    
    # Annoyingly, dfNI has one entry missing at start of 2015. Interpolate it.
    
    NIentry = pd.DataFrame(  [ [ datetime.datetime(2015, 1, 1), dfNI.iloc[0,1] ] ] , columns = ['Date', 'DTotal']  )
    
    dfNI = pd.concat( [NIentry, dfNI] )
    
    
    # Data for different countries of UK start and end in different places.
    # startDate is the latest time at which data for a country begins
    # endDate is the earliest time at which data for a country stops.
    
    startDate = max( dfEW.iloc[0,0],  dfNI.iloc[0,0] , dfSC.iloc[0,0] )
    
    endDate = min(  dfEW.iloc[-1,0],  dfNI.iloc[-1,0] , dfSC.iloc[-1,0]  )
    
    # prune removes data records whose data is before startDate, or after endDate.
    
    def prune(df, startDate, endDate):
        
        df = df[ (df['Date'] >= startDate) &  (df['Date'] <= endDate)  ]
        
        # Make the row index equal to row number
    
        df.index = np.arange( len(df) )
        
        return df
         
    # Prune EW, NI and SC data, then add together into dfUK 
        
    dfEW = prune(dfEW, startDate, endDate)
    
    dfNI = prune(dfNI, startDate, endDate)
    
    dfSC = prune(dfSC, startDate, endDate)
    
    
    dfUK = dfEW
    
    dfUK.iloc[:, 1] = dfEW.iloc[:, 1] + dfNI.iloc[:, 1] + dfSC.iloc[:, 1]
    
    # Rename Total column to Deaths. 
    
    dfUK = dfUK.rename(columns={"DTotal": "Weekly deaths UK"} )

    # Save the dataframe as a pickle object
    
    Save(dfUK, 'Mort')
    
    return
    



# importGDP imports the GDP data, puts it into a more useful format
# and saves it.

# There is an annoying issue with March 2017, which has 
# 2017MAR in the date column rather than 2017 MAR, so that has to be 
# fixed manually.

def importGDP():
    
    url = 'https://www.ons.gov.uk/generator?uri=/economy/grossdomesticproductgdp/bulletins/gdpmonthlyestimateuk/november2020/7e8704cf&format=csv'
    
    
    r = requests.get(url)
    
    df = pd.read_csv(io.StringIO(r.text))
  
    # Rename columns appropriately
    
    df.columns = ['Date', 'Monthly GDP index UK'] 
    
    # The actual numbers start from row 6 onwards, so drop everything before that.
    
    df = df.iloc[6:, :]
    
     # Rename to remove fourth letters cos theyre fucking idiots
    
    df['Date'] = df['Date'].str.replace('June', 'Jun')
    
    df['Date'] = df['Date'].str.replace('July', 'Jul')
    
    df['Date'] = df['Date'].str.replace('Sept', 'Sep')
    
    # Convert the Date entries to a proper timedate
    
    df['Date'] = pd.to_datetime( df.Date.astype(str), format= '%b %y')
       
    # Make the row index equal to row number
    
    df.index = np.arange( len(df) )
    
    
    # Start off with Dates from 2010 in the first column, and weekly deaths
    # for 2010 in the second column.
    
    yearlyGDP = df[ df['Date'].dt.year == 2007 ]
    
    # Rename columns appropriately.
    
    yearlyGDP = yearlyGDP.rename(columns={"Monthly GDP index UK": "Monthly GDP index UK 2007"} )
    

    # Add columns for weekly deaths for years 2011-2019


    for year in range(2008, 2020):
        
    
        yearlyGDP['Monthly GDP index UK ' + str(year)] = df[  df['Date'].dt.year == year ]['Monthly GDP index UK'].values
    
    
    
    GDP2020 = df[  df['Date'].dt.year == 2020 ]['Monthly GDP index UK'].values


    endGDP2020 = np.empty(12- len(GDP2020)  )

    endGDP2020[:] = np.nan

    GDP2020 = np.concatenate( [GDP2020, endGDP2020] )

    yearlyGDP['Monthly GDP index UK 2020'] = GDP2020
    
    # Make all non-date entries floats.
    
    yearlyGDP.iloc[0:, 1:] = yearlyGDP.iloc[0:, 1:].astype(float)
   
    # Save the dataframe as a pickle object
    
    Save(yearlyGDP, 'yearlyGDP')
    
    return
 

    
# importOWID imports the OWID data, puts it into a more useful format
# and saves it.
  

def importOWID():
    
    # This url contains link to daily updated data
    
    url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
    
    # Read in from above url
    
    df = pd.read_csv(url)
    
    # Select all records from the UK
    
    df = df.loc[ df['location'] == 'United Kingdom']
    
    # Select only those columns that are of interest
    
    df = df[  ['date', 'positive_rate'] ]
    
    # Convert date column to timedates

    df['date'] = pd.to_datetime( df.date.astype(str), format= '%Y-%m-%d')
    
    # Rename 'date' to 'Date', just for consistency
    
    df.columns = ['Date', 'Positive test rate UK'  ]
    
    # Make the row index equal to row number
    
    df.index = np.arange( len(df) )
    
    # Save the dataframe as a pickle object
    
    Save(df, 'OWID')
    
    return
    

    
# importUC imports the Universal credit claims data, puts it into a more useful format
# and saves it.  

def importUC():
    
    df = pd.read_excel(r'Data/UC claims.xlsx')
    
    # Row 5 contains dates, and row 7 contains number of claims
    # The last column contains totals, which I don't want
    # The data only starts from column 2 onwards
    # Get rid of everything else.
    
    df = df.iloc[ [5,7], 2:-1  ].T
    
    # Rename columns appropriately
    
    df.columns = [ 'Date', 'Weekly universal credit claims UK'  ]
    
    # Some of the dates randomly have the string ' (r)' added at the end
    # so get rid of that.
    
    df['Date'] = df['Date'].str.rstrip(' (r)')
    
    # Turn the date column into a timedate object
    
    df['Date'] = pd.to_datetime( df.Date.astype(str), format= '%B %d, %Y')

    # Make the row index equal to row number
    
    df.index = np.arange( len(df) )
  
    # Save the dataframe as a pickle object
    
    Save(df, 'UC')
    
    return



# importIandP imports influenza + pneumonia mortality data, puts it into a more 
# useful format and saves it.


def importIandP():
    
    df = pd.read_excel(r'Data/Influenza and pneumonia deaths.xlsx', sheet_name='Table 5')
    
    # Data starts in row 4, and ends in row 123
    # Only the first and second columns are of interest.
    # They are the data, and the total number of influenza + pneumonia deaths
    # Get rid of everything else
    
    df = df.iloc[4:123, :2]
    
    # Rename columns
    
    df.columns = [ 'Date', 'Yearly influenza and pneumonia deaths England and Wales'  ] 
    
    # Convert years into timedates
    
    df['Date'] = pd.to_datetime( df.Date.astype(str), format= '%Y')
    
    # Make the row index equal to row number
    
    df.index = np.arange( len(df) )
    
  
    # Save the dataframe as a pickle object
    
    Save(df, 'IandP')
    
    return
    
    
# importLCD imports leading cause of death data, puts it into a more 
# useful format and saves it.


def importLCD():
    
    # LCDM is leading cause of death for males
    # LCDF is leading cause of death for females
    
    LCDM = pd.read_excel (r'Data/LCD male.xlsx', sheet_name='Table 5')
    
    LCDF = pd.read_excel (r'Data/LCD female.xlsx', sheet_name='Table 5')

    # ACtual data starts in row 14. Discard everything else, and transpose
    
    LCDM = LCDM.iloc[14:, :].T

    LCDF = LCDF.iloc[14:, :].T

    # Initialise df so it has same shape and columns as LCDM
    
    df = LCDM
    
    # Make the non-date entries equal to the sum of male and female deaths
    
    df.iloc[:, 1:] = LCDM.iloc[1:, 1:] + LCDF.iloc[1:, 1:]
    
    # Give columns appropriate names
    
    df.columns = LCDF.iloc[0]
    
    # Drop the 0th row. It just contains column names
    
    df.index = np.arange( len(df) )
    
    df = df.drop([0])
    
    # Make the row index equal to row number

    df.index = np.arange( len(df) )
    
    # Rename the 'Leading cause' column to 'Date'.
    
    df = df.rename( columns = {"Leading cause ": "Date" } )
    
    # Entries in the Date column correspond to year, but not all of them are
    # integeres. This coerces them to integers
    
    df['Date'] = df['Date'].astype(int)
    
    # Convert the Data column to datetime objects
    
    df['Date'] = pd.to_datetime( df.Date.astype(str), format= '%Y')
    
    # The index column has a name that is unnecessary, delete to keep things neat
    
    df.columns.name = None
    
    # Save the dataframe as a pickle object
 
    Save(df, 'LCD')
    
    return
   


# importdeathByAge imports death by age data, puts it into a more 
# useful format and saves it.


def importdeathByAge():
    
    df = pd.read_excel(r'Data/Deaths by age.xlsx')
    
    # Data starts in row 6

    df = df.iloc[2:, [2,4, 6]]
    
    df.columns = ['Date', 'Age group', 'Deaths']
    
    
    df['Date'] = pd.to_datetime( df['Date'].map(lambda x: x[15:]), format = '%d-%b-%y')
    # Label columns appropriately
    
    deathByAge = df.groupby(['Age group']).sum()
    
    deathByAge['Age group'] = deathByAge.index
    
    deathByAge['Age group'] = deathByAge['Age group'].replace(to_replace = '-', value =" to ", regex = True)
    
    # Make the row index equal to row number
    
    deathByAge.index = np.arange( len(deathByAge) )
    
     # Save the dataframe as a pickle object
 
    Save(deathByAge, 'deathByAge')
    
    return


# importGovSpending imports government spending data, puts it into a more 
# useful format and saves it.


def importGovSpending():
    
    # import CJRS and SEIS data
    
    CJRS = pd.read_excel(r'Data/CJRS.xls')

    SEIS = pd.read_excel(r'Data/SEIS.xls')
    
    businessLoans = pd.read_excel(r'Data/Business loans.xls')
    
    VAT = pd.read_excel(r'Data/Vat deferral.xls')
    
    # Merge dataframes

    df = pd.merge(CJRS, SEIS, how='outer')
    
    df = pd.merge(df, businessLoans, how = 'outer')
    
    df = pd.merge(df, VAT, how = 'outer')
    
    # Sort by date
    
    df = df.sort_values(by='Date')
    
    # Make the row index equal to row number
    
    df.index = np.arange( len(df) )
    
    
    # Fill in the NaNs where possible
    
    for index in df.index:
        
        df.loc[index, 'Total value of claims made CJRS'] = df.iloc[ :(index+1) ,3].max()
    
        df.loc[index, 'Total value of claims made SEIS'] = df.iloc[ :(index+1) ,5].max()
        
        df.loc[index, 'Value of facilities approved CBILS'] = df.iloc[ :(index+1) ,6].max()
        
        df.loc[index, 'Value of facilities approved CBLILS'] = df.iloc[ :(index+1) ,8].max()
        
        df.loc[index, 'Value of facilities approved BBLS'] = df.iloc[ :(index+1) ,10].max()
        
        df.loc[index, 'Value of convertible loans approved FF'] = df.iloc[ :(index+1) ,14].max()
        
        df.loc[index, 'Total value of VAT deferred'] = df.iloc[ :(index+1) ,18].max()
     
    # Add a columns that has total value of CJRS and SEIS claims
        
    df['Total value of CJRS and SEIS claims'] = df['Total value of claims made CJRS'] + df['Total value of claims made SEIS']
    
    df['Total value of approved business loans'] = df['Value of facilities approved CBILS'] + df['Value of facilities approved CBLILS'] \
        + df['Value of facilities approved BBLS'] + df['Value of convertible loans approved FF'] + df['Total value of VAT deferred']
        
    df.insert(22, 'Total NHS spending UK 2018-2019'  , 152900000000  )
    
     # Save the dataframe as a pickle object
 
    Save(df, 'govSpending')
    
    return



# importBeds imports NHS bed data, puts it into a more useful format and saves it.



def importAvgBedsOcc():
    
    #dfON is overnight beds
    
    df = pd.read_excel(r'Data/Avg bed occupancy.xls', sheet_name = 'Open Overnight')
    
    # df is day only beds
    
    #df = pd.read_excel(r'Data/Avg bed occupancy.xls', sheet_name = 'Open Day Only')

    # Pick out the data that is of interest
    
    df = df.iloc[13:54, [1, 2, 4, 10 ]]
    
    #df = df.iloc[13:54, [1, 2, 4, 10 ]]
    
   # Add the overnight and day only entries together
    
   # df.iloc[:, 2:] = dfON.iloc[:, 2:] + df.iloc[:, 2:]
    
    # Rename columns more appropriately
    
    df.columns = ['Year', 'Quarter', 'Total available', 'Total occupied'  ]
    
    # Make the row index equal to row number
    
    df.index = np.arange( len(df) )
    
    # Create columns for the dataframe that we want
     
    columns = ['Date']
        
    for year in  range(2010, 2021):   
        columns.append( 'Quarterly mean overnight NHS beds available England ' + str(year) )
        columns.append(  'Quarterly mean overnight NHS beds occupied England ' + str(year) )
        
     # beds is the dataframe we want   
        
    beds = pd.DataFrame(columns = columns)
     
    # Make the first columns of beds equal to the dates of the year. 
    
    beds['Date'] = pd.date_range(start = '2020-01-01', end = '2020-12-31', freq='D')
    
    # dayRange returns the range of days that are for each quarter in 2020
    
    def dayRange(quarter):
        if quarter == 1:
            return [0, 91]
        elif quarter == 2:
            return [91, 182]
        elif quarter ==3:
            return [182, 274]
        elif quarter == 4:
            return [274, 366]
        
    # bedsMeat will form the 'meat' of the beds dataframe. Initialise it.    
        
    bedsMeat = np.empty( [366, 22]  ) 
    
    bedsMeat[:] = np.nan
    
    # Fill bedsMeat with quarterly averages.

    for index in df.index:
        
        year = int(df.iloc[index, 0][0:4])
        
        quarter = int(df.iloc[index, 1][1:])
        
        
        bedsMeat[ dayRange(quarter)[0] : dayRange(quarter)[1],  (year-2010)*2  ] = df.iloc[index, 2]
        
        bedsMeat[ dayRange(quarter)[0] : dayRange(quarter)[1],  (year-2010)*2 +1 ] = df.iloc[index, 3]
    
    # put the meat into beds
        
    beds.iloc[:, 1:] = bedsMeat
    
    
    # Save the dataframe as a pickle object
 
    Save(beds, 'avgBedsOcc')
    
    return



# Import historic daily bed occupied and available data, and format.


def importHistBedsOcc():
    
    bedsOpen = pd.read_excel(r'Data/Bed occupancy.xlsx', sheet_name = 'G&A_Beds_Open_crosstab').T

    
    # Make columns equal to hospital code
    
    bedsOpen.columns = bedsOpen.loc['Unnamed: 2', :]
    
    # keep rows of interest
    
    bedsOpen = bedsOpen.iloc[3:, :]
    
    # Insert Date column

    bedsOpen.insert(loc=0, column='Date', value= bedsOpen.index  )
    
  
    

    # Make the row index equal to row number
    
    bedsOpen.index = np.arange( len(bedsOpen) )
    
    # Save the dataframe as a pickle object
 
    Save(bedsOpen, 'histBedsOpen')
    
    

    
    bedsOcc = pd.read_excel(r'Data/Bed occupancy.xlsx', sheet_name = 'G&A_Beds_Occupied_crosstab').T

    
    # Make columns equal to hospital code
    
    bedsOcc.columns = bedsOcc.loc['Unnamed: 2', :]
    
    # keep rows of interest
    
    bedsOcc = bedsOcc.iloc[3:, :]
    
    # Insert Date column

    bedsOcc.insert(loc=0, column='Date', value= bedsOcc.index  )
    

    # Make the row index equal to row number
    
    bedsOcc.index = np.arange( len(bedsOcc) )
    
    # Save the dataframe as a pickle object
 
    Save(bedsOcc, 'histBedsOcc')
    
    return




# createYearlyMort creates a dataframe of year by year mortality, and saves it.

def createYearlyMort(Mort, deaths):
    
    Mort = Open('Mort')
    
    # Start off with Dates from 2015 in the first column, and weekly deaths
    # for 2015 in the second column.
    
    yearlyMort = Mort[ Mort['Date'].dt.year == 2015 ][['Date', 'Weekly deaths UK']]
    
    
    # Rename columns appropriately.
    
    yearlyMort = yearlyMort.rename(columns={"Weekly deaths UK": "Weekly deaths UK 2015"} )
    
    # Make the row index equal to row number
    
    yearlyMort.index = np.arange( len(yearlyMort.index) )

    # Add columns for weekly deaths for years 2016-2019



    for year in range(2016, 2020):
                    
        yearlyMort['Weekly deaths UK ' + str(year)] = Mort[  Mort['Date'].dt.year == year ]['Weekly deaths UK'].values[-52:]
    
    # Add column for weekly deaths in 2020
    # This isn't so easy because 2020 isn't finished yet, so need to create
    # an array that has apropriate NaN's concatenated at the end

    Mort2020 = Mort[  Mort['Date'].dt.year == 2020 ]['Weekly deaths UK'].values


    endMort2020 = np.empty(52- len(Mort2020)  )

    endMort2020[:] = np.nan

    Mort2020 = np.concatenate( [Mort2020, endMort2020] )

    yearlyMort['Weekly deaths UK 2020'] = Mort2020
    
    # Add a column that has weekly mean deaths for 2015-2019

    yearlyMort['Mean weekly deaths UK 2015-2019'] = yearlyMort.iloc[:, 1:5].mean(axis=1)
    
    
    
    
    # Initialise a dataframe
    
    deaths2020 = pd.DataFrame()
     
    # Make the first columns of deaths2020 equal to the dates of the year. 
    
    deaths2020['Date'] = pd.date_range(start = '2020-01-01', end = '2020-12-31', freq='D')
    
    # Merge deaths and deaths2020
    
    deaths2020 = pd.merge(deaths2020, deaths, how='outer' )
    
    
  
    # noWeeks is the number of weeks for which we have weekly deaths in 2020
    
    noWeeks = 52 - yearlyMort['Weekly deaths UK 2020'].isna().sum()
    
    
    weeklyCoronaDeaths = np.empty(52)
    
    weeklyCoronaDeaths[:] = np.nan
   
    # Calculate weeklyCoronaDeaths by summing in groups of 7
  
    for week in range(noWeeks):    
        
        weeklyCoronaDeaths[ week ] = deaths2020.iloc[ 7*week : (7*week)+6, 1 ].sum()
        
        
       
    # Add a column to yearlyMort that is mean deaths + Covid-19 deaths
    
    yearlyMort['Mean weekly deaths 2015-2019<br>plus Covid-19 deaths UK 2020'] = \
              weeklyCoronaDeaths + yearlyMort['Mean weekly deaths UK 2015-2019']
                
                
    # Save the dataframe as a pickle object
    
    Save(yearlyMort, 'yearlyMort')
    
    return 




def importUnemployment():
    
    url = 'https://www.ons.gov.uk/generator?format=csv&uri=/employmentandlabourmarket/peoplenotinwork/unemployment/timeseries/mgsx/lms'
    
    r = requests.get(url)
    
    df = pd.read_csv(io.StringIO(r.text))
    
    # Unemployment by month starts in row 255
    
    df = df.iloc[255:, :]
    
    # Rename columns appropriately
    
    df.columns = ['Date', 'Unemployment rate (seasonally adjusted)']
    
    # Make unemployment a number between zero and one.
    
    df['Unemployment rate (seasonally adjusted)'] = df['Unemployment rate (seasonally adjusted)'].astype(float)/100
    
    # Turns date column into proper datetime objects
 
    df['Date'] =   pd.to_datetime(df.Date.astype(str), format='%Y %b')
    
    # Make the row index equal to row number
    
    df.index = np.arange( len(df) )
    
    # Save the dataframe as a pickle object
    
    Save(df, 'Unemployment')
    
    return



def importONS():
    
    url = 'https://coronavirus.data.gov.uk/api/v1/data?filters=areaType=overview&structure=%7B%22areaType%22:%22areaType%22,%22areaName%22:%22areaName%22,%22areaCode%22:%22areaCode%22,%22date%22:%22date%22,%22newDeaths28DaysByDeathDate%22:%22newDeaths28DaysByDeathDate%22,%22cumDeaths28DaysByDeathDate%22:%22cumDeaths28DaysByDeathDate%22%7D&format=csv'
    
    
    deaths = pd.read_csv(url)
    
    deaths = deaths.iloc[:, 3:6]
    
    deaths.columns = ['Date', 'Daily Covid-19 deaths UK', 'Cumulative deaths']
    
    deaths['Date'] =  pd.to_datetime( deaths.Date, format = '%Y-%m-%d'  )
    
    # Save the dataframe as a pickle object
    
    Save(deaths, 'deaths')
    
    
    
    url = 'https://coronavirus.data.gov.uk/api/v1/data?filters=areaName=United%2520Kingdom;areaType=overview&structure=%7B%22areaType%22:%22areaType%22,%22areaName%22:%22areaName%22,%22areaCode%22:%22areaCode%22,%22date%22:%22date%22,%22newVirusTests%22:%22newVirusTests%22,%22cumVirusTests%22:%22cumVirusTests%22%7D&format=csv'
    
    tests = pd.read_csv(url)
    
    tests = tests.iloc[:, 3:6]
    
    tests.columns = ['Date', 'Daily tests UK', 'Cumulative tests UK' ]

    tests['Date'] =  pd.to_datetime( tests.Date, format = '%Y-%m-%d'  )
    
    # Save the dataframe as a pickle object
    
    Save(tests, 'tests')
    

    
    url = 'https://api.coronavirus.data.gov.uk/v1/data?filters=areaType=overview&structure=%7B%22areaType%22:%22areaType%22,%22areaName%22:%22areaName%22,%22areaCode%22:%22areaCode%22,%22date%22:%22date%22,%22newCasesBySpecimenDate%22:%22newCasesBySpecimenDate%22,%22cumCasesBySpecimenDate%22:%22cumCasesBySpecimenDate%22%7D&format=csv'

    cases = pd.read_csv(url)

    cases = cases.iloc[:, 3:5]

    cases.columns = ['Date', 'Daily new Covid-19 cases UK']
    
    cases['Date'] =  pd.to_datetime( cases.Date, format = '%Y-%m-%d'  )

    # Save the dataframe as a pickle object
    
    Save(cases, 'cases')
    
    
    
    
    
    url = 'https://api.coronavirus.data.gov.uk/v1/data?filters=areaName=United%2520Kingdom;areaType=overview&structure=%7B%22areaType%22:%22areaType%22,%22areaName%22:%22areaName%22,%22areaCode%22:%22areaCode%22,%22date%22:%22date%22,%22newOnsDeathsByRegistrationDate%22:%22newOnsDeathsByRegistrationDate%22,%22cumOnsDeathsByRegistrationDate%22:%22cumOnsDeathsByRegistrationDate%22%7D&format=csv'
    
    deathsCert = pd.read_csv(url)
    
    # Picks out relevant columns
    
    deathsCert = deathsCert.iloc[:, 3:5]
    
    # Rename columns appropriately

    deathsCert.columns = ['Date', 'Weekly deaths with Covid-19 on death certificate']
    
    # Date column of deathsCert is not a datetime, so convert it
    
    deathsCert['Date'] = pd.to_datetime(  deathsCert.Date, format = '%Y-%m-%d'  ) 
    
   
    # deathsComp will allow comparison of the two different ways of counting deaths
    
    deathComp = pd.merge(deathsCert, deaths[['Date', 'Cumulative deaths']] , how = 'outer')

    deathComp['Weekly deaths within 28 days of a positive test'] = np.nan
    
    # Clculate first difference of cumulative deaths
    
    for index in range( len(deathComp)-1 ):
        
        deathComp.iloc[index, 3] = deathComp.iloc[index, 2] - deathComp.iloc[index + 1, 2]

    

    # Drop any entries with na in the 'Weekly deaths with Covid-19 on death certificate' column

    deathComp = deathComp[deathComp['Weekly deaths with Covid-19 on death certificate'].notna()] 
    
    
    
    # Save the dataframe as a pickle object
    
    Save(deathComp, 'deathComp')
    
   
    
  
    return



def importRed():
    
    url = 'https://www.ons.gov.uk/generator?uri=/employmentandlabourmarket/peopleinwork/employmentandemployeetypes/bulletins/uklabourmarket/december2020/934f3335&format=csv'

    r = requests.get(url)
    
    redundancies = pd.read_csv(io.StringIO(r.text))

    
    # keep relevant rows

    redundancies = redundancies.iloc[6:, :]
    
    #rename appropriately

    redundancies.columns = ['Date', 'Redundancies in last 3 months']

    # Convert date column to a datetime object

    redundancies['Date'] = pd.to_datetime( redundancies['Date'].str[4:] )
    
    # Redundancies measured in 1000s.

    redundancies['Redundancies in last 3 months'] = redundancies['Redundancies in last 3 months'].astype(float) *1000

    # Make the row index equal to row number
    
    redundancies.index = np.arange( len(redundancies) )
    
    # Save the dataframe as a pickle object
    
    Save(redundancies, 'Redundancies')
    
    return




def importJSA():
    
    JSA = pd.read_excel(r'Data/Claimants.xlsx')
    
    # Keep relevant rows and columns
    
    JSA = JSA.iloc[10:96, 1:3]
    
    # Rename columns appropriately
    
    JSA.columns = ['Date', 'Number of JSA claimants']
    
    # Convert date column to datetime object
    
    JSA['Date'] = pd.to_datetime( JSA.Date.astype(str), format= '%B %Y')
    
    # Make the row index equal to row number
    
    JSA.index = np.arange( len(JSA) )
    
    # Save the dataframe as a pickle object
    
    Save(JSA, 'JSA')
    
    return
    

def importClaimants():
    
    url = 'https://www.ons.gov.uk/generator?uri=/employmentandlabourmarket/peopleinwork/employmentandemployeetypes/bulletins/employmentintheuk/december2020/e5def0fc&format=csv'

    r = requests.get(url)
    
    claimants = pd.read_csv(io.StringIO(r.text))

    # Keep relevant rows

    claimants = claimants.iloc[6:, :]
    
    # Rename columns appropriately

    claimants.columns = ['Date', 'Claimant count, seasonally adjusted']
    
    # Make Date column a datetime object
    
    claimants['Date'] = pd.to_datetime( claimants.Date.astype(str), format= '%B %Y')
    
    # Claimant count is measured in thousands
    
    claimants['Claimant count, seasonally adjusted'] = claimants['Claimant count, seasonally adjusted'].astype(float) *1000
    
    # Make the row index equal to row number
    
    claimants.index = np.arange( len(claimants) )
    
    # Save the dataframe as a pickle object
    
    Save(claimants, 'claimants')
    
    return
    
    

def importPathways():
    
    
    
    url = 'https://files.digital.nhs.uk/E0/27538C/NHS%20Pathways%20Covid-19%20data%202021-01-21.csv'


    calls = pd.read_csv(url)

    calls = calls[[ 'Call Date', 'TriageCount']]
    
    calls['Call Date'] = pd.to_datetime( calls['Call Date'], format = '%d/%m/%Y'   )

    calls = calls.groupby(['Call Date']).sum()


    url = 'https://files.digital.nhs.uk/51/5E2425/111%20Online%20Covid-19%20data_2021-01-21.csv'
    

    online = pd.read_csv(url)

    online = online[[ 'journeydate', 'Total']]
    
    online['journeydate'] = pd.to_datetime( online['journeydate'], format = '%d/%m/%Y'   )

    online = online.groupby(['journeydate']).sum()


    pathways = pd.concat( [calls, online], axis=1)
    
    
    pathways['Date'] = pathways.index

    # Make the row index equal to row number
    
    pathways.index = np.arange( len(pathways) )
    
    pathways = pathways.rename(columns={"TriageCount": "Daily potential Covid-19 telephone triages England", \
                                        "Total": "Daily potential Covid-19 online assessments England"})

    # Save the dataframe as a pickle object
    
    Save(pathways, 'pathways')

    return




def importSurveilance():
    
   url = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/954730/Weekly_Influenza_and_COVID19_report_data_W3_V2.xlsx'
    
   ICU = pd.read_excel(url, sheet_name = 'Figure 42. SARIWatch-ICUPHEC')
   
   
   start =  ICU.index[ ICU['Unnamed: 2'].str[:12] == '(a) COVID-19' ] [0] + 2
   
   end = ICU.index[ ICU['Unnamed: 2'].str[:13] == '(b) Influenza' ] [0] - 2
   
   ICU.iloc[7,1] = 'Date'
   
   ICU.columns = ICU.iloc[7, :]
   
   ICU = ICU.iloc[start:end, 1:]
    
   ICU.loc[:34,  'Date'] =  pd.Timestamp(2020, 1, 1) + pd.to_timedelta(  (ICU.loc[:34,  'Date'].astype(int)).mul(7).astype(str) + ' days') 
   
   ICU.loc[35:, 'Date'] =  pd.Timestamp(2021, 1, 1) + pd.to_timedelta(  (ICU.loc[35:, 'Date'].astype(int)).mul(7).astype(str) + ' days')

   Save(ICU, 'ICU') 
   
   return
  
    

     
def importDepression():
    
    
    depression = pd.read_excel(r'Data/Depression.xlsx', sheet_name = 'Table 1')

    rows = [9,10,11,20,21,24,25,28,29 ]
    
    columns = [0,6,15,24]

    depression = depression.iloc[rows, columns]    
    
    depression.columns = ['Group', 'November 2020', 'June 2020', 'July 2019 to March 2020' ]
    
    depression =  pd.melt( depression, depression.columns[0], depression.columns[1:]  )
    
    depression = depression.rename({'variable': 'Time period', 'value': 'Prevalence'}, axis=1)
    
    depression['Time period'] = 'Prevalence of moderate to severe depression symptoms ' \
         + depression['Time period'] + ' Britain'
         
    depression['Prevalence'] = depression['Prevalence'].astype(float) 
    
    Save(depression, 'depression') 
    
    return
    
    
    
    
    
    
    
