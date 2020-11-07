# -*- coding: utf-8 -*-
"""
Created on Sat Oct  3 11:22:19 2020

@author: Steven
"""


import pandas as pd

import pickle

import numpy as np


import math

import ssl

from datetime import date

import datetime

import shutil

# This handles ssl certificates of urls we are downloading data from.

ssl._create_default_https_context = ssl._create_unverified_context





# Save saves Object as filename.pkl in the working directory

def Save(Object, filename):
    
    file = open( 'Pickle files/' + filename + ".pkl", "wb")
    
    pickle.dump(Object, file)

    file.close()
    

# Open opens filename.pkl from the working directory

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




# importHospAd imports the old Hospital admissions data that records admissions
# with Covid-19, as well as the old beds occupied data, a puts them into a 
# more useful format and saves them.

def importOldHosp():
    
    # This url contains a link to hospital admissions data.

    
    url = "https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2020/10/Covid-Publication-08-10-2020v3.xlsx"
    
    df = pd.read_excel(url, sheet_name='Admissions Total')
    
    # Drop everything except rows 11 and 12, and all columns from 5 onwards.
    # Row 11 has the date
    # Row 12 has the number of hospital admissions
    # The actual numbers start from column 5 onwards
    
    df = df.iloc[11:13, 5:   ].T
    
    # This takes the columns whose values are pandas timestamps, and makes the 
    # column labels the corresponding date.
    
    df.columns = ['Date', 'Daily hospital admissions with Covid-19 England']
    
    # Make the row index equal to row number
    
    df.index = np.arange( len(df) )
    
    # Save the dataframe as a pickle object
    
    Save(df, 'oldHospAd')
    
    
    # Import and format the oldBedsOccCovid data
    
    oldBedsOccCovid = pd.read_excel (url, sheet_name='Total Beds Occupied Covid')
    
    oldBedsOccCovid = oldBedsOccCovid.iloc[[11,12], 5:].T
    
    oldBedsOccCovid.columns = ['Date', 'NHS beds occupied Covid-19 England 2020']
    
    
    # Make the row index equal to row number
    
    oldBedsOccCovid.index = np.arange( len(oldBedsOccCovid) )
    
    # Save the dataframe as a pickle object
    
    Save(oldBedsOccCovid, 'oldBedsOccCovid')
    
    return



#importNewBedsOcc imports the new total beds occupied data, puts it into
# a better format and saves it.


def importNewBedsOcc():
    
    # This url contains a link to hospital admissions data.
    # It looks like this url is going to change each month, so 
    # it needs updated by hand each time
    
    url = "https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2020/10/Covid-Publication-08-10-2020v3.xlsx"
    
    
    newBedsOcc = pd.read_excel (url, sheet_name='Total Beds Occupied')
    
    newBedsOcc = newBedsOcc.iloc[[11,12], 5:].T
    
    newBedsOcc.columns = ['Date', 'Total NHS beds occupied England 2020']
    
    # Make the row index equal to row number
    
    newBedsOcc.index = np.arange( len(newBedsOcc) )
    
    # Save the dataframe as a pickle object
    
    Save(newBedsOcc, 'newBedsOcc')
    
    return 







# importHospAd2 imports the Hospital admissions data that records admissions
# plus diagnoses with Covid-19, and the new bed occupied data, puts them into 
# a more useful format and saves it.



def importNewHosp():
    
    # Get yesterday's date, because it is used in the PHE url
    
    yesterday = date.today() - datetime.timedelta(1)
    
    dateStr = str(yesterday).replace("-", "")
    
    # Create url of hospital admissions data
    
    url = ('https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/'  + str(yesterday.year) + "/" + str(yesterday.month) + '/' "COVID-19-daily-admissions-" + dateStr + '.xlsx')
    
    url = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2020/11/COVID-19-daily-admissions-and-beds-20201106-1.xlsx'
    
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
    
    # Pick out relevant rows and columns for mechanical ventilator beds (MVB)

    MVB = df.iloc[102:104, 2:   ].T
    
    # Rename columns appropriately
    
    MVB.columns = ['Date', 'Mechanical ventilation beds occupied by patients with Covid-19 England']
    
     # Make the row index equal to row number
    
    MVB.index = np.arange( len(MVB) )
    
    # Merge admissions and MVB
    
    newHosp = pd.merge(admissions, MVB, how = 'outer')
    
    # Save dataframe as a pickle object.
    
    Save(newHosp, 'newHosp')
    
    # Pick out beds occupid with covid patients data
    
    newBedsOccCovid = df.iloc[87:89, 2:   ].T
    
    # Rename columns appropriately
    
    newBedsOccCovid.columns = ['Date', 'NHS beds occupied Covid-19 England 2020']
    
     # Make the row index equal to row number
    
    newBedsOccCovid.index = np.arange( len(newBedsOccCovid) )
    
    # Save the dataframe as a pickle object
    
    Save(newBedsOccCovid, 'newBedsOccCovid')
    
    
    return
    

# importMort imports the mortality data, puts it into a more useful format
# and saves it.


def importMort():
    
    # This url contains link to frequently updated data
    
    url = 'https://www.mortality.org/Public/STMF/Outputs/stmf.csv'
    
    # Import is using np.array because there are some weird formatting issues
    # if you try with pandas dataframe
    
    array = np.genfromtxt(url, delimiter = ',', dtype =str)
    
    # Importing mortalit.org data automatically creates a new folder for some
    # reason. Delete it.
    
    shutil.rmtree('www.mortality.org')
    
    # Now stick it into a dataframe
    
    df = pd.DataFrame(data = array[1:, :] , columns = array[0, :])
    
    #  EW is England and Wales, NIR is Northern Ireland, SC is Scotland
    
    dfEW = df[ df['CountryCode']  == 'GBRTENW'  ]
    
    dfNI = df[ df['CountryCode']  == 'GBR_NIR'  ]
    
    dfSC = df[ df['CountryCode']  == 'GBR_SCO'  ]
    
    
    def formatMort(df):
    
        # Index the dataframe by timedates.
        # To conver a year and a week into a timedate object, first convert just the
        # year to a timedate. Then calculate how many days pass from the start of the year
        # to the start of the numbered week. Convert that to a timedelta, and add onto
        # the timedate for the year
    
        df.insert(0, 'Date', pd.to_datetime(df.Year.astype(str), format='%Y') + pd.to_timedelta(  (df.Week.astype(int)-1).mul(7).astype(str) + ' days') )
        
        # Keep only entries that have 'b' in the 'Sex' field. 'b' stands for 'both,
        # and it means that both male and female deaths are counted
    
        df = df.loc[ df['Sex'] == 'b'][ ['Date', 'DTotal'] ]
        
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
    
    
    df = pd.read_csv(r'Data/GDP Index.csv')
    
    # Rename columns appropriately
    
    df.columns = ['Date', 'Monthly GDP index UK'] 
    
    # The actual numbers start from row 6 onwards, so drop everything before that.
    
    df = df.iloc[6:, :]
    
    # Convert the Date entries to a proper timedate
    
    df['Date'] = pd.to_datetime( df.Date.astype(str), format= '%Y %b')
       
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
    
    df = df[  ['date', 'new_cases', 'new_deaths', 'new_tests', 'positive_rate'] ]
    
    # Convert date column to timedates

    df['date'] = pd.to_datetime( df.date.astype(str), format= '%Y-%m-%d')
    
    # Rename 'date' to 'Date', just for consistency
    
    df.columns = ['Date', 'Daily positive tests UK', 'Daily Covid-19 deaths UK', 'Daily tests UK', 'Positive test rate UK'  ]
    
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
    
    df = pd.read_excel(r'Data/Deaths by age.xls')
    
    # Data starts in row 6

    df = df.iloc[6:, :]
    
    # Label columns appropriately
    
    df.columns = ['Age', 'Male', 'Female']
    
    # Everything is strings. Makes relevant entries integers
    
    df.iloc[:, 1:] = df.iloc[:, 1:].astype(int)
    
    # Make a new column that adds together male and female deaths
    
    df['Deaths'] = df['Male'] + df['Female']
    
    #Reorder cos they are arse pieces
    
    df = df.iloc[::-1]
    
    # Make the row index equal to row number
    
    df.index = np.arange( len(df) )
    
     # Save the dataframe as a pickle object
 
    Save(df, 'deathByAge')
    
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



def importOldBedsOcc():
    
    #dfON is overnight beds
    
    dfON = pd.read_excel(r'Data/Bed occupancy.xls', sheet_name = 'Open Overnight')
    
    # df is day only beds
    
    df = pd.read_excel(r'Data/Bed occupancy.xls', sheet_name = 'Open Day Only')

    # Pick out the data that is of interest
    
    dfON = dfON.iloc[13:54, [1, 2, 4, 10 ]]
    
    df = df.iloc[13:54, [1, 2, 4, 10 ]]
    
   # Add the overnight and day only entries together
    
    df.iloc[:, 2:] = dfON.iloc[:, 2:] + df.iloc[:, 2:]
    
    # Rename columns more appropriately
    
    df.columns = ['Year', 'Quarter', 'Total available', 'Total occupied'  ]
    
    # Make the row index equal to row number
    
    df.index = np.arange( len(df) )
    
    # Create columns for the dataframe that we want
     
    columns = ['Date']
        
    for year in  range(2010, 2021):   
        columns.append( 'Quarterly mean NHS beds available England ' + str(year) )
        columns.append(  'Quarterly mean NHS beds occupied England ' + str(year) )
        
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
 
    Save(beds, 'oldBedsOcc')
    
    return







# createYearlyMort creates a dataframe of year by year mortality, and saves it.

def createYearlyMort(Mort, OWID):
    
    # Start off with Dates from 2015 in the first column, and weekly deaths
    # for 2015 in the second column.
    
    yearlyMort = Mort[ Mort['Date'].dt.year == 2015 ][['Date', 'Weekly deaths UK']]
    
    # Annoyingly, 2015 has only 51 weeks. 
    
    # Rename columns appropriately.
    
    yearlyMort = yearlyMort.rename(columns={"Weekly deaths UK": "Weekly deaths UK 2015"} )
    
    # Make the row index equal to row number
    
    yearlyMort.index = np.arange( len(yearlyMort.index) )

    # Add columns for weekly deaths for years 2011-2019


    for year in range(2016, 2020):
        
        yearlyMort['Weekly deaths UK ' + str(year)] = Mort[  Mort['Date'].dt.year == year ]['Weekly deaths UK'].values
    
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
    
    
    
    # Isolate Covid-19 deaths in 2020
    
    OWIDDeaths2020 = OWID[ OWID.Date.dt.year ==2020 ][ 'Daily Covid-19 deaths UK' ]
    
    # Make the row index equal to row number
    
    OWIDDeaths2020.index = np.arange( len(OWIDDeaths2020) )
    
    # These are useful for finding weekly Covid-19 deaths
    
    lastWeekFloor = math.floor( len(OWIDDeaths2020)/7 ) 
    
    lastWeekCeil = math.ceil( len(OWIDDeaths2020)/7 ) 
    
    # Initialise weeklyCoronaDeaths
    
    weeklyCoronaDeaths = np.empty( lastWeekCeil )
    
    # Calculate weeklyCoronaDeaths by summing in groups of 7
    # Have to take care of the fact that number of days may not be divisible by
    # 7
    
    for week in range(lastWeekFloor ):
        
        weeklyCoronaDeaths[ week ] = OWIDDeaths2020.iloc[ 7*week : (7*week)+6 ].sum()
        
    if lastWeekCeil > lastWeekFloor:
        
        weeklyCoronaDeaths[ lastWeekFloor ] = OWIDDeaths2020.iloc[ 7*lastWeekFloor:  ].sum()
       
    # This fills out the rest of weeklyCoronaDeaths so it has 52 entries, one
    # for each week. If the data for that week doesn't exist yet, it gets a NaN.
       
    endOWID2020 = np.empty(52- len(weeklyCoronaDeaths)  )

    endOWID2020[:] = np.nan  
    
    weeklyCoronaDeaths = np.concatenate( [weeklyCoronaDeaths, endOWID2020] )
    
    # Add a column to yearlyMort that is mean deaths + Covid-19 deaths
    
    yearlyMort['Mean weekly deaths 2015-2019<br>plus Covid-19 deaths UK 2020'] = \
                weeklyCoronaDeaths + yearlyMort['Mean weekly deaths UK 2015-2019']
    
    # Save the dataframe as a pickle object
    
    Save(yearlyMort, 'yearlyMort')
    
    return 


# Comebine all the bed occupancy availability, covid-19 etc data into one
# dataframe



def combineBeds(oldBedsOcc, newBedsOcc, oldBedsOccCovid, newBedsOccCovid):


    df = pd.merge(oldBedsOcc, newBedsOcc, how='outer')

    # endDate is where the oldBedsOccCovid data will get cut off, and newBedsOccCovid
    # data will start

    endDate = newBedsOccCovid.iloc[0,0]
    
    # Cut off oldBedsOcc at the date where newBedsOccCovid starts
    
    oldBedsOccCovid = oldBedsOccCovid[  oldBedsOccCovid['Date'] < endDate ]
    
    # Stacks oldBedsOccCovid and oldBedsOccCovid vertically
        
    bedsOccCovid = pd.concat( [ oldBedsOccCovid, newBedsOccCovid ], axis=0)
        
    # Merge df with bedsOccCovid
    
    df = pd.merge(df, bedsOccCovid, how='outer')
      
     
    df['NHS beds occupied non-Covid-19 England 2020'] = df['Total NHS beds occupied England 2020']  - df['NHS beds occupied Covid-19 England 2020']
    
    # Make all entries the same type
    
    df.iloc[:, 1:] = df.iloc[:, 1:].astype(np.float)
    
    # Save the dataframe as a pickle object
    
    Save(df, 'beds')
    
    return











