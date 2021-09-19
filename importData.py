###################################################################### 

## Code author: Steven Kerr

## Description: 
# This code imports all the data

###################################################################### 

import io
import requests
import pandas as pd
import pickle
import numpy as np
import ssl
from datetime import date
import datetime
import os

os.chdir('C:/Users/Steven/Desktop/Git/Covid_stats' )

# This handles ssl certificates of urls we are downloading data from.
ssl._create_default_https_context = ssl._create_unverified_context

####################### GENERAL FUNCTIONS ###############################

# Save saves Object as filename.pkl in "Pickle files"
def Save(Object, filename):
    
    file = open( 'Pickle files/' + filename + ".pkl", "wb")
    
    pickle.dump(Object, file)

    file.close()
    

# Open opens filename.pkl from "Pickle files"
def Open(filename):

    file = open( 'Pickle files/' + filename + ".pkl", "rb")

    return pickle.load(file)    

# mergeFrames does an outer merge of all the data sets, filling in
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
     
    # Get rid of duplicates    
    old = old.loc[:,~old.columns.duplicated()]   
    new = new.loc[:,~new.columns.duplicated()]  
     
    # Stacks old and new vertically
    df = pd.concat( [ old, new ], axis=0)

    df.index = np.arange( len(df) )
    
    return df

# format_1, format_2 and format_3 are used repeatedly to format data pulled
# from different sheets of excel files

def format_1(df, col_name):
    
    df = df.iloc[11:13, 5:].T
    
    df.columns = ['Date', col_name]
    
    df.index = np.arange(len(df))
    
    return df


def format_2(df):
    
    df = df.iloc[2:, 11:]
    
    df = df.drop(np.arange(12,22), axis = 1 )
    
    df.columns = df.loc['Unnamed: 2',]
    
    df = df.iloc[2:, ]
        
    df = df.rename(columns={"Code": "Date"})
    
    df.index = np.arange(len(df))
    
    return df


def format_3(df):
    
    df = df.iloc[2:, 13:]
    
    df = df.drop( np.arange(14,23), axis = 1 )
    
    df.columns = df.loc['Unnamed: 2',]
    
    df = df.iloc[2:, ]
    
    df= df.rename(columns={"Code": "Date"})
    
    return df

################################ IMPORTS ########################################


def importMonthlyHosp():
    
    # newer data.
    url = "https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2021/09/Covid-Publication-09-09-2021.xlsx"
    
    
    newHospAd = pd.read_excel(url, sheet_name='Admissions Total')
    
    newHospAd = format_1(newHospAd, 'Daily hospital admissions with Covid-19 England')

    Save(newHospAd , 'newHospAd')
    
    
    # import and format the monthlyBedsOcc data
    monthlyBedsOcc = pd.read_excel(url, sheet_name='Total Beds Occupied').T

    monthlyBedsOcc = monthlyBedsOcc.iloc[1:, 11:]
    
    monthlyBedsOcc = monthlyBedsOcc.drop( np.arange(12,22), axis = 1 )
    
    monthlyBedsOcc.columns = monthlyBedsOcc.loc['Unnamed: 2',]
    
    # There is a column with name nan in one month - drop it
    monthlyBedsOcc = monthlyBedsOcc.loc[:, monthlyBedsOcc.columns.notnull()]
    
    # Create a dataframe that is a hospital region lookup table
    hospMeta = monthlyBedsOcc.iloc[0, 1:].T
    
    Save(hospMeta, 'hospMeta')
    
    
    monthlyBedsOcc = monthlyBedsOcc.iloc[3:, ]
    
    monthlyBedsOcc = monthlyBedsOcc.rename(columns={"Code": "Date"})
     
    monthlyBedsOcc.index = np.arange( len(monthlyBedsOcc) )

    Save(monthlyBedsOcc, 'monthlyBedsOcc')
    
     
    monthlyBedsOccCovid = pd.read_excel(url, sheet_name='Total Beds Occupied Covid').T
    
    monthlyBedsOccCovid = format_2(monthlyBedsOccCovid)
    
    monthlyMVbedsOccNew = pd.read_excel(url, sheet_name='MV Beds Occupied')
    
    monthlyMVbedsOccNew = format_1(monthlyMVbedsOccNew, 'Mechanical ventilation beds occupied England')
    
      
    monthlyMVbedsOccCovidNew = pd.read_excel (url, sheet_name='MV Beds Occupied Covid-19')
    
    monthlyMVbedsOccNew = format_1(monthlyMVbedsOccCovidNew, 'Mechanical ventilation beds occupied Covid-19 England')
    
    
    # Create addmissions by age dataframe from newer data
    admissionsDict = { '0-5': 'Admissions 0-5',
                 '6-17':   'Admissions 6-17',
                 '18-64': 'Admissions 18-64',
                 '65-84': 'Admissions 65-84',
                 '85+': 'Admissions 85+'}
    
    admissionsByAgeNew= pd.DataFrame(columns = ['Date'])
    
    for sheet in admissionsDict:
    
        admissionsNew = pd.read_excel (url, sheet_name=admissionsDict[sheet] ).T
        
        admissionsNew= admissionsNew.iloc[5:, 11:13]
        
        admissionsNew.columns = ['Date', 'Daily hospital admissions with Covid-19 England age ' + sheet]
        
        admissionsByAgeNew = pd.merge(admissionsByAgeNew, admissionsNew, how = 'outer')
    
    admissionsByAgeNew.index = np.arange( len(admissionsByAgeNew) )
  
  
    '''
    # older data
    # It appears this doesn't ever get updated
    url = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2021/05/Covid-Publication-06-04-2021.xlsx'
    
    oldHospAd = pd.read_excel(url, sheet_name='Admissions Total')
    
    
    oldHospAd = format_1(oldHospAd, 'Daily hospital admissions with Covid-19 England')
    
    Save(oldHospAd , 'oldHospAd')
    
    
    monthlyMVbedsOccOld = pd.read_excel (url, sheet_name='MV Beds Occupied')
    
    monthlyMVbedsOccOld = format_1(monthlyMVbedsOccOld, 'Mechanical ventilation beds occupied England')
    
  
    monthlyBedsOccCovidOld = pd.read_excel(url, sheet_name='Total Beds Occupied Covid').T
    
    monthlyBedsOccCovidOld = format_2(monthlyBedsOccCovidOld)
    
         
    monthlyMVbedsOccCovidOld  = pd.read_excel (url, sheet_name='MV Beds Occupied Covid-19')
    
    monthlyMVbedsOccCovidOld = format_1(monthlyMVbedsOccCovidOld, 'Mechanical ventilation beds occupied Covid-19 England' )
    

    monthlyBedsOccCovid = stackData(monthlyBedsOccCovidOld, monthlyBedsOccCovid, 'old')
    
    Save(monthlyBedsOccCovid, 'monthlyBedsOccCovid')
    

    monthlyMVbedsOcc = stackData(monthlyMVbedsOccOld, monthlyMVbedsOccNew, 'old')
    
    Save(monthlyMVbedsOcc, 'monthlyMVbedsOcc')
    
    
    monthlyMVbedsOccCovid = stackData(monthlyMVbedsOccCovidOld, monthlyMVbedsOccCovidNew, 'old')
    
    Save(monthlyMVbedsOccCovid, 'monthlyMVbedsOccCovid')
    
   
    # Create admissions by age dataframe from newer data
    admissionsByAgeOld= pd.DataFrame(columns = ['Date'])
    
    for sheet in admissionsDict:
    
        admissionsOld = pd.read_excel (url, sheet_name=admissionsDict[sheet] ).T
        
        admissionsOld= admissionsOld.iloc[5:, 11:13]
        
        admissionsOld.columns = ['Date', 'Daily hospital admissions with Covid-19 England age ' + sheet]
        
        admissionsByAgeOld = pd.merge(admissionsByAgeOld, admissionsOld, how = 'outer')
    
    admissionsByAgeOld.index = np.arange( len(admissionsByAgeOld) )
   
    
    admissionsByAge = stackData(admissionsByAgeOld, admissionsByAgeNew, 'old')
    
    Save(admissionsByAge, 'admissionsByAge')
    '''
    
    return


def importWeeklyHosp():
    
    # older data
    url = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2021/04/Weekly-covid-admissions-and-beds-publication-210429-up-to-210406.xlsx'

    
    weeklyGABedsOccCovidOld = pd.read_excel(url, sheet_name='Adult G&A Beds Occupied COVID').T

    weeklyGABedsOccCovidOld = format_3(weeklyGABedsOccCovidOld)
  
      
    weeklyGABedsOccNonCovidOld = pd.read_excel(url, sheet_name='Adult G&A Bed Occupied NonCOVID').T

    weeklyGABedsOccNonCovidOld = format_3(weeklyGABedsOccNonCovidOld)
    

    weeklyBedsOccCovidOld = pd.read_excel(url, sheet_name='All beds COVID').T

    weeklyBedsOccCovidOld = format_3(weeklyBedsOccCovidOld)
    

    weeklyBedsUnoccupiedOld = pd.read_excel(url, sheet_name='Adult G&A Beds Unoccupied').T
    
    weeklyBedsUnoccupiedOld = format_3(weeklyBedsUnoccupiedOld)

       
    #newer data
    url = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2021/09/Weekly-covid-admissions-and-beds-publication-210916.xlsx'

    
    weeklyGABedsOccCovidNew = pd.read_excel(url, sheet_name='Adult G&A Beds Occupied COVID').T

    weeklyGABedsOccCovidNew = format_3(weeklyGABedsOccCovidNew)
  
    
    weeklyGABedsOccNonCovidNew = pd.read_excel(url, sheet_name='Adult G&A Bed Occupied NonCOVID').T

    weeklyGABedsOccNonCovidNew = format_3(weeklyGABedsOccNonCovidNew)
    

    weeklyBedsOccCovidNew = pd.read_excel(url, sheet_name='All beds COVID').T

    weeklyBedsOccCovidNew = format_3(weeklyBedsOccCovidNew)
 

    weeklyBedsUnoccupiedNew = pd.read_excel(url, sheet_name='Adult G&A Beds Unoccupied').T
    
    weeklyBedsUnoccupiedNew = format_3(weeklyBedsUnoccupiedNew)  



    weeklyGABedsOccCovid = stackData (weeklyGABedsOccCovidOld, weeklyGABedsOccCovidNew, 'old')

    weeklyGABedsOccCovid['Date'] = weeklyGABedsOccCovid['Date'].apply(pd.to_datetime)

    weeklyGABedsOccNonCovid = stackData (weeklyGABedsOccNonCovidOld, weeklyGABedsOccNonCovidNew, 'old')

    weeklyGABedsOccNonCovid['Date'] = weeklyGABedsOccNonCovid['Date'].apply(pd.to_datetime)

    weeklyBedsOccCovid = stackData (weeklyBedsOccCovidOld, weeklyBedsOccCovidNew, 'old')

    weeklyBedsOccCovid['Date'] = weeklyBedsOccCovid['Date'].apply(pd.to_datetime)

    weeklyBedsUnoccupied = stackData(weeklyBedsUnoccupiedOld, weeklyBedsUnoccupiedNew, 'old')

    weeklyBedsUnoccupied['Date'] = weeklyBedsUnoccupied['Date'].apply(pd.to_datetime)


    # add elementwise
    weeklyBedsOpen = weeklyGABedsOccCovid.copy()
    
    weeklyBedsOpen.iloc[:, 1:] = weeklyBedsOpen.iloc[:, 1:].add(weeklyGABedsOccNonCovid.iloc[:, 1:], fill_value=0) 
    
    weeklyBedsOpen.iloc[:, 1:] = weeklyBedsOpen.iloc[:, 1:].add(weeklyBedsUnoccupied.iloc[:, 1:], fill_value= 0)
    

    Save(weeklyGABedsOccCovid, 'weeklyGABedsOccCovid')
    
    Save(weeklyGABedsOccNonCovid, 'weeklyGABedsOccNonCovid')
    
    Save(weeklyBedsOccCovid, 'weeklyBedsOccCovid')
    
    Save(weeklyBedsOpen, 'weeklyBedsOpen')
  
    return


def importDailyHosp():
    
    # newer data
    # Get yesterday's date, because it is used in the PHE url
    yesterday = date.today() - datetime.timedelta(1)
    
    dateStr = str(yesterday).replace("-", "")
    
    # Create url of hospital admissions data
    url = ('https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/'  + str(yesterday.year) + "/" + str('{:02}'.format(yesterday.month)) + '/' "COVID-19-daily-admissions-and-beds-" + dateStr + '.xlsx')
    
    df = pd.read_excel(url)   
    
    newHospAdDiag = df.iloc[11:13, 2:   ].T

    newHospAdDiag = newHospAdDiag.dropna(axis=0)

    newHospAdDiag.columns = ['Date', 'Daily hospital admissions plus hospital diagnoses with Covid-19 England']
    
    newHospAdDiag.index = np.arange( len(newHospAdDiag) )
    
    Save(newHospAdDiag, 'newHospAdDiag')
    

    dailyMVbedsOccCovid = df.iloc[102:104, 2:   ].T
    
    dailyMVbedsOccCovid.columns = ['Date', 'Mechanical ventilation beds occupied Covid-19 England']
    
    dailyMVbedsOccCovid.index = np.arange( len(dailyMVbedsOccCovid) )
    
    Save(dailyMVbedsOccCovid, 'dailyMVbedsOccCovid')
    
    
    dailyBedsOccCovid = df.iloc[87:89, 2:   ].T
    
    dailyBedsOccCovid.columns = ['Date', 'Hospital beds occupied Covid-19 England']
    
    dailyBedsOccCovid.index = np.arange( len(dailyBedsOccCovid) )
    
    Save(dailyBedsOccCovid, 'dailyBedsOccCovid')
    
    '''
    #older data
    url = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2021/04/COVID-19-daily-admissions-and-beds-20210406-1.xlsx'
    
    df = pd.read_excel(url) 
    
    oldHospAdDiag = df.iloc[11:13, 2:   ].T

    oldHospAdDiag = oldHospAdDiag.dropna(axis=0)

    oldHospAdDiag.columns = ['Date', 'Daily hospital admissions plus hospital diagnoses with Covid-19 England']
    
    oldHospAdDiag.index = np.arange( len(oldHospAdDiag) )

    Save(oldHospAdDiag, 'oldHospAdDiag')
    '''
        
    return
    

def importMort():
           
    df = pd.read_csv('https://www.mortality.org/Public/STMF/Outputs/stmf.csv', skiprows = 2)
   
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
    
    dfUK = dfUK.rename(columns={"DTotal": "Weekly deaths UK"} )

    Save(dfUK, 'Mort')
    
    return
    

def importGDP():
    
    url = 'https://www.ons.gov.uk/generator?uri=/economy/grossdomesticproductgdp/bulletins/gdpmonthlyestimateuk/may2021/e2947586&format=csv'
    
    r = requests.get(url)
    
    df = pd.read_csv(io.StringIO(r.text))
  
    df.columns = ['Date', 'Monthly GDP index UK'] 
    
    df = df.iloc[6:, :]
    
    df['Date'] = df['Date'].str.replace('June', 'Jun')
    
    df['Date'] = df['Date'].str.replace('July', 'Jul')
    
    df['Date'] = df['Date'].str.replace('Sept', 'Sep')
    
    df['Date'] = pd.to_datetime( df.Date.astype(str), format= '%b %Y')
       
    df.index = np.arange( len(df) )
      
    # Start off with Dates from 2007 in the first column, and weekly deaths
    # for 2007 in the second column.
    yearlyGDP = df[ df['Date'].dt.year == 2007 ]
    
    yearlyGDP = yearlyGDP.rename(columns={"Monthly GDP index UK": "Monthly GDP index UK 2007"} )
    
    # Add columns for GDP for years 2018-2019
    for year in range(2008, 2021):
        yearlyGDP['Monthly GDP index UK ' + str(year)] = df[  df['Date'].dt.year == year ]['Monthly GDP index UK'].values
    
    
    GDP2021 = df[  df['Date'].dt.year == 2021 ]['Monthly GDP index UK'].values

    endGDP2021 = np.empty(12- len(GDP2021)  )

    endGDP2021[:] = np.nan

    GDP2021 = np.concatenate( [GDP2021, endGDP2021] )

    yearlyGDP['Monthly GDP index UK 2021'] = GDP2021
    
    yearlyGDP.iloc[0:, 1:] = yearlyGDP.iloc[0:, 1:].astype(float)
   
    Save(yearlyGDP, 'yearlyGDP')
    
    return
 
    
def importOWID():
    
    # This url contains link to daily updated data
    url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
    
    r = requests.get(url)
    
    df = pd.read_csv(io.StringIO(r.text))
        
    df = df.loc[ df['location'] == 'United Kingdom']
    
    df = df[  ['date', 'positive_rate', 'people_vaccinated', 'people_fully_vaccinated' ] ]
    
    df['date'] = pd.to_datetime( df.date.astype(str), format= '%Y-%m-%d')
    
    df.columns = ['Date', 'Positive test rate UK', 'Individuals one-dose-vaccinated', 'Individuals two-dose-vaccinated']
    
    df.index = np.arange( len(df) )
    
    Save(df, 'OWID')
    
    return
    
 
def importUC():
    
    df = pd.read_excel(r'Data/UC claims.xlsx')
    
    df = df.iloc[ [5,7], 2:-1  ].T
    
    df.columns = [ 'Date', 'Weekly universal credit claims UK'  ]
    
    # Some of the dates randomly have the string ' (r)' added at the end
    # so get rid of that.
    df['Date'] = df['Date'].str.rstrip(' (r)')
    
    df['Date'] = pd.to_datetime( df.Date.astype(str), format= '%B %d, %Y')

    df.index = np.arange( len(df) )
  
    Save(df, 'UC')
    
    return


def importIandP():
    
    df = pd.read_excel(r'Data/Influenza and pneumonia deaths.xlsx', sheet_name='Table 5')
    
    df = df.iloc[4:123, :2]
    
    df.columns = [ 'Date', 'Yearly influenza and pneumonia deaths England and Wales'  ] 
    
    df['Date'] = pd.to_datetime( df.Date.astype(str), format= '%Y')
    
    df.index = np.arange( len(df) )
    
    Save(df, 'IandP')
    
    return
    
    
def importLCD():
    
    # LCDM is leading cause of death for males
    # LCDF is leading cause of death for females
    LCDM = pd.read_excel (r'Data/LCD male.xlsx', sheet_name='Table 5')
    
    LCDF = pd.read_excel (r'Data/LCD female.xlsx', sheet_name='Table 5')

    LCDM = LCDM.iloc[14:, :].T

    LCDF = LCDF.iloc[14:, :].T

    # Initialise df so it has same shape and columns as LCDM
    df = LCDM
    
    # Make the non-date entries equal to the sum of male and female deaths
    df.iloc[:, 1:] = LCDM.iloc[1:, 1:] + LCDF.iloc[1:, 1:]
    
    df.columns = LCDF.iloc[0]
    
    df.index = np.arange( len(df) )
    
    df = df.drop([0])
    
    df.index = np.arange( len(df) )
    
    df = df.rename( columns = {"Leading cause ": "Date" } )
    
    # Entries in the Date column correspond to year, but not all of them are
    # integeres. This coerces them to integers
    df['Date'] = df['Date'].astype(int)
    
    df['Date'] = pd.to_datetime( df.Date.astype(str), format= '%Y')
    
    # The index column has a name that is unnecessary, delete to keep things neat
    df.columns.name = None
    
    Save(df, 'LCD')
    
    return
   

def importdeathByAge():
    
    df = pd.read_excel(r'Data/Deaths by age.xlsx')
    
    df = df.iloc[2:, [2,4, 6]]
    
    df.columns = ['Date', 'Age group', 'Deaths']
    
    df['Date'] = pd.to_datetime( df['Date'].map(lambda x: x[15:]), format = '%d-%b-%y')
    
    deathByAge = df.groupby(['Age group']).sum()
    
    deathByAge['Age group'] = deathByAge.index
    
    deathByAge['Age group'] = deathByAge['Age group'].replace(to_replace = '-', value =" to ", regex = True)
    
    deathByAge.index = np.arange( len(deathByAge) )
    
    Save(deathByAge, 'deathByAge')
    
    return


def importGovSpending():
    
    # import CJRS and SEIS data
    CJRS = pd.read_excel(r'Data/CJRS.xls')

    SEIS = pd.read_excel(r'Data/SEIS.xls')
    
    businessLoans = pd.read_excel(r'Data/Business loans.xls')
    
    VAT = pd.read_excel(r'Data/Vat deferral.xls')
    
    
    df = pd.merge(CJRS, SEIS, how='outer')
    
    df = pd.merge(df, businessLoans, how = 'outer')
    
    df = pd.merge(df, VAT, how = 'outer')
    
    df = df.sort_values(by='Date')
    
    df.index = np.arange( len(df) )
    
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
        
    df.insert(22, 'Total NHS spending UK 2018-2019', 152900000000  )
    
    Save(df, 'govSpending')
    
    return


def importAvgBedsOcc():
    
    df = pd.read_excel(r'Data/Avg bed occupancy.xls', sheet_name = 'Open Overnight')
    
    df = df.iloc[13:54, [1, 2, 4, 10 ]]
    
    df.columns = ['Year', 'Quarter', 'Total available', 'Total occupied'  ]
    
    df.index = np.arange( len(df) )
    
    # Create columns for the dataframe that we want
    columns = ['Date']
        
    for year in  range(2010, 2021):   
        columns.append( 'Quarterly mean overnight NHS beds available England ' + str(year) )
        columns.append(  'Quarterly mean overnight NHS beds occupied England ' + str(year) )
         
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
    
    Save(beds, 'avgBedsOcc')
    
    return


def importHistBedsOcc():
    
    bedsOpen = pd.read_excel(r'Data/Bed occupancy.xlsx', sheet_name = 'G&A_Beds_Open_crosstab').T

    bedsOpen.columns = bedsOpen.loc['Unnamed: 2', :]
    
    bedsOpen = bedsOpen.iloc[3:, :]
    
    bedsOpen.insert(loc=0, column='Date', value= bedsOpen.index  )
    
    bedsOpen.index = np.arange( len(bedsOpen) )
    
    
    url1 = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2020/11/UEC-Daily-SitRep-Acute-Web-File-9-November-15-November-2020-Final-v2.xlsx'
    
    url2 = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2020/11/UEC-Daily-SitRep-Acute-Web-File-16-November-22-November-2020.xlsx'
    
    url3 = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2020/12/UEC-Daily-SitRep-Acute-Web-File-23-November-29-November-2020.xlsx'
    
    url4 = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2020/12/UEC-Daily-SitRep-Acute-Web-File-30-November-06-December.xlsx'
    
    def extract(url):
        
        df = pd.read_excel(url , sheet_name = 'G&A beds').T
    
        df.columns = df.loc['Unnamed: 3', :]
        
        df = df[  df['Code'] == 'Total Beds Open'] 
        
        df = df.iloc[:, 11: ]
        
        df = df.drop( df.columns[range(2,5)], axis=1  )
        
        df.rename(columns= {df.columns[0] : 'Date' } , inplace=True)
        
        return(df)        
        
    bedsOpen2 = pd.concat( [extract(url1), extract(url2)] )
    
    bedsOpen2 = pd.concat( [bedsOpen2, extract(url3)] )
    
    bedsOpen2 = pd.concat( [bedsOpen2, extract(url4)] )
    
    bedsOpen = pd.merge(bedsOpen, bedsOpen2, how= 'outer')
    
    Save(bedsOpen, 'histBedsOpen')
    
    bedsOcc = pd.read_excel(r'Data/Bed occupancy.xlsx', sheet_name = 'G&A_Beds_Occupied_crosstab').T

    bedsOcc.columns = bedsOcc.loc['Unnamed: 2', :]
    
    bedsOcc = bedsOcc.iloc[3:, :]
    
    bedsOcc.insert(loc=0, column='Date', value= bedsOcc.index  )
    
    bedsOcc.index = np.arange( len(bedsOcc) )
    
    Save(bedsOcc, 'histBedsOcc')
    
    return


def createYearlyMort(Mort, deaths):
    
    Mort = Open('Mort')
    
    # Start off with Dates from 2015 in the first column, and weekly deaths
    # for 2015 in the second column.
    yearlyMort = Mort[ Mort['Date'].dt.year == 2015 ][['Date', 'Weekly deaths UK']]
    
    yearlyMort = yearlyMort.rename(columns={"Weekly deaths UK": "Weekly deaths UK 2015"})
    
    yearlyMort.index = np.arange( len(yearlyMort.index))

    # Add columns for weekly deaths for years 2016-2020
    for year in range(2016, 2021):
                    
        yearlyMort['Weekly deaths UK ' + str(year)] = Mort[  Mort['Date'].dt.year == year ]['Weekly deaths UK'].values[-52:]
    
    # Add column for weekly deaths in 2021
    # This isn't so easy because 2021 isn't finished yet, so need to create
    # an array that has apropriate NaN's concatenated at the end
    Mort2021 = Mort[  Mort['Date'].dt.year == 2021 ]['Weekly deaths UK'].values

    endMort2021 = np.empty(52- len(Mort2021)  )

    endMort2021[:] = np.nan

    Mort2021 = np.concatenate( [Mort2021, endMort2021] )

    yearlyMort['Weekly deaths UK 2021'] = Mort2021
    
    # Add a column that has weekly mean deaths for 2015-2019
    yearlyMort['Mean weekly deaths UK 2015-2019'] = yearlyMort.iloc[:, 1:6].mean(axis=1)
    
    deaths2020 = pd.DataFrame()
     
    # Make the first columns of deaths2020 equal to the dates of the year. 
    deaths2020['Date'] = pd.date_range(start = '2020-01-01', end = '2020-12-31', freq='D')
    
    deaths2020 = pd.merge(deaths2020, deaths[ pd.DatetimeIndex( deaths['Date']).year == 2020 ], how='outer' )
    
    # noWeeks is the number of weeks for which we have weekly deaths in 2020
    noWeeks = 52 - yearlyMort['Weekly deaths UK 2020'].isna().sum()
    
    weeklyCoronaDeaths2020 = np.empty(52)
    
    weeklyCoronaDeaths2020[:] = np.nan
   
    # Calculate weeklyCoronaDeaths by summing in groups of 7
    for week in range(52):    
        
        weeklyCoronaDeaths2020[ week ] = deaths2020.iloc[ 7*week : (7*week)+6, 1 ].sum()
               
    deaths2021 = pd.DataFrame()
     
    # Make the first columns of deaths2020 equal to the dates of the year. 
    deaths2021['Date'] = pd.date_range(start = '2021-01-01', end = '2021-12-31', freq='D')
    
    deaths2021 = pd.merge(deaths2021, deaths[ pd.DatetimeIndex( deaths['Date']).year == 2021 ], how='outer' )
    
    # noWeeks is the number of weeks for which we have weekly deaths in 2020
    noWeeks = 52 - yearlyMort['Weekly deaths UK 2021'].isna().sum()
    
    weeklyCoronaDeaths2021 = np.empty(52)
    
    weeklyCoronaDeaths2021[:] = np.nan
   
    # Calculate weeklyCoronaDeaths by summing in groups of 7
    for week in range(noWeeks):    
        
        weeklyCoronaDeaths2021[ week ] = deaths2021.iloc[ 7*week : (7*week)+6, 1 ].sum()
    
    
    # Add a column to yearlyMort that is mean deaths + Covid-19 deaths
    yearlyMort['Mean weekly deaths 2015-2019<br>plus Covid-19 deaths UK 2020'] = \
              weeklyCoronaDeaths2020 + yearlyMort['Mean weekly deaths UK 2015-2019']
              
    yearlyMort['Mean weekly deaths 2015-2019<br>plus Covid-19 deaths UK 2021'] = \
              weeklyCoronaDeaths2021 + yearlyMort['Mean weekly deaths UK 2015-2019']
                
    Save(yearlyMort, 'yearlyMort')
    
    return 


def importUnemployment():
    
    url = 'https://www.ons.gov.uk/generator?format=csv&uri=/employmentandlabourmarket/peoplenotinwork/unemployment/timeseries/mgsx/lms'
    
    r = requests.get(url)
    
    df = pd.read_csv(io.StringIO(r.text))
    
    df = df.iloc[255:, :]

    df.columns = ['Date', 'Unemployment rate (seasonally adjusted)']
    
    # Make unemployment a number between zero and one.
    df['Unemployment rate (seasonally adjusted)'] = df['Unemployment rate (seasonally adjusted)'].astype(float)/100
    
    # For reasons unknown, some of them are in the wrong format.
    df = df[ df['Date'].str.len() == 8 ]

    df['Date'] =   pd.to_datetime(df.Date.astype(str), format='%Y %b')
    
    df.index = np.arange( len(df) )
    
    Save(df, 'Unemployment')
    
    return


def importONS():
    
    url = 'https://coronavirus.data.gov.uk/api/v1/data?filters=areaType=overview&structure=%7B%22areaType%22:%22areaType%22,%22areaName%22:%22areaName%22,%22areaCode%22:%22areaCode%22,%22date%22:%22date%22,%22newDeaths28DaysByDeathDate%22:%22newDeaths28DaysByDeathDate%22,%22cumDeaths28DaysByDeathDate%22:%22cumDeaths28DaysByDeathDate%22%7D&format=csv'
    
    deaths = pd.read_csv(url)
    
    deaths = deaths.iloc[:, 3:6]
    
    deaths.columns = ['Date', 'Daily Covid-19 deaths UK', 'Cumulative deaths']
    
    deaths['Date'] =  pd.to_datetime( deaths.Date, format = '%Y-%m-%d'  )
    
    Save(deaths, 'deaths')
    
    
    url = 'https://coronavirus.data.gov.uk/api/v1/data?filters=areaName=United%2520Kingdom;areaType=overview&structure=%7B%22areaType%22:%22areaType%22,%22areaName%22:%22areaName%22,%22areaCode%22:%22areaCode%22,%22date%22:%22date%22,%22newVirusTests%22:%22newVirusTests%22,%22cumVirusTests%22:%22cumVirusTests%22%7D&format=csv'
    
    tests = pd.read_csv(url)
    
    tests = tests.iloc[:, 3:6]
    
    tests.columns = ['Date', 'Daily tests UK', 'Cumulative tests UK' ]

    tests['Date'] =  pd.to_datetime( tests.Date, format = '%Y-%m-%d'  )

    Save(tests, 'tests')
    
    
    url = 'https://api.coronavirus.data.gov.uk/v1/data?filters=areaType=overview&structure=%7B%22areaType%22:%22areaType%22,%22areaName%22:%22areaName%22,%22areaCode%22:%22areaCode%22,%22date%22:%22date%22,%22newCasesBySpecimenDate%22:%22newCasesBySpecimenDate%22,%22cumCasesBySpecimenDate%22:%22cumCasesBySpecimenDate%22%7D&format=csv'

    cases = pd.read_csv(url)

    cases = cases.iloc[:, 3:5]

    cases.columns = ['Date', 'Daily new Covid-19 cases UK']
    
    cases['Date'] =  pd.to_datetime( cases.Date, format = '%Y-%m-%d'  )
 
    Save(cases, 'cases')
    
    
    url = 'https://api.coronavirus.data.gov.uk/v1/data?filters=areaName=United%2520Kingdom;areaType=overview&structure=%7B%22areaType%22:%22areaType%22,%22areaName%22:%22areaName%22,%22areaCode%22:%22areaCode%22,%22date%22:%22date%22,%22newOnsDeathsByRegistrationDate%22:%22newOnsDeathsByRegistrationDate%22,%22cumOnsDeathsByRegistrationDate%22:%22cumOnsDeathsByRegistrationDate%22%7D&format=csv'
    
    #deaths with covid on the death certificate
    deathsCert = pd.read_csv(url)
    
    deathsCert = deathsCert.iloc[:, 3:5]

    deathsCert.columns = ['Date', 'Weekly deaths with Covid-19 on death certificate']
    
    deathsCert['Date'] = pd.to_datetime(  deathsCert.Date, format = '%Y-%m-%d') 
    
    # deathsComp will allow comparison of the two different ways of counting deaths
    deathComp = pd.merge(deathsCert, deaths[['Date', 'Cumulative deaths']] , how = 'left')

    deathComp['Weekly deaths within 28 days of a positive test'] = np.nan
    
    deathComp = deathComp.sort_values(by = ['Date'], ascending = False)
    
    # Calculate first difference of cumulative deaths
    for index in range( len(deathComp)-1 ):
        
        deathComp.iloc[index, 3] = deathComp.iloc[index, 2] - deathComp.iloc[index + 1, 2]

   # Drop any entries with na in the 'Weekly deaths with Covid-19 on death certificate' column
    deathComp = deathComp[deathComp['Weekly deaths with Covid-19 on death certificate'].notna()] 
    
    Save(deathComp, 'deathComp')
    
    return



def importRed():
    
    '''
    url = 'https://www.ons.gov.uk/generator?uri=/employmentandlabourmarket/peopleinwork/employmentandemployeetypes/bulletins/uklabourmarket/january2021/0636c13e&format=csv'

    r = requests.get(url)
    
    redundancies = pd.read_csv(io.StringIO(r.text))

    #keep relevant rows
    redundancies = redundancies.iloc[6:, :]
    
    redundancies.columns = ['date', 'Redundancies in last 3 months']
    
    redundancies.insert(0, 'Date', pd.to_datetime(redundancies.date.str[:4], format='%Y')  \
    + pd.to_timedelta(  ( (redundancies.date.str[6].astype(int)-1) * 13 + redundancies.date.str[-2:].astype('int')).astype(str) + ' W') )
    
    redundancies = redundancies.drop('date', axis = 1 )
       
    #rename appropriately
    redundancies.columns = ['Date', 'Redundancies in last 3 months']

    #Convert date column to a datetime object
    redundancies['Date'] = pd.to_datetime( redundancies['Date'].str[4:] )
    
    #Redundancies measured in 1000s.
    redundancies['Redundancies in last 3 months'] = redundancies['Redundancies in last 3 months'].astype(float) *1000
    '''


    url2 = 'https://www.ons.gov.uk/generator?uri=/employmentandlabourmarket/peopleinwork/employmentandemployeetypes/bulletins/employmentintheuk/july2021/97d6896a&format=csv'
    
    r = requests.get(url2)
    
    redundancies2 = pd.read_csv(io.StringIO(r.text))
    
    redundancies2 = redundancies2.iloc[6:, :]
    
    redundancies2.columns = ['Date', 'Redundancies in last 3 months']

    redundancies2['Date'] = pd.to_datetime( redundancies2['Date'].str[-8:] )
    
    # Redundancies measured in 1000s.
    redundancies2['Redundancies in last 3 months'] = redundancies2['Redundancies in last 3 months'].astype(float) *1000

    redundancies2.index = np.arange( len(redundancies2) )

    Save(redundancies2, 'Redundancies')
    
    return



def importJSA():
    
    JSA = pd.read_excel(r'Data/Claimants.xlsx')
    
    JSA = JSA.iloc[10:96, 1:3]

    JSA.columns = ['Date', 'Number of JSA claimants']
    
    JSA['Date'] = pd.to_datetime( JSA.Date.astype(str), format= '%B %Y')
    
    JSA.index = np.arange( len(JSA) )

    Save(JSA, 'JSA')
    
    return
    

def importClaimants():
    
    url = 'https://www.ons.gov.uk/generator?uri=/employmentandlabourmarket/peopleinwork/employmentandemployeetypes/bulletins/employmentintheuk/april2021/1a9b53b0&format=csv'

    r = requests.get(url)
    
    claimants = pd.read_csv(io.StringIO(r.text))

    claimants = claimants.iloc[6:, :]
    
    claimants.columns = ['Date', 'Claimant count, seasonally adjusted']
    
    claimants['Date'] = pd.to_datetime( claimants.Date.astype(str), format= '%B %Y')
    
    # Claimant count is measured in thousands
    claimants['Claimant count, seasonally adjusted'] = claimants['Claimant count, seasonally adjusted'].astype(float) *1000

    claimants.index = np.arange( len(claimants) )
    
    Save(claimants, 'claimants')
    
    return
    
    

def importPathways():
    
    url = 'https://files.digital.nhs.uk/11/BD1AFA/NHS%20Pathways%20Covid-19%20data%20%202021-08-05.csv'

    calls = pd.read_csv(url)

    calls = calls[[ 'Call Date', 'TriageCount']]
    
    calls['Call Date'] = pd.to_datetime( calls['Call Date'], format = '%d/%m/%Y'   )

    calls = calls.groupby(['Call Date']).sum()


    url = 'https://files.digital.nhs.uk/22/FF905B/111%20Online%20Covid-19%20data_2021-08-05.csv'
    
    online = pd.read_csv(url)

    online = online[[ 'journeydate', 'Total']]
    
    online['journeydate'] = pd.to_datetime( online['journeydate'], format = '%d/%m/%Y'   )

    online = online.groupby(['journeydate']).sum()

    
    pathways = pd.concat( [calls, online], axis=1)
    
    pathways['Date'] = pathways.index

    pathways.index = np.arange( len(pathways) )
    
    pathways = pathways.rename(columns={"TriageCount": "Daily potential Covid-19 telephone triages England", \
                                        "Total": "Daily potential Covid-19 online assessments England"})

    Save(pathways, 'pathways')

    return


def importSurveilance():
    
   url = 'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1000718/Weekly_Influenza_and_COVID19_report_data_w27v2.xlsx'
    
   ICU = pd.read_excel(url, sheet_name = 'Figure 44. SARIWatch-ICUPHEC')
   
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