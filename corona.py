# -*- coding: utf-8 -*-
"""
Created on Sat Oct  3 11:22:19 2020

@author: Steven
"""


import pandas as pd

import pickle

import plotly.io as pio

import numpy as np

import os

import plotly.express as px

import chart_studio.tools as tls

import math

import ssl

import json

from datetime import datetime

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




# importHospAd imports the Hospital admissions data that records admissions
# with coronavirus , puts it into a more useful format and saves it.


def importHospAd():
    
    # This url contains a link to hospital admissions data.
    # It looks like this url is going to change each month, so 
    # it needs updated by hand each time
    
    url = "https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2020/10/Covid-Publication-08-10-2020v3.xlsx"
    
    df = pd.read_excel (url, sheet_name='Admissions Total')
    
    # Drop everything except rows 11 and 12, and all columns from 5 onwards.
    # Row 11 has the date
    # Row 12 has the number of hospital admissions
    # The actual numbers start from column 5 onwards
    
    df = df.iloc[11:13, 5:   ].T
    
    # This takes the columns whose values are pandas timestamps, and makes the 
    # column labels the corresponding date.
    
    df.columns = ['Date', 'Daily hospital admissions with<br>coronavirus England']
    
    # Make the row index equal to row number
    
    df.index = np.arange( len(df) )
    
    # Save the dataframe as a pickle object
    
    Save(df, 'HospAd')

    return


# importHospAd2 imports the Hospital admissions data that records admissions
# plus diagnoses with coronavirus , puts it into a more useful format and 
# saves it.



def importHospAd2():
    
    url = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2020/10/COVID-19-daily-admissions-20201030.xlsx'

    df = pd.read_excel (url)

    df = df.iloc[11:13, 2:   ].T
    
    # This takes the columns whose values are pandas timestamps, and makes the 
    # column labels the corresponding date.
    
    df.columns = ['Date', 'Daily hospital admissions<br>plus hospital diagnoses<br>with coronavirus England']
    
    # Make the row index equal to row number
    
    df.index = np.arange( len(df) )
    
    # Save the dataframe as a pickle object
    
    Save(df, 'HospAd2')
    
    return
    

# importMort imports the mortality data, puts it into a more useful format
# and saves it.


def importMort():
    
    # This url contains link to frequently updated data
    
    url = "https://www.mortality.org/Public/STMF/Outputs/stmf.xlsx"
    
    # Read in from above url
    
    df = pd.read_excel(url, sheet_name='GBRTENW')
    
    
    # Label the columns according to the values in row 1

    df.columns = df.iloc[1]
    
    # The index column has a name that is unnecessary, delete to keep things neat
    
    df.columns.name = None
    
    # Remove row 1, because it is redundant

    df = df.iloc[2:, :]
    
    # Index the dataframe by timedates.
    # To conver a year and a week into a timedate object, first convert just the
    # year to a timedate. Then calculate how many days pass from the start of the year
    # to the start of the numbered week. Convert that to a timedelta, and add onto
    # the timedate for the year

    df['Date'] = pd.to_datetime(df.Year.astype(str), format='%Y') + pd.to_timedelta(   (df.Week-1).mul(7).astype(str) + ' days')
    
    # Keep only entries that have 'b' in the 'Sex' field. 'b' stands for 'both,
    # and it means that both male and female deaths are counted

    df = df.loc[df['Sex'] == 'b']
    
    # Column 9 is Total deaths for that week. I have to reference it by 
    # column number, because there are two columns called 'Total'.
    # Column 19 is the date.

    df = df.iloc[:, [19, 9 ] ]
    
    # Rename Total column to Deaths. 
    
    df = df.rename(columns={"Total": "Weekly deaths England and Wales"} )
    
    
    # Coerce the deaths column to type int.
    
    df.loc[:, 'Weekly deaths England and Wales'] = df.loc[:, 'Weekly deaths England and Wales'].astype(int)
    
    # Make the row index equal to row number
    
    df.index = np.arange( len(df) )
    
    # Save the dataframe as a pickle object
    
    Save(df, 'Mort')
    
    return
    



# importGDP imports the GDP data, puts it into a more useful format
# and saves it.

# Note that although GDP data is availble for download in csv format which
# I generally prefer, there is a strange issue where the date format gets altered
# when the file is read in with pd.read_csv. On the other hand, the excel file
# does not have that issue.

# Also even then there is an annoying issue with March 2017, which has 
# 2017MAR in the date column rather than 2017 MAR, so tha has to be 
# fixed manually.

def importGDP():
    
    
    df = pd.read_excel(r'Data/GDP Index.xls')
    
    # Rename columns appropriately
    
    df.columns = ['Date', 'Monthly GDP index UK'] 
    
    # The actual numbers start from row 6 onwards, so drop everything before that.
    
    df = df.iloc[6:, :]
    
    # Convert the Date entries to a proper timedate
    
    df['Date'] = pd.to_datetime( df.Date.astype(str), format= '%Y %b')
       
    # Make the row index equal to row number
    
    df.index = np.arange( len(df) )
   
    # Save the dataframe as a pickle object
    
    Save(df, 'GDP')
    
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
    
    df.columns = ['Date', 'Daily positive tests UK', 'Daily coronavirus deaths UK', 'Daily tests UK', 'Positive test rate UK'  ]
    
  
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







# Import and format all the data



# OWID data is updated daily
# The file is downloaded automatically.
    
importOWID()


OWID = Open('OWID')  


# The HospAd2 data series is updated daily.
# The link need to be updated manually.


importHospAd2()


HospAd2 = Open('HospAd2')



# Mortality data is updated weekly, on Thursdays.
# The file is downloaded automatically.
    
#importMort()


Mort = Open('Mort')


# Government spending data is updated roughly monthly.
    
importGovSpending()


govSpending = Open('govSpending')


# Deaths by age is updated weekly, on Tuesdays.
# The file needs to be downloaded manually.
    
# importdeathByAge()


deathByAge = Open('deathByAge') 



# UC data is updated sporadically.
# The file needs to be downloaded manually.
    
# importUC()


UC = Open('UC') 


# GDP data is updated monthly, approximately around the 10th of each month.
# The file needs to be downloaded manually.

# importGDP()


GDP = Open('GDP')


# IandP data is never updated.
    
# importIandP()


IandP = Open('IandP')

# LCD data is never updated.
    
# importLCD()


LCD = Open('LCD') 

# HospAd never needs to be updated, because the data series of interest
# has been terminated.


#importHospAd()


HospAd = Open('HospAd')









# Create combined OWID, HospAd dataframe

df = mergeFrames(OWID, HospAd )

df = mergeFrames(df, HospAd2)



# createYearlyMort creates a dataframe of year by year mortality, and saves it.

def createYearlyMort(Mort, OWID):
    
    # Start off with Dates from 2010 in the first column, and weekly deaths
    # for 2010 in the second column.
    
    yearlyMort = Mort[ Mort['Date'].dt.year == 2010 ][['Date', 'Weekly deaths England and Wales']]
    
    # Rename columns appropriately.
    
    yearlyMort = yearlyMort.rename(columns={"Weekly deaths England and Wales": "Weekly deaths England<br>and Wales 2010"} )
    
    # Make the row index equal to row number
    
    yearlyMort.index = np.arange( len(yearlyMort.index) )

    # Add columns for weekly deaths for years 2011-2019


    for year in range(2011, 2020):
    
        yearlyMort['Weekly deaths England<br>and Wales ' + str(year)] = Mort[  Mort['Date'].dt.year == year ]['Weekly deaths England and Wales'].values
    
    # Add column for weekly deaths in 2020
    # This isn't so easy because 2020 isn't finished yet, so need to create
    # an array that has apropriate NaN's concatenated at the end

    Mort2020 = Mort[  Mort['Date'].dt.year == 2020 ]['Weekly deaths England and Wales'].values


    endMort2020 = np.empty(52- len(Mort2020)  )

    endMort2020[:] = np.nan

    Mort2020 = np.concatenate( [Mort2020, endMort2020] )

    yearlyMort['Weekly deaths England<br>and Wales 2020'] = Mort2020
    
    # Add a column that has weekly mean deaths for 2010-2019

    yearlyMort['Mean weekly deaths 2010-2019<br>England and Wales'] = yearlyMort.iloc[:, 1:12].mean(axis=1)
    
    
    
    # Isolate coronavirus deaths in 2020
    
    OWIDDeaths2020 = OWID[ OWID.Date.dt.year ==2020 ][ 'Daily coronavirus deaths UK' ]
    
    # Make the row index equal to row number
    
    OWIDDeaths2020.index = np.arange( len(OWIDDeaths2020) )
    
    # These are useful for finding weekly coronavirus deaths
    
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
    
    # Add a column to yearlyMort that is mean deaths + coronavirus deaths
    
    yearlyMort['Mean weekly deaths 2010-2019 England<br>and  Wales plus coronavirus deaths 2020'] = \
                weeklyCoronaDeaths + yearlyMort['Mean weekly deaths 2010-2019<br>England and Wales']
    
    # Save the dataframe as a pickle object
    
    Save(yearlyMort, 'yearlyMort')
    
    return 

# createYearlyMort only has to be run once, so it is commented out.
    
createYearlyMort(Mort, OWID)




yearlyMort = Open('yearlyMort') 


# lastDate is the final data where there is data in the combined HospAd + OWID
# data frame

lastDate =  str(df.iloc[-1,0])[:10]


# totalCoronaDeaths is what it says.

totalCoronaDeaths = int( OWID['Daily coronavirus deaths UK'].sum() )


# Add a column to IandP and LCD that is constant and equal to total 
# coronavirus deaths. This is useful for plotting purposes


IandP.insert(1, 'Coronavirus deaths 2020 UK'  , totalCoronaDeaths  )

LCD.insert(1, 'Coronavirus deaths 2020 UK'  , totalCoronaDeaths  )


# This line is to make all the columns of IandP, LCD the same type, so they 
# can be  plotted together on the same plot by plotly.

IandP['Yearly influenza and pneumonia deaths England and Wales'] = IandP['Yearly influenza and pneumonia deaths England and Wales'].astype(np.int)


LCD.iloc[:, 1:] = LCD.iloc[:, 1:].astype(np.int)


# Calculate total coronavirus excess deaths and total excess deaths this year.

totalNonCoronaED = (yearlyMort['Weekly deaths England<br>and Wales 2020'] - yearlyMort['Mean weekly deaths 2010-2019 England<br>and  Wales plus coronavirus deaths 2020']).sum()

totalED = (yearlyMort['Weekly deaths England<br>and Wales 2020'] - yearlyMort['Mean weekly deaths 2010-2019<br>England and Wales']).sum()

# Create a dictionary with some death related variables of interest.

deathDict = { "TCD": '{:,}'.format(totalCoronaDeaths), 
        "TNCED": '{:,}'.format(round(totalNonCoronaED,2)),
        "TED": '{:,}'.format(round(totalED,2))}

# Save that dictinary in json format. This can then be passed to other
# applications.

with open('deaths.json', 'w') as file:
    json.dump(deathDict, file)








# Create all the figures.


fig1 = px.line(df, x="Date", y=['Daily coronavirus deaths UK', 'Daily hospital admissions with<br>coronavirus England', 'Daily positive tests UK', \
                                'Daily hospital admissions<br>plus hospital diagnoses<br>with coronavirus England' ], range_x=['2020-01-01',lastDate], \
             template = "simple_white", color_discrete_sequence =['red', 'gold', 'blue', 'green'] )

fig1.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=1.6,
    xanchor="left",
    x=0.02
)
    
)


# fig1.add_trace( go.Scatter(x = Mort["Date"], y = Mort["Excess deaths"]/7, mode = 'lines', name = 'Excess deaths', line=dict(color="green")) ) 



testsFig = px.bar(df, x="Date", y=['Daily tests UK'], range_x=['2020-01-01',lastDate], \
             template = "simple_white", color_discrete_sequence =['slateblue' ] )

testsFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="left",
    x=0.02
)
)





testPositiveFig = px.line(df, x="Date", y=['Positive test rate UK'], range_x=['2020-01-01',lastDate], \
             template = "simple_white", color_discrete_sequence =['indigo' ] )

testPositiveFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="right",
    x=0.99
)
)

testPositiveFig.layout.yaxis.tickformat = ',.0%'



UCFig = px.line(UC, x="Date", y=['Weekly universal credit claims UK'], range_x=['2020-01-01',lastDate], \
             template = "simple_white", color_discrete_sequence =['deeppink' ] )

UCFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="right",
    x=0.99
)
)







GDPFig = px.line(GDP, x="Date", y=['Monthly GDP index UK'], range_x=['2020-01-01',lastDate], \
             template = "simple_white", color_discrete_sequence =['darkslategray' ] )

GDPFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="right",
    x=0.99
)
)








IandPFig = px.line(IandP, x="Date", y=['Coronavirus deaths 2020 UK', 'Yearly influenza and pneumonia deaths England and Wales' ], \
             template = "simple_white", color_discrete_sequence =['orange' ,'teal' ] )

IandPFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="right",
    x=0.99
)
)








yearlyMortCols = list(yearlyMort.columns)

yearlyMortCols = yearlyMortCols[10:] 

yearlyMortCols = [ yearlyMortCols[-2], yearlyMortCols[-1], yearlyMortCols[1], yearlyMortCols[0]]



meanDeathsFig = px.line(yearlyMort, x="Date", y= yearlyMortCols, range_x=['2010-01-01','2011-01-01'], template = "simple_white" )

meanDeathsFig.update_layout(
    
)


meanDeathsFig.update_layout(xaxis=dict(tickformat="%d-%m"),
            yaxis_title="Weekly deaths",
            legend_title="Variable:",
            legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.99) 
)





deathByAgeFig = px.bar(deathByAge, x="Age", y='Deaths' ,template = "simple_white")

deathByAgeFig.update_layout(
    yaxis_title="Deaths",
    legend_title="Variable:",
)





# Create a list of columns that I want to plot. It's everything except 'Date'

LCDcols = list(LCD.columns)

LCDcols.remove('Date')

LCDFig = px.line(LCD, x="Date", y= LCDcols, template = "simple_white" ) 

LCDFig.update_layout(
    yaxis_title="Yearly deaths UK",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=1.05,
    xanchor="right",
    x=0.99
)
)



govSpendingFig = px.line(govSpending, x="Date", y=['Total value of CJRS and SEIS claims', \
        'Total value of approved business loans', 'Total NHS spending UK 2018-2019'],template = "simple_white" )

govSpendingFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.8,
    xanchor="left",
    x=0.02
)
)





# Create HTML files

pio.write_html(fig1, file='HTML files/fig1.html', auto_open=True)

pio.write_html(testsFig, file='HTML files/testsFig.html', auto_open=True)

pio.write_html(testPositiveFig, file='HTML files/testPositiveFig.html', auto_open=True)

pio.write_html(UCFig, file='HTML files/UCFig.html', auto_open=True)

pio.write_html(GDPFig, file='HTML files/GDPFig.html', auto_open=True)

pio.write_html(IandPFig, file='HTML files/IandPFig.html', auto_open=True)

pio.write_html(meanDeathsFig, file='HTML files/meanDeathsFig.html', auto_open=True)

pio.write_html(LCDFig, file='HTML files/LCDFig.html', auto_open=True)

pio.write_html(deathByAgeFig, file='HTML files/deathByAgeFig.html', auto_open=True)

pio.write_html(govSpendingFig, file='HTML files/govSpendingFig.html', auto_open=True)





