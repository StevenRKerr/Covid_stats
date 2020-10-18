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

import ssl



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



# importHospAd imports the Hospital admissions data, puts it into a more useful 
# format and saves it.


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
    
    df.columns = ['Date', 'Daily hospital admissions England and Wales']
    
    # Make the row index equal to row number
    
    df.index = np.arange( len(df) )
    
    # Save the dataframe as a pickle object
    
    Save(df, 'HospAd')

    return

# importHospAd only needs to be run once, so it is commented out.

importHospAd()


HospAd = Open('HospAd')


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
    
# importMort only has to be run once, so it is commented out.
    
importMort()


Mort = Open('Mort')


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
 
# importGDP only has to be run once, so it is commented out.
    
importGDP()


GDP = Open('GDP')
    
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
    
    df.columns = ['Date', 'Daily new cases UK', 'Daily coronavirus deaths UK', 'Daily new tests UK', 'Test positive rate UK'  ]
    
  
    # Make the row index equal to row number
    
    df.index = np.arange( len(df) )
    
    # Save the dataframe as a pickle object
    
    Save(df, 'OWID')
    
    return
    
# importGDP only has to be run once, so it is commented out.
    
importOWID()


OWID = Open('OWID')  
    
    
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


# importUC only has to be run once, so it is commented out.
    
importUC()


UC = Open('UC') 


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
    
    
# importIandP only has to be run once, so it is commented out.
    
importIandP()


IandP = Open('IandP') 




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
   
# importLCD only has to be run once, so it is commented out.
    
importLCD()


LCD = Open('LCD') 







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



# Create combined OWID, HospAd dataframe

df = mergeFrames(OWID, HospAd )



# createYearlyMort creates a dataframe of year by year mortality, and saves it.

def createYearlyMort(Mort):
    
    # Start off with Dates from 2010 in the firs column, and weekly deaths
    # for 2010 in the second column.
    
    yearlyMort = Mort[ Mort['Date'].dt.year == 2010 ][['Date', 'Weekly deaths England and Wales']]
    
    # Rename columns appropriately.
    
    yearlyMort = yearlyMort.rename(columns={"Weekly deaths England and Wales": "2010"} )
    
    # Make the row index equal to row number
    
    yearlyMort.index = np.arange( len(yearlyMort.index) )

    # Add columns for weekly deaths for years 2011-2019


    for year in range(2011, 2020):
    
        yearlyMort[str(year)] = Mort[  Mort['Date'].dt.year == year ]['Weekly deaths England and Wales'].values
    
    # Add column for weekly deaths in 2020
    # This isn't so easy because 2020 isn't finished yet, so need to create
    # an array that has apropriate NaN's concatenated at the end

    Mort2020 = Mort[  Mort['Date'].dt.year == 2020 ]['Weekly deaths England and Wales'].values


    Mort2020End = np.empty(52- len(Mort2020)  )

    Mort2020End[:] = np.nan

    Mort2020 = np.concatenate( [Mort2020, Mort2020End] )

    yearlyMort['2020'] = Mort2020
    
    # Add a column that has weekly mean deaths for 2010-2019

    yearlyMort['Mean weekly deaths 2010-2019 England and Wales'] = yearlyMort.iloc[:, 1:12].mean(axis=1)
    
    # Save the dataframe as a pickle object
    
    Save(yearlyMort, 'yearlyMort')
    
    return 

# createYearlyMort only has to be run once, so it is commented out.
    
createYearlyMort(Mort)




yearlyMort = Open('yearlyMort') 


# lastDate is the final data where there is data in the combined HospAd + OWID
# data frame

lastDate =  str(df.iloc[-1,0]).strip(' 00:00:00')


# totalCoronaDeaths is what it says.

totalCoronaDeaths = int( OWID['Daily coronavirus deaths UK'].sum() )


# Add a column to IandP and LCD that is constant and equal to total 
# coronavirus deaths. This is useful for plotting purposes

IandP['Coronavirus deaths 2020 UK'] = totalCoronaDeaths

LCD['Coronavirus deaths 2020 UK'] = totalCoronaDeaths


# This line is to make all the columns of IandP, LCD the same type, so they 
# can be  plotted together on the same plot by plotly.

IandP['Yearly influenza and pneumonia deaths England and Wales'] = IandP['Yearly influenza and pneumonia deaths England and Wales'].astype(np.int)


LCD.iloc[:, 1:] = LCD.iloc[:, 1:].astype(np.int)



# Create all the figures.


fig1 = px.bar(df, x="Date", y=['Daily coronavirus deaths UK', 'Daily hospital admissions England and Wales', 'Daily new cases UK'], range_x=['2020-01-01',lastDate], \
             template = "simple_white", color_discrete_sequence =['red', 'gold', 'blue'] )

fig1.update_layout(
    yaxis_title="",
    legend_title="Variable:",
)



# fig1.add_trace( go.Scatter(x = Mort["Date"], y = Mort["Excess deaths"]/7, mode = 'lines', name = 'Excess deaths', line=dict(color="green")) ) 



testsFig = px.bar(df, x="Date", y=['Daily new tests UK'], range_x=['2020-01-01',lastDate], \
             template = "simple_white", color_discrete_sequence =['cadetblue' ] )

testsFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
)





testPositiveFig = px.line(df, x="Date", y=['Test positive rate UK'], range_x=['2020-01-01',lastDate], \
             template = "simple_white", color_discrete_sequence =['indigo' ] )

testPositiveFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
)

testPositiveFig.layout.yaxis.tickformat = ',.0%'



UCFig = px.line(UC, x="Date", y=['Weekly universal credit claims UK'], range_x=['2020-01-01',lastDate], \
             template = "simple_white", color_discrete_sequence =['deeppink' ] )

UCFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
)





GDPFig = px.line(GDP, x="Date", y=['Monthly GDP index UK'], range_x=['2020-01-01',lastDate], \
             template = "simple_white", color_discrete_sequence =['darkslategray' ] )

GDPFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
)





IandPFig = px.line(IandP, x="Date", y=['Yearly influenza and pneumonia deaths England and Wales', 'Coronavirus deaths 2020 UK'], \
             template = "simple_white", color_discrete_sequence =['teal', 'orange' ] )

IandPFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
)






meanDeathsFig = px.line(yearlyMort, x="Date", y= ['Mean weekly deaths 2010-2019 England and Wales', '2015', '2016', '2017', '2018', '2019', '2020' ], range_x=['2010-01-01','2011-01-01'], \
             template = "simple_white" )

meanDeathsFig.update_layout(
    yaxis_title="Weekly deaths",
    legend_title="Variable:",
)


meanDeathsFig.update_layout(xaxis=dict(tickformat="%d-%m"))




# Create a list of columns that I want to plot. It's everything except 'Date'

LCDcols = list(LCD.columns)

LCDcols.remove('Date')

LCDFig = px.line(LCD, x="Date", y= LCDcols, template = "simple_white" ) 

LCDFig.update_layout(
    yaxis_title="Yearly deaths UK",
    legend_title="Variable:",
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



# The following command is used to generate the iframe embed code for a figure
# After uploading the html files above to github, get the github url and
# paste it into this command. It will generate HTML iframe code that will
# embed the graph in a webpage.

# tls.get_embed('github url goes here')



