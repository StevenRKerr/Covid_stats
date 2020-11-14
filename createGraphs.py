# -*- coding: utf-8 -*-
"""
Created on Mon Nov  2 12:08:14 2020

@author: Steven
"""


import pandas as pd

import pickle

import plotly.io as pio

import numpy as np

import plotly.express as px

import json

import importData as iD

import datetime


# Import and format all the data



# ONS data is updated daily
# The file is downloaded automatically.

iD.importONS()


deaths = iD.Open('deaths') 

cases = iD.Open('cases')

deathComp = iD.Open('deathComp')


# OWID data is updated daily
# The file is downloaded automatically.
    
iD.importOWID()


OWID = iD.Open('OWID')  


# The NewHosp data series is updated daily.
# The file is downloaded automatically.

iD.importDailyHosp()


newHospAd = iD.Open('newHospAd')

dailyBedsOccCovid = iD.Open('dailyBedsOccCovid')

dailyMVbedsOccCovid = iD.Open('dailyMVbedsOccCovid')



# Mortality data is updated weekly, on Thursdays.
# The file is downloaded automatically.
    
iD.importMort()


Mort = iD.Open('Mort')


# importMonthlyHosp is updated around the 12th of each month

#iD.importMonthlyHosp()

oldHospAd = iD.Open('oldHospAd')

monthlyBedsOcc = iD.Open('monthlyBedsOcc')

monthlyBedsOccCovid = iD.Open('monthlyBedsOccCovid')

monthlyMVbedsOcc = iD.Open('monthlyMVbedsOcc')

monthlyMVbedsOccCovid = iD.Open('monthlyMVbedsOccCovid')



# Unempoyment data is updated roughly monthly

# iD.importUnemployment()

Unemployment = iD.Open('Unemployment')



# Government spending data is updated roughly monthly.
    
#iD.importGovSpending()


govSpending = iD.Open('govSpending')


# Deaths by age is updated weekly, on Tuesdays.
# The file needs to be downloaded manually.
    
#iD.importdeathByAge()


deathByAge = iD.Open('deathByAge') 



# UC data is updated sporadically.
# The file needs to be downloaded manually.
    
# iD.importUC()


UC = iD.Open('UC') 


# GDP data is updated monthly, approximately around the 10th of each month.
# The file needs to be downloaded manually.

#iD.importGDP()


GDP = iD.Open('yearlyGDP')


# IandP data is never updated.
    
#iD.importIandP()


IandP = iD.Open('IandP')

# LCD data is never updated.
    
#iD.importLCD()


LCD = iD.Open('LCD') 



# oldBedsOcc never needs to be updated

#iD.importOldBedsOcc()


oldBedsOcc = iD.Open('oldBedsOcc')







# Combine deaths, oldHospAd and newHospAd


df = iD.mergeFrames(deaths, oldHospAd )

df = iD.mergeFrames(df, newHospAd)


# Combine cases with OWID


casesOWID = iD.mergeFrames(cases, OWID)



iD.createYearlyMort(Mort, deaths)


yearlyMort = iD.Open('yearlyMort') 


# Stack the monthly and daily data on beds occupied covid

bedsOccCovid = iD.stackData(monthlyBedsOccCovid, dailyBedsOccCovid)

# Combine the beds occupied with beds occupied covid

bedsOcc = pd.merge( bedsOccCovid, monthlyBedsOcc , how = 'outer' )

# Add a non-covid beds occupied column

bedsOcc['NHS beds occupied non-Covid-19 England 2020'] = bedsOcc['Total NHS beds occupied England 2020'] -  bedsOcc['NHS beds occupied Covid-19 England 2020']



# Stack the months and daily data for MV beds occupied Covid

MVbedsOccCovid = iD.stackData(monthlyMVbedsOccCovid, dailyMVbedsOccCovid)

# Combine MV beds occupied with MV beds occupied covid

MVbeds = pd.merge(monthlyMVbedsOcc, MVbedsOccCovid, how='outer'  )

# Add an MV beds occupied non-covid column

MVbeds['Mechanical ventilation beds occupied non-Covid-19 England'] =  MVbeds['Mechanical ventilation beds occupied England'] - MVbeds['Mechanical ventilation beds occupied Covid-19 England']




# Create beds, which will combine hisotrical quarterly bed occupancy with bedsOcc

beds = pd.merge(oldBedsOcc, bedsOcc, how='outer')

    
# Make all entries the same type
    
beds.iloc[:, 1:] = beds.iloc[:, 1:].astype(np.float)





# lastDate is the final data where there is data in the df data frame

lastDate =  str(df.iloc[-1,0])[:10]


# totalCoronaDeaths is what it says.

totalCoronaDeaths = int( deaths.loc[0, 'Cumulative deaths'] )


# Add a column to IandP and LCD that is constant and equal to total 
# Covid-19 deaths. This is useful for plotting purposes


IandP.insert(1, 'Covid-19 deaths 2020 UK'  , totalCoronaDeaths  )

LCD.insert(1, 'Covid-19 deaths 2020 UK'  , totalCoronaDeaths  )


# This line is to make all the columns of IandP, LCD the same type, so they 
# can be  plotted together on the same plot by plotly.

IandP['Yearly influenza and pneumonia deaths England and Wales'] = IandP['Yearly influenza and pneumonia deaths England and Wales'].astype(np.int)


LCD.iloc[:, 1:] = LCD.iloc[:, 1:].astype(np.int)


# Calculate total Covid-19 excess deaths and total excess deaths this year.

totalNonCoronaED = (yearlyMort['Weekly deaths UK 2020'] - yearlyMort['Mean weekly deaths 2015-2019<br>plus Covid-19 deaths UK 2020']).sum()

totalED = (yearlyMort['Weekly deaths UK 2020'] - yearlyMort['Mean weekly deaths UK 2015-2019']).sum()

# Create a dictionary with some death related variables of interest.

deathDict = { "TCD": '{:,}'.format(totalCoronaDeaths), 
        "TNCED": '{:,}'.format(round(totalNonCoronaED,2)),
        "TED": '{:,}'.format(round(totalED,2))}

# Save that dictinary in json format. This can then be passed to other
# applications.

with open('deaths.json', 'w') as file:
    json.dump(deathDict, file)








# Create all the figures.


fig1 = px.line(df, x="Date", y=['Daily Covid-19 deaths UK', 'Daily hospital admissions with Covid-19 England', \
                    'Daily hospital admissions plus hospital diagnoses with Covid-19 England'] , \
               range_x=['2020-01-01',lastDate], \
             template = "simple_white", color_discrete_sequence =['red', 'gold', 'blue'] )

fig1.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=1.4,
    xanchor="left",
    x=0.02
)
    
)




testsFig = px.bar(OWID, x="Date", y=['Daily tests UK'], range_x=['2020-01-01',lastDate], \
             template = "simple_white", color_discrete_sequence =['magenta' ] )

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





casesFig =  px.bar(cases, x="Date", y=['Daily new Covid-19 cases UK'], \
             template = "simple_white" )
    
tests = px.line(OWID, x='Date', y=['Daily tests UK'], color_discrete_sequence = ['orange'], template = "simple_white") 

casesFig.add_trace(tests.data[0])

casesFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="left",
    x=0.02
)
)





    
    
    

testPosRateFig = px.line(OWID, x="Date", y=['Positive test rate UK'], range_x=['2020-01-01',lastDate], \
             template = "simple_white", color_discrete_sequence =['indigo' ] )

testPosRateFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="right",
    x=0.99
)
)

testPosRateFig.layout.yaxis.tickformat = ',.0%'



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





GDPFig = px.line(GDP, x="Date", y=['Monthly GDP index UK 2020', 'Monthly GDP index UK 2019', \
              'Monthly GDP index UK 2018', 'Monthly GDP index UK 2017'], range_x=['2007-01-01','2007-12-01'], \
             template = "simple_white")

GDPFig.update_layout(xaxis=dict(tickformat="%b"),
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.5,
    xanchor="right",
    x=0.99
)
)





IandPFig = px.line(IandP, x="Date", y=['Covid-19 deaths 2020 UK', 'Yearly influenza and pneumonia deaths England and Wales' ], \
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

yearlyMortCols = yearlyMortCols[5:] 

yearlyMortCols = [ yearlyMortCols[-2], yearlyMortCols[-1], yearlyMortCols[1], yearlyMortCols[0]]


meanDeathsFig = px.line(yearlyMort, x="Date", y= yearlyMortCols, range_x=['2015-01-01','2015-12-24'], template = "simple_white" )

meanDeathsFig.update_layout(
    
)


meanDeathsFig.update_layout(xaxis=dict(tickformat="%b %d"),
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
    yaxis_tickprefix = '£',
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.8,
    xanchor="left",
    x=0.02
)
)





bedLineCols =   beds.columns[19:23]

bedBarCols = [ beds.columns[-3], beds.columns[-1] ]



bedsFig = px.bar(beds, x='Date', y=bedBarCols, range_x=['2020-01-01',lastDate], \
  template = "simple_white", color_discrete_sequence =[ 'fuchsia', 'blue'] )


Line0 = px.line(beds, x='Date', y=[bedLineCols[0]], range_x=['2020-01-01',lastDate], color_discrete_sequence = ['red'], template = "simple_white") 

bedsFig.add_trace(Line0.data[0])

Line1 = px.line(beds, x='Date', y=[bedLineCols[1]], range_x=['2020-01-01',lastDate], color_discrete_sequence =[ 'green'], template = "simple_white") 

bedsFig.add_trace(Line1.data[0])

Line2 = px.line(beds, x='Date', y=[bedLineCols[2]], range_x=['2020-01-01',lastDate], color_discrete_sequence =[ 'orange'], template = "simple_white") 

bedsFig.add_trace(Line2.data[0])

Line3 = px.line(beds, x='Date', y=[bedLineCols[3]], range_x=['2020-01-01',lastDate], color_discrete_sequence =[ 'lightgreen'], template = "simple_white") 

bedsFig.add_trace(Line3.data[0])

bedsFig.update_layout(
    xaxis=dict(tickformat="%b %d"),
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=1.35,
    xanchor="left",
    x=0.02
)
)




unemploymentStart = datetime.datetime(2007, 1, 1)


unemploymentEnd = Unemployment.iloc[-1,0]

unemploymentFig = px.line(Unemployment, x="Date", y=['Unemployment rate (seasonally adjusted)'], \
                          range_x=[unemploymentStart, unemploymentEnd ], \
             template = "simple_white", color_discrete_sequence =[ 'yellowgreen'] )

unemploymentFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="right",
    x=0.99
)
)

unemploymentFig.layout.yaxis.tickformat = ',.1%'






MVbedsFig = px.line(MVbeds,  x="Date",  y=['Mechanical ventilation beds occupied England' , \
                    'Mechanical ventilation beds occupied Covid-19 England',  \
              'Mechanical ventilation beds occupied non-Covid-19 England'],                            
                     range_x=['2020-04-02',lastDate], template = "simple_white" )


MVbedsFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="right",
    x=0.99
)
)    



deathCompFig = px.line(deathComp,  x="Date",  y=['Weekly deaths within 28 days of a positive test' , \
        'Weekly deaths with Covid-19 on death certificate',] , color_discrete_sequence =[ 'lime', 'crimson'], \
                       template = "simple_white" )


deathCompFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="right",
    x=0.99
)
)    






# Create HTML files

pio.write_html(fig1, file='HTML files/fig1.html', auto_open=True)

pio.write_html(testsFig, file='HTML files/testsFig.html', auto_open=True)

pio.write_html(casesFig, file='HTML files/casesFig.html', auto_open=True)

pio.write_html(testPosRateFig, file='HTML files/testPosRateFig.html', auto_open=True)

pio.write_html(UCFig, file='HTML files/UCFig.html', auto_open=True)

pio.write_html(GDPFig, file='HTML files/GDPFig.html', auto_open=True)

pio.write_html(IandPFig, file='HTML files/IandPFig.html', auto_open=True)

pio.write_html(meanDeathsFig, file='HTML files/meanDeathsFig.html', auto_open=True)

pio.write_html(LCDFig, file='HTML files/LCDFig.html', auto_open=True)

pio.write_html(deathByAgeFig, file='HTML files/deathByAgeFig.html', auto_open=True)

pio.write_html(govSpendingFig, file='HTML files/govSpendingFig.html', auto_open=True)

pio.write_html(bedsFig, file='HTML files/bedsFig.html', auto_open=True)

pio.write_html(unemploymentFig, file='HTML files/unemploymentFig.html', auto_open=True)

pio.write_html(MVbedsFig, file='HTML files/MVbedsFig.html', auto_open=True)

pio.write_html(deathCompFig, file='HTML files/deathCompFig.html', auto_open=True)

