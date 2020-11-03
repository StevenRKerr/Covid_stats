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

import math

import ssl

import json

from datetime import date

import datetime

import importData as iD


import plotly.graph_objects as go





# Import and format all the data



# OWID data is updated daily
# The file is downloaded automatically.
    
iD.importOWID()


OWID = iD.Open('OWID')  


# The HospAd2 data series is updated daily.
# The link need to be updated manually.


iD.importHospAd2()


HospAd2 = iD.Open('HospAd2')



# Mortality data is updated weekly, on Thursdays.
# The file is downloaded automatically.
    
iD.importMort()


Mort = iD.Open('Mort')


# Beds needs to be updated on the first of each month.

#iD.importBeds()


beds = iD.Open('beds')

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

# HospAd never needs to be updated, because the data series of interest
# has been terminated.


#importHospAd()


HospAd = iD.Open('HospAd')









# Create combined OWID, HospAd, HospAd2 dataframe, and yearlyMort

df = iD.mergeFrames(OWID, HospAd )

df = iD.mergeFrames(df, HospAd2)



    
iD.createYearlyMort(Mort, OWID)


yearlyMort = iD.Open('yearlyMort') 


# lastDate is the final data where there is data in the combined HospAd + OWID
# data frame

lastDate =  str(df.iloc[-1,0])[:10]


# totalCoronaDeaths is what it says.

totalCoronaDeaths = int( OWID['Daily Covid-19 deaths UK'].sum() )


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


fig1 = px.line(df, x="Date", y=['Daily Covid-19 deaths UK', 'Daily hospital admissions with<br>Covid-19 England', \
                                'Daily hospital admissions<br>plus hospital diagnoses<br>with Covid-19 England' ], range_x=['2020-01-01',lastDate], \
             template = "simple_white", color_discrete_sequence =['red', 'gold', 'blue'] )

fig1.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=1.45,
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



testPosFig =  px.bar(OWID, x="Date", y=['Daily positive tests UK'], range_x=['2020-01-01',lastDate], \
             template = "simple_white", color_discrete_sequence =['lightseagreen' ] )

testPosFig.update_layout(
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


meanDeathsFig.update_layout(xaxis=dict(tickformat="%b"),
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

bedBarCols = [ beds.columns[-2], beds.columns[-1] ]



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
    y=1.2,
    xanchor="left",
    x=0.02
)
)






color_discrete_sequence =['orange' ,'teal' ]



# Create HTML files

pio.write_html(fig1, file='HTML files/fig1.html', auto_open=True)

pio.write_html(testsFig, file='HTML files/testsFig.html', auto_open=True)

pio.write_html(testPosFig, file='HTML files/testPosFig.html', auto_open=True)

pio.write_html(testPosRateFig, file='HTML files/testPosRateFig.html', auto_open=True)

pio.write_html(UCFig, file='HTML files/UCFig.html', auto_open=True)

pio.write_html(GDPFig, file='HTML files/GDPFig.html', auto_open=True)

pio.write_html(IandPFig, file='HTML files/IandPFig.html', auto_open=True)

pio.write_html(meanDeathsFig, file='HTML files/meanDeathsFig.html', auto_open=True)

pio.write_html(LCDFig, file='HTML files/LCDFig.html', auto_open=True)

pio.write_html(deathByAgeFig, file='HTML files/deathByAgeFig.html', auto_open=True)

pio.write_html(govSpendingFig, file='HTML files/govSpendingFig.html', auto_open=True)

pio.write_html(bedsFig, file='HTML files/bedsFig.html', auto_open=True)




